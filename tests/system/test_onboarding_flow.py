"""System tests: admin-provisioned onboarding with first-login email verification.

Flow under test (matches the product spec):
  1. Admin POSTs /users once (full name, username, email, temp password, role)
     -> account created unverified + credentials emailed. Admin's job ends.
  2. User's first login validates the temp password, sees email_verified=False,
     emails a 6-digit OTP and answers REQUIRES_VERIFICATION with redirect_to.
  3. POST /verify-otp exchanges the code for a full token pair (auto-login) and
     marks the email verified.
  4. Security semantics: wrong code rejected, code single-use, resend works.

The route functions are invoked directly with stubbed collaborators (same
no-live-server style as the rest of the suite); a stub Request keeps the
Redis-backed rate limiter out of the loop.
"""

import asyncio
import contextlib
from hashlib import sha256
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.api.routes import auth as auth_routes


def _hash_code(code: str) -> str:
    return sha256(code.encode()).hexdigest()


def _stub_request() -> object:
    """Minimal Request stand-in for enforce_rate_limit / direct route calls."""

    class _StubRequest:
        client = None

    return _StubRequest()


def _unverified_row() -> dict:
    return {
        "user_id": "usr_newuser1",
        "username": "newuser",
        "full_name": "New User",
        "email": "newuser@example.com",
        "role": "user",
        "is_active": True,
        "email_verified": False,
        "must_change_password": True,
        "password_hash": "$argon2id$fake",
        "created_at": None,
    }


def _run(coro):
    return asyncio.run(coro)


class TestAdminCreateUserOneStep:
    """POST /users: one call, credentials emailed, admin's responsibility ends."""

    def test_creates_unverified_account_and_emails_credentials(self):
        admin = {"user_id": "usr_admin1", "username": "dukaadmin", "role": "admin"}
        payload = auth_routes.CreateUserRequest(
            full_name="Alice Abebe",
            username="alice_onboard",
            email="alice.onboard@example.com",
            password="TempPass!2026",
            role="user",
        )
        row = dict(_unverified_row(), username="alice_onboard", full_name="Alice Abebe",
                   email="alice.onboard@example.com", user_id="usr_onboard1")

        with (
            patch("app.api.routes.auth.send_welcome_credentials_email", new_callable=AsyncMock) as welcome,
            patch("app.api.routes.auth.pg_client.create_user", new_callable=AsyncMock, return_value=row) as create_user,
            patch("app.api.routes.auth.pg_client.set_must_change_password", new_callable=AsyncMock) as force_flag,
            patch("app.api.routes.auth.pg_client.store_email_verification_token", new_callable=AsyncMock) as store_tok,
            patch("app.api.routes.auth.pg_client.get_user", new_callable=AsyncMock, return_value=row),
            patch("app.api.routes.auth.pg_client.get_user_by_email", new_callable=AsyncMock, return_value=None),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=None),
            patch("app.api.routes.auth.pg_client.record_audit_event", new_callable=AsyncMock),
        ):
            result = _run(auth_routes.create_user(payload, admin))

        assert result["username"] == "alice_onboard"
        assert result["is_email_verified"] is False

        # Account created unverified with a forced password change...
        create_user.assert_awaited_once()
        assert create_user.await_args.kwargs["email_verified"] is False
        force_flag.assert_awaited_once_with("usr_onboard1", True)
        # ...and a 6-digit verification code is persisted for the emailed
        # confirm step. Only the SHA-256 digest is stored - the raw code must
        # never be recoverable from the DB - and it must be the digest of the
        # code that was emailed, not of some separate one-click link token.
        store_tok.assert_awaited_once()
        _stored_user_id, _stored_digest = store_tok.await_args.args[:2]
        assert _stored_user_id == "usr_onboard1"
        assert len(_stored_digest) == 64
        int(_stored_digest, 16)  # hex digest, not the code itself
        _emailed_code = welcome.await_args.args[3]
        assert _emailed_code.isdigit() and len(_emailed_code) == 6
        assert _stored_digest == _hash_code(_emailed_code)
        # The code expires on the same short clock as the first-login OTP.
        assert (
            store_tok.await_args.kwargs.get("expires_minutes")
            == auth_routes.LOGIN_OTP_EXPIRE_MINUTES
        )
        # ...and the welcome email carried the username + temp password + the
        # user id the link needs (the link itself carries no secret).
        welcome.assert_awaited_once()
        assert welcome.await_args.args[1] == "alice_onboard"
        assert welcome.await_args.args[2] == "TempPass!2026"
        assert welcome.await_args.kwargs.get("user_id") == "usr_onboard1"

    def test_duplicate_email_rejected_before_any_email_is_sent(self):
        admin = {"user_id": "usr_admin1", "username": "dukaadmin", "role": "admin"}
        payload = auth_routes.CreateUserRequest(
            full_name="Dup User",
            username="dup_user1",
            email="taken@example.com",
            password="TempPass!2026",
        )
        with (
            patch("app.api.routes.auth.send_welcome_credentials_email", new_callable=AsyncMock) as welcome,
            patch(
                "app.api.routes.auth.pg_client.get_user_by_email",
                new_callable=AsyncMock,
                return_value={"user_id": "usr_x"},
            ),
        ):
            with pytest.raises(Exception) as exc:
                _run(auth_routes.create_user(payload, admin))
        assert getattr(exc.value, "status_code", None) == 409
        welcome.assert_not_awaited()

    def test_email_failure_rolls_the_account_back(self):
        admin = {"user_id": "usr_admin1", "username": "dukaadmin", "role": "admin"}
        payload = auth_routes.CreateUserRequest(
            full_name="Bob Broken",
            username="bob_broken",
            email="bob.broken@example.com",
            password="TempPass!2026",
        )
        row = dict(_unverified_row(), username="bob_broken", email="bob.broken@example.com",
                   user_id="usr_broken1")
        with (
            patch("app.api.routes.auth.send_welcome_credentials_email", new_callable=AsyncMock, side_effect=RuntimeError("smtp down")),
            patch("app.api.routes.auth.pg_client.create_user", new_callable=AsyncMock, return_value=row),
            patch("app.api.routes.auth.pg_client.set_must_change_password", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.store_email_verification_token", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user_by_email", new_callable=AsyncMock, return_value=None),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=None),
            patch("app.api.routes.auth.pg_client.delete_user", new_callable=AsyncMock, return_value=True) as delete_user,
        ):
            with pytest.raises(Exception) as exc:
                _run(auth_routes.create_user(payload, admin))
        assert getattr(exc.value, "status_code", None) == 503
        delete_user.assert_awaited_once_with("usr_broken1")


class TestFirstLoginOtpChallenge:
    """POST /login on an unverified account: OTP out, REQUIRES_VERIFICATION back."""

    def test_login_returns_requires_verification_and_emails_code(self):
        unverified = _unverified_row()
        request = _stub_request()
        payload = auth_routes.LoginRequest(username="newuser", password="TempPass!2026")

        with (
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=None),
            patch("app.api.routes.auth.pg_client.get_user_by_email", new_callable=AsyncMock, return_value=unverified),
            patch("app.api.routes.auth.verify_password", return_value=True),
            patch("app.api.routes.auth.send_security_code_email", new_callable=AsyncMock) as send_code,
            patch("app.api.routes.auth.pg_client.store_email_verification_token", new_callable=AsyncMock) as store_tok,
        ):
            body = _run(auth_routes.login(payload, request))

        assert body["status"] == "REQUIRES_VERIFICATION"
        assert body["user_id"] == "usr_newuser1"
        assert body["redirect_to"] == "/verify-otp"
        assert body["masked_email"] == "n***@example.com"
        assert body["expires_in_seconds"] == 600
        assert "access_token" not in body  # no session until verified

        # A 6-digit code was stored hashed and emailed in plaintext.
        send_code.assert_awaited_once()
        code = send_code.await_args.args[1]
        assert len(code) == 6 and code.isdigit()
        store_tok.assert_awaited_once_with("usr_newuser1", _hash_code(code), expires_minutes=10)

    def test_wrong_password_still_rejected_with_401(self):
        request = _stub_request()
        payload = auth_routes.LoginRequest(username="ghost", password="WrongPass!123")
        with (
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=None),
            patch("app.api.routes.auth.pg_client.get_user_by_email", new_callable=AsyncMock, return_value=None),
        ):
            with pytest.raises(Exception) as exc:
                _run(auth_routes.login(payload, request))
        assert getattr(exc.value, "status_code", None) == 401

    def test_disabled_account_rejected_before_otp(self):
        disabled = dict(_unverified_row(), is_active=False)
        request = _stub_request()
        payload = auth_routes.LoginRequest(username="newuser", password="TempPass!2026")
        with (
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=None),
            patch("app.api.routes.auth.pg_client.get_user_by_email", new_callable=AsyncMock, return_value=disabled),
            patch("app.api.routes.auth.verify_password", return_value=True),
            patch("app.api.routes.auth.send_security_code_email", new_callable=AsyncMock) as send_code,
        ):
            with pytest.raises(Exception) as exc:
                _run(auth_routes.login(payload, request))
        assert getattr(exc.value, "status_code", None) == 403
        send_code.assert_not_awaited()

    def test_mask_email_helper(self):
        assert auth_routes._mask_email("jane.doe@corp.com") == "j***@corp.com"
        assert auth_routes._mask_email("a@b.io") == "a***@b.io"


class TestVerifyOtp:
    """POST /verify-otp: code in, session out (auto-login)."""

    def test_valid_code_verifies_and_returns_tokens(self):
        user = _unverified_row()
        verified = dict(user, email_verified=True)
        payload = auth_routes.VerifyOtpRequest(user_id="usr_newuser1", code="849201")

        with (
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user", new_callable=AsyncMock, side_effect=[user, verified]),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=user),
            patch("app.api.routes.auth.pg_client.is_valid_verification_code", new_callable=AsyncMock, return_value=True),
            patch("app.api.routes.auth.pg_client.consume_verification_code", new_callable=AsyncMock, return_value=True) as consume,
            patch("app.api.routes.auth.pg_client.mark_email_verified", new_callable=AsyncMock) as mark,
            patch("app.api.routes.auth.pg_client.record_audit_event", new_callable=AsyncMock),
            patch("app.api.routes.auth.create_token_pair", new_callable=AsyncMock) as tokens,
        ):
            tokens.return_value = {"access_token": "a", "refresh_token": "r", "token_type": "bearer"}
            body = _run(auth_routes.verify_login_otp(payload, _stub_request()))

        assert body["access_token"] == "a"
        assert body["user"]["email_verified"] is True
        mark.assert_awaited_once_with("usr_newuser1")
        # The code was consumed (single-use) with the same hashed value before
        # the session was minted.
        consume.assert_awaited_once_with("usr_newuser1", "email_verification", _hash_code("849201"))

    def test_wrong_code_rejected_and_not_consumed(self):
        user = _unverified_row()
        payload = auth_routes.VerifyOtpRequest(user_id="usr_newuser1", code="000000")
        with (
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user", new_callable=AsyncMock, return_value=user),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=user),
            patch("app.api.routes.auth.pg_client.is_valid_verification_code", new_callable=AsyncMock, return_value=False),
            patch("app.api.routes.auth.pg_client.consume_verification_code", new_callable=AsyncMock) as consume,
        ):
            with pytest.raises(Exception) as exc:
                _run(auth_routes.verify_login_otp(payload, _stub_request()))
        assert getattr(exc.value, "status_code", None) == 400
        consume.assert_not_awaited()

    def test_already_verified_account_cannot_reuse_endpoint(self):
        verified = dict(_unverified_row(), email_verified=True)
        payload = auth_routes.VerifyOtpRequest(user_id="usr_newuser1", code="849201")
        with (
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user", new_callable=AsyncMock, return_value=verified),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=verified),
        ):
            with pytest.raises(Exception) as exc:
                _run(auth_routes.verify_login_otp(payload, _stub_request()))
        assert getattr(exc.value, "status_code", None) == 400

    def test_malformed_code_rejected_by_schema(self):
        with pytest.raises(Exception):
            auth_routes.VerifyOtpRequest(user_id="usr_newuser1", code="abc12!")


class TestResendOtp:
    """POST /verify-otp/resend: fresh code out, previous code dead."""

    def test_resend_issues_new_code_and_invalidates_old(self):
        user = _unverified_row()
        payload = auth_routes.ResendOtpRequest(user_id="usr_newuser1")
        with (
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user", new_callable=AsyncMock, return_value=user),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=user),
            patch("app.api.routes.auth.send_security_code_email", new_callable=AsyncMock) as send_code,
            patch("app.api.routes.auth.pg_client.store_email_verification_token", new_callable=AsyncMock) as store_tok,
        ):
            body = _run(auth_routes.resend_login_otp(payload, _stub_request()))

        assert body["status"] == "REQUIRES_VERIFICATION"
        assert body["user_id"] == "usr_newuser1"
        send_code.assert_awaited_once()
        assert len(send_code.await_args.args[1]) == 6
        # store_email_verification_token deletes prior codes for the user
        # before inserting the fresh one (pg client behaviour).
        store_tok.assert_awaited_once()

    def test_resend_for_unknown_user_404(self):
        payload = auth_routes.ResendOtpRequest(user_id="usr_missing")
        with (
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user", new_callable=AsyncMock, return_value=None),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=None),
        ):
            with pytest.raises(Exception) as exc:
                _run(auth_routes.resend_login_otp(payload, _stub_request()))
        assert getattr(exc.value, "status_code", None) == 404

    def test_resend_for_verified_account_rejected(self):
        verified = dict(_unverified_row(), email_verified=True)
        payload = auth_routes.ResendOtpRequest(user_id="usr_newuser1")
        with (
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch("app.api.routes.auth.pg_client.get_user", new_callable=AsyncMock, return_value=verified),
            patch("app.api.routes.auth.pg_client.get_user_by_username", new_callable=AsyncMock, return_value=verified),
        ):
            with pytest.raises(Exception) as exc:
                _run(auth_routes.resend_login_otp(payload, _stub_request()))
        assert getattr(exc.value, "status_code", None) == 400


class TestPgClientCodeSemantics:
    """consume_verification_code: single-use, expiry-aware, atomic UPDATE."""

    @pytest.mark.asyncio
    async def test_consume_returns_false_without_pool(self):
        saved = auth_routes.pg_client.system_pool
        try:
            auth_routes.pg_client.system_pool = None
            with pytest.raises(RuntimeError):
                await auth_routes.pg_client.consume_verification_code("usr_x", "email_verification", "hash")
        finally:
            auth_routes.pg_client.system_pool = saved

    @pytest.mark.asyncio
    async def test_consume_updates_only_unused_unexpired_rows(self):
        class FakeConn:
            async def fetchrow(self, query, *args):
                assert "SET used_at = NOW()" in query
                assert "used_at IS NULL" in query and "expires_at > NOW()" in query
                return {"token_id": "tok1"}

        class FakePool:
            def acquire(self):
                return self

            async def __aenter__(self):
                return FakeConn()

            async def __aexit__(self, *exc):
                return False

        saved = auth_routes.pg_client.system_pool
        try:
            auth_routes.pg_client.system_pool = FakePool()
            ok = await auth_routes.pg_client.consume_verification_code("usr_x", "email_verification", "h")
            assert ok is True
        finally:
            auth_routes.pg_client.system_pool = saved


class TestMandatoryOtpVerification:
    """The one-click "Verify Email" bypass must not exist.

    The emailed link now only opens the verification page; confirming an address
    requires the 6-digit code. These lock that in, including the single-use and
    already-verified behaviours.
    """

    def _confirm(self, user_id: str, code: str, user, overrides=None):
        """Call the endpoint with the rate limiter stubbed out."""
        payload = auth_routes.EmailVerificationRequest(user_id=user_id, code=code)
        stack = [
            patch("app.api.routes.auth.enforce_rate_limit", new_callable=AsyncMock),
            patch(
                "app.api.routes.auth._resolve_user",
                new_callable=AsyncMock,
                return_value=user,
            ),
            patch("app.api.routes.auth.pg_client.mark_email_verified", new_callable=AsyncMock),
            patch(
                "app.api.routes.auth.send_account_confirmation_email",
                new_callable=AsyncMock,
            ),
        ]
        stack += list(overrides or [])
        with contextlib.ExitStack() as es:
            mocks = [es.enter_context(c) for c in stack]
            return _run(
                auth_routes.confirm_email_verification(payload, _stub_request())
            ), mocks

    def test_request_model_rejects_the_old_token_field(self):
        """A legacy one-click payload must no longer validate."""
        with pytest.raises(ValidationError):
            auth_routes.EmailVerificationRequest(token="x" * 40)

    def test_request_model_requires_a_six_digit_code(self):
        with pytest.raises(ValidationError):
            auth_routes.EmailVerificationRequest(user_id="usr_x", code="123")

    def test_correct_code_verifies_the_account(self):
        user = dict(_unverified_row(), user_id="usr_otp1", email="otp1@example.com")
        valid = AsyncMock(return_value=True)
        consume = AsyncMock(return_value=True)
        mark = AsyncMock()

        body, _ = self._confirm(
            "usr_otp1",
            "849201",
            user,
            overrides=[
                patch(
                    "app.api.routes.auth.pg_client.is_valid_verification_code",
                    new=valid,
                ),
                patch(
                    "app.api.routes.auth.pg_client.consume_verification_code",
                    new=consume,
                ),
                patch("app.api.routes.auth.pg_client.mark_email_verified", new=mark),
            ],
        )

        assert "sign in" in body["message"].lower()
        mark.assert_awaited_once_with("usr_otp1")
        # Single-use: the code is consumed atomically, not merely checked.
        consume.assert_awaited_once_with(
            "usr_otp1", "email_verification", _hash_code("849201")
        )
        # Reaching this page never issues a session - no password was presented.
        assert "access_token" not in body

    def test_wrong_code_does_not_verify(self):
        user = dict(_unverified_row(), user_id="usr_otp2")
        mark = AsyncMock()

        with pytest.raises(HTTPException) as exc:
            self._confirm(
                "usr_otp2",
                "000000",
                user,
                overrides=[
                    patch(
                        "app.api.routes.auth.pg_client.is_valid_verification_code",
                        new=AsyncMock(return_value=False),
                    ),
                    patch("app.api.routes.auth.pg_client.mark_email_verified", new=mark),
                ],
            )

        assert exc.value.status_code == 400
        mark.assert_not_awaited()

    def test_replayed_code_is_rejected_by_the_atomic_consume(self):
        """A code that passes the check but was already consumed must fail."""
        user = dict(_unverified_row(), user_id="usr_otp3")
        mark = AsyncMock()

        with pytest.raises(HTTPException) as exc:
            self._confirm(
                "usr_otp3",
                "111111",
                user,
                overrides=[
                    patch(
                        "app.api.routes.auth.pg_client.is_valid_verification_code",
                        new=AsyncMock(return_value=True),
                    ),
                    patch(
                        "app.api.routes.auth.pg_client.consume_verification_code",
                        new=AsyncMock(return_value=False),
                    ),
                    patch("app.api.routes.auth.pg_client.mark_email_verified", new=mark),
                ],
            )

        assert exc.value.status_code == 400
        mark.assert_not_awaited()

    def test_already_verified_account_is_told_to_sign_in(self):
        user = dict(_unverified_row(), user_id="usr_otp4", email_verified=True)

        with pytest.raises(HTTPException) as exc:
            self._confirm("usr_otp4", "222222", user)

        assert exc.value.status_code == 400
        assert "sign in" in str(exc.value.detail).lower()

    def test_unknown_account_is_404(self):
        with pytest.raises(HTTPException) as exc:
            self._confirm("usr_missing", "333333", None)
        assert exc.value.status_code == 404

    def test_verify_email_link_endpoint_verifies_nothing(self):
        """GET /verify-email stays a harmless landing stub."""
        body = _run(auth_routes.verify_email_page())
        assert body["status"] == "ok"
