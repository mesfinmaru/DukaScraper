"""Auto-signup handler — detects login vs signup, handles verification.

Flow:
  1. Try LOGIN with stored credentials
  2. If login fails → detect "already registered" → retry login
  3. If not registered → do SIGNUP
  4. If verification needed → poll Gmail for verification link
  5. Navigate to verification link to complete signup
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from patchright.async_api import Page

from app.services.credential_service import credential_service
from app.services.gmail_verification import gmail_handler

logger = logging.getLogger(__name__)

# Phrases that indicate an account already exists
_ALREADY_REGISTERED_MARKERS = [
    "already registered",
    "already have an account",
    "account already exists",
    "already signed up",
    "already a member",
    "email already in use",
    "email address already",
    "this email is already",
    "user already exists",
    "account with this email",
    "login instead",
    "sign in instead",
]

# Phrases that indicate the site demands PHONE/SMS verification — signup is
# treated as failed because we cannot receive SMS codes.
# Explicit page-error feedback after a registration submit (e.g. "Wrong
# solved puzzle", "invalid username"). Any of these means the signup FAILED,
# regardless of what else is on the page.
_SUBMIT_ERROR_MARKERS = [
    "wrong solved",
    "wrong captcha",
    "incorrect captcha",
    "invalid captcha",
    "solve the puzzle",
    "wrong",
    "incorrect",
    "invalid",
    "captcha",
    "puzzle",
    "try again",
]

# Multi-step success requires stronger evidence than the single word
# "welcome" — registration pages often say "Welcome back" in the login
# prompt right next to the (failed) form.
_MULTI_STEP_SUCCESS_MARKERS = [
    "account created",
    "registration complete",
    "successfully registered",
    "your account has been created",
    "dashboard",
    "congratulations",
]

_PHONE_VERIFICATION_MARKERS = [
    "verify your phone",
    "verify your number",
    "phone verification",
    "phone number verification",
    "verify via sms",
    "verify by sms",
    "sms verification",
    "text message verification",
    "code sent to your phone",
    "code sent to your mobile",
    "sent a code to your phone",
    "enter your phone number",
    "confirm your phone",
    "otp sent to your phone",
    "otp sent to your mobile",
    "enter the otp sent",
    "enter verification code sent to your",
]

# Phrases that indicate verification is needed
_VERIFICATION_NEEDED_MARKERS = [
    "verify your email",
    "confirm your email",
    "check your email",
    "verification link sent",
    "confirmation link sent",
    "we sent a verification",
    "we sent a confirmation",
    "activate your account",
    "email sent to",
    "please check your inbox",
]

# Login form selectors (ordered by priority)
_USERNAME_SELECTORS = [
    "input[type='email']",
    "input[name='email']",
    "input[name='username']",
    "input[name='login']",
    "input[name='user']",
    "input[name='session_key']",
    "input[id='email']",
    "input[id='username']",
    "input[placeholder*='email' i]",
    "input[placeholder*='user' i]",
    "input[type='text']",
]

_PASSWORD_SELECTORS = [
    "input[type='password']",
    "input[name='password']",
    "input[name='passwd']",
    "input[name='pass']",
]

_SUBMIT_SELECTORS = [
    "button[type='submit']",
    "input[type='submit']",
    "button:has-text('Login')",
    "button:has-text('Sign in')",
    "button:has-text('Log in')",
    "button:has-text('Continue')",
    "button:has-text('Submit')",
]

_SIGNUP_LINK_SELECTORS = [
    "a:has-text('Sign up')",
    "a:has-text('Register')",
    "a:has-text('Create account')",
    "a:has-text('Create New Account')",
    "a:has-text('Join')",
    "a:has-text('Register Now')",
    "a[href*='signup']",
    "a[href*='register']",
    "a[href*='create-account']",
    "button:has-text('Register')",
    "button:has-text('Sign up')",
]

# Default signup credentials (overridable per-domain via portal_config)
_DEFAULT_SIGNUP_PASSWORD = "Duka@12345"

# Identity used when a signup form asks for a name
_SIGNUP_FIRST_NAME = "Duka"
_SIGNUP_LAST_NAME = "S."
_SIGNUP_FULL_NAME = "Duka S."

# Dropdown fields to auto-fill
_DROPDOWN_AUTO_FILL = {
    # Country
    "country": "United States",
    "country_id": "United States",
    "location": "United States",
    # Timezone
    "timezone": "UTC",
    "tz": "UTC",
    # Gender (if required)
    "gender": "Other",
    "sex": "Other",
    # Language
    "language": "English",
    "lang": "English",
}

# Radio button fields to auto-select
_RADIO_AUTO_SELECT = {
    "gender": "other",
    "sex": "other",
    "age_range": "25-34",
    "terms": "agree",
}

# Date of birth patterns
_DOB_SELECTORS = [
    "select[name*='birth' i]",
    "select[name*='dob' i]",
    "select[name*='day']",
    "select[name*='month']",
    "select[name*='year']",
    "input[name*='birth' i]",
    "input[name*='dob' i]",
    "input[type='date'][name*='birth' i]",
]

# Phone number patterns
_PHONE_SELECTORS = [
    "input[type='tel']",
    "input[name*='phone' i]",
    "input[name*='mobile' i]",
    "input[placeholder*='phone' i]",
    "input[placeholder*='mobile' i]",
]


class AutoSignupHandler:
    """Handles automated signup/login with email verification."""

    async def handle(
        self,
        page: Page,
        email: str,
        password: str,
        domain: str,
        portal_config: dict | None = None,
        perform_verification: bool = True,
    ) -> dict[str, Any]:
        """Main entry point: detect login vs signup and handle accordingly.

        Args:
            perform_verification: when False, signup stops after the form is
                submitted (verification email is NOT polled). Set False when
                the crawl did not ask for email verification.

        Returns:
            dict with: action, success, message, needs_verification, verified
        """
        result: dict[str, Any] = {
            "action": "unknown",
            "success": False,
            "message": "",
            "needs_verification": False,
            "verified": False,
        }

        # Check if we've used this credential on this domain before
        is_registered = await credential_service.is_domain_registered(domain)

        if is_registered:
            # Try login with stored credentials
            logger.info("[%s] Domain already registered — attempting login", domain)
            login_result = await self._try_login(page, email, password)
            result.update(login_result)

            if login_result["success"]:
                result["action"] = "login"
                await credential_service.record_usage(
                    email, domain, "login", "success"
                )
            else:
                # Login failed — maybe password changed or account locked
                result["action"] = "login_failed"
                await credential_service.record_usage(
                    email, domain, "login", "failed",
                    error_message=login_result["message"],
                )
            return result

        # Not registered — try signup
        logger.info("[%s] New domain — attempting signup", domain)
        signup_result = await self._try_signup(page, email, password, portal_config, domain=domain)
        result.update(signup_result)

        if signup_result["success"]:
            result["action"] = "signup"
            await credential_service.record_usage(
                email, domain, "signup", "success"
            )
            # Persist the reusable site credential (encrypted password) so
            # future crawls can log in automatically without signing up again.
            try:
                await credential_service.store_site_credential(
                    email=email,
                    domain=domain,
                    username=signup_result.get("username") or email,
                    password=password,
                    action="signup",
                    display_name=_SIGNUP_FULL_NAME,
                )
            except Exception:
                logger.warning("[%s] Failed to persist site credential after signup", domain, exc_info=True)

            if signup_result.get("needs_verification") and perform_verification:
                result["needs_verification"] = True
                # Handle verification
                verified = await self._handle_verification(
                    page, email, domain
                )
                result["verified"] = verified
            elif signup_result.get("needs_verification"):
                result["needs_verification"] = True
                logger.info(
                    "[%s] Site requested email verification but the crawl did not "
                    "enable it — skipping verification polling", domain,
                )
                await credential_service.record_usage(
                    email, domain, "verification_sent", "pending",
                    error_message="Verification email not received within timeout",
                )
        else:
            # Check if "already registered" appeared during signup
            page_text = await self._get_page_text(page)
            if self._check_markers(page_text, _ALREADY_REGISTERED_MARKERS):
                logger.info("[%s] Already registered — retrying login", domain)
                login_result = await self._try_login(page, email, password)
                result.update(login_result)
                result["action"] = "login_after_signup_failed"
                await credential_service.record_usage(
                    email, domain, "login", "success" if login_result["success"] else "failed"
                )
            else:
                result["action"] = "signup_failed"
                await credential_service.record_usage(
                    email, domain, "signup", "failed",
                    error_message=signup_result["message"],
                )

        return result

    async def _try_login(self, page: Page, email: str, password: str) -> dict[str, Any]:
        """Attempt to fill and submit a login form."""
        result = {"success": False, "message": ""}

        try:
            # --- Guard: do NOT fill credentials on Cloudflare challenge pages ---
            is_cf = await self._is_cloudflare_challenge(page)
            if is_cf:
                logger.info("[%s] Cloudflare challenge detected before login — attempting solve", email)
                solved = await self._try_solve_captcha(page)
                if solved:
                    await asyncio.sleep(3)
                # Re-check: if challenge persists, bail out
                if await self._is_cloudflare_challenge(page):
                    logger.warning("[%s] Cloudflare challenge not resolved — cannot proceed with login", email)
                    result["message"] = "Cloudflare challenge not resolved — login skipped"
                    return result

            # Find username/email field
            username_input = None
            for selector in _USERNAME_SELECTORS:
                el = await page.query_selector(selector)
                if el:
                    username_input = el
                    break

            if not username_input:
                result["message"] = "No username/email field found"
                return result

            # Find password field
            password_input = None
            for selector in _PASSWORD_SELECTORS:
                el = await page.query_selector(selector)
                if el:
                    password_input = el
                    break

            if not password_input:
                result["message"] = "No password field found"
                return result

            # Fill credentials
            await username_input.fill(email)
            await asyncio.sleep(0.3)
            await password_input.fill(password)
            await asyncio.sleep(0.3)

            # Submit
            submit_btn = None
            for selector in _SUBMIT_SELECTORS:
                el = await page.query_selector(selector)
                if el:
                    submit_btn = el
                    break

            if submit_btn:
                try:
                    async with page.expect_navigation(wait_until="commit", timeout=15_000):
                        await submit_btn.click()
                except Exception:
                    await submit_btn.click()
                    await asyncio.sleep(3)
            else:
                # Try pressing Enter
                await password_input.press("Enter")
                await asyncio.sleep(3)

            # Check result
            await asyncio.sleep(2)
            page_text = await self._get_page_text(page)

            # Check for error messages
            error_markers = [
                "incorrect password", "wrong password", "invalid credentials",
                "invalid email or password", "login failed", "authentication failed",
                "account not found", "no account found",
            ]
            if self._check_markers(page_text, error_markers):
                result["message"] = "Login failed — invalid credentials"
                return result

            # Check for success indicators
            success_markers = [
                "dashboard", "welcome", "profile", "account", "settings",
                "logout", "sign out", "my account",
            ]
            if self._check_markers(page_text, success_markers):
                result["success"] = True
                result["message"] = "Login successful"
            else:
                # Assume success if no clear error (many SPAs redirect)
                result["success"] = True
                result["message"] = "Login submitted (no clear error detected)"

        except Exception as exc:
            result["message"] = f"Login error: {exc}"

        return result

    async def _try_signup(
        self, page: Page, email: str, password: str, portal_config: dict | None = None, domain: str = ""
    ) -> dict[str, Any]:
        """Attempt to find and complete a signup form."""
        result: dict[str, Any] = {"success": False, "message": "", "needs_verification": False}

        try:
            # --- Guard: do NOT fill credentials on Cloudflare challenge pages ---
            is_cf = await self._is_cloudflare_challenge(page)
            if is_cf:
                logger.info("[%s] Cloudflare challenge detected before signup — attempting solve", email)
                solved = await self._try_solve_captcha(page)
                if solved:
                    await asyncio.sleep(3)
                # Re-check: if challenge persists, bail out
                is_cf = await self._is_cloudflare_challenge(page)
                if is_cf:
                    logger.warning("[%s] Cloudflare challenge not resolved — cannot proceed with signup", email)
                    result["message"] = "Cloudflare challenge not resolved — signup skipped"
                    return result

            # First, look for a signup link if we're on a login page
            # BUT skip this if we're already on a registration/signup URL
            current_url = page.url.lower()
            already_on_signup_page = any(
                kw in current_url
                for kw in ("/register", "/signup", "/sign-up", "/create-account")
            )
            if not already_on_signup_page:
                page_text = await self._get_page_text(page)
                if self._check_markers(page_text, ["login", "sign in", "log in"]):
                    for selector in _SIGNUP_LINK_SELECTORS:
                        link = await page.query_selector(selector)
                        if link:
                            await link.click()
                            await asyncio.sleep(3)
                            break
            else:
                logger.info("[%s] Already on signup page (%s) — skipping navigation", email, current_url)

            # --- Second Cloudflare check (page may have navigated to signup page) ---
            is_cf = await self._is_cloudflare_challenge(page)
            if is_cf:
                logger.info("[%s] Cloudflare challenge on signup page — solving", email)
                solved = await self._try_solve_captcha(page)
                if solved:
                    await asyncio.sleep(3)
                # Final check
                if await self._is_cloudflare_challenge(page):
                    logger.warning("[%s] Cloudflare challenge persists on signup page — aborting", email)
                    result["message"] = "Cloudflare challenge not resolved on signup page"
                    return result

            # --- Dismiss cookie consent / GDPR banners that may overlay the form ---
            await self._dismiss_overlay_banners(page)

            # Debug: log ALL input fields (visible or not) on the page
            all_raw_inputs = await page.query_selector_all("input")
            _debug_fields = []
            for _inp in all_raw_inputs:
                try:
                    _vis = await _inp.is_visible()
                    _type = await _inp.get_attribute("type") or "text"
                    _name = await _inp.get_attribute("name") or ""
                    _id = await _inp.get_attribute("id") or ""
                    _debug_fields.append(f"type={_type} name={_name} id={_id} vis={_vis}")
                except Exception:
                    pass
            logger.info("[%s] All input fields on signup page (%d): %s", email, len(_debug_fields), _debug_fields)

            # Custom puzzle/math captchas (radio answer lists, decaptcha_hash)
            # cannot be solved automatically — fail fast with a clear reason
            # instead of submitting and pretending it worked.
            _puzzle_present = bool(
                await page.query_selector(
                    "input[name='decaptcha_hash'], input[name^='captcha_answer'], "
                    "input[id='decaptcha_hash'], input[name^='puzzle']"
                )
            )
            if _puzzle_present:
                logger.warning(
                    "[%s] Custom puzzle/captcha registration detected — cannot auto-register",
                    email,
                )
                result["message"] = (
                    "Signup failed: site requires solving a custom puzzle/captcha, "
                    "which is not supported"
                )
                return result

            # Find all input fields — include both visible and hidden (some forms use JS to reveal)
            all_inputs = all_raw_inputs
            username_filled = False
            email_filled = False
            password_filled = False
            confirm_password_filled = False
            name_filled = False

            # Generate a username from the email for forum registrations
            _domain = locals().get("domain", "") or getattr(self, "_current_domain", "")
            _generated_username = self._generate_username(email, _domain)
            result["username"] = _generated_username

            for inp in all_inputs:
                input_type = await inp.get_attribute("type") or ""
                input_name = (await inp.get_attribute("name") or "").lower()
                input_placeholder = (await inp.get_attribute("placeholder") or "").lower()
                input_id = (await inp.get_attribute("id") or "").lower()
                # For form filling, prefer visible fields but fall back to all fields
                # (some XenForo forms render inputs hidden until JS activates them)

                # Username field (XenForo, Discourse, etc.)
                if not username_filled and (
                    "username" in input_name
                    or "user_name" in input_name
                    or "user-name" in input_name
                    or "login_name" in input_name
                    or input_placeholder == "username"
                    or "display name" in input_placeholder
                    or "display_name" in input_placeholder
                    or input_id == "ctrl_username"
                ):
                    if input_type not in ("email", "password"):
                        await inp.fill(_generated_username)
                        username_filled = True
                        logger.info("Filled username field '%s' with '%s'", input_name, _generated_username)
                        continue

                # Email field
                if not email_filled and (
                    input_type == "email"
                    or "email" in input_name
                    or "email" in input_placeholder
                    or "e-mail" in input_name
                    or input_id == "ctrl_email"
                ):
                    await inp.fill(email)
                    email_filled = True
                    logger.info("Filled email field '%s'", input_name)
                    continue

                # Password field
                if not password_filled and input_type == "password":
                    await inp.fill(password)
                    password_filled = True
                    logger.info("Filled password field '%s'", input_name)
                    continue

                # Name fields (first/last/full/display name)
                if not name_filled and input_type not in ("email", "password"):
                    _name_value: str | None = None
                    if "full_name" in input_name or "fullname" in input_name \
                            or "full name" in input_placeholder:
                        _name_value = _SIGNUP_FULL_NAME
                    elif "first_name" in input_name or "firstname" in input_name \
                            or "fname" in input_name or "first name" in input_placeholder:
                        _name_value = _SIGNUP_FIRST_NAME
                    elif "last_name" in input_name or "lastname" in input_name \
                            or "lname" in input_name or "surname" in input_name \
                            or "last name" in input_placeholder:
                        _name_value = _SIGNUP_LAST_NAME
                    elif input_name in ("name", "realname", "real_name") \
                            or input_placeholder in ("name", "your name", "full name"):
                        _name_value = _SIGNUP_FULL_NAME
                    if _name_value:
                        await inp.fill(_name_value)
                        name_filled = True
                        logger.info("Filled name field '%s' with '%s'", input_name, _name_value)
                        continue

            # The per-field heuristics above fill at most one name input, so a
            # split firstName/lastName form keeps a required field empty. HTML5
            # then refuses to submit: nothing navigates, no error is shown, and
            # the form still looks present, which we report as a signup failure
            # with no clue why. Sweep whatever is still required and empty.
            await self._fill_missing_required_fields(page, email, password)

            if not email_filled or not password_filled:
                # Try XenForo-specific fallback selectors
                if not email_filled:
                    xen_email = await page.query_selector("#ctrl_email")
                    if xen_email:
                        await xen_email.fill(email)
                        email_filled = True
                        logger.info("Filled XenForo email field via #ctrl_email")
                if not password_filled:
                    xen_pass = await page.query_selector("#ctrl_password")
                    if xen_pass:
                        await xen_pass.fill(password)
                        password_filled = True
                        logger.info("Filled XenForo password field via #ctrl_password")

            if not email_filled or not password_filled:
                # Try multi-step registration wizard
                logger.info(
                    "[%s] Single-page signup incomplete (email=%s, password=%s) — trying multi-step",
                    email, email_filled, password_filled,
                )
                return await self._handle_multi_step_registration(page, email, password)

            # Look for confirm password field
            for inp in all_inputs:
                input_type = await inp.get_attribute("type") or ""
                input_name = (await inp.get_attribute("name") or "").lower()
                if input_type == "password" and not confirm_password_filled and (
                    "confirm" in input_name or "repeat" in input_name or "verify" in input_name
                    or "ctrl_password_confirm" in input_name
                ):
                    await inp.fill(password)
                    confirm_password_filled = True
                    logger.info("Filled confirm password field '%s'", input_name)
                    break
            # XenForo confirm password fallback
            if not confirm_password_filled:
                xen_confirm = await page.query_selector("#ctrl_password_confirm")
                if xen_confirm:
                    await xen_confirm.fill(password)
                    confirm_password_filled = True
                    logger.info("Filled XenForo confirm password via #ctrl_password_confirm")

            # --- Check / tick agreement checkboxes (Terms of Service, etc.) ---
            await self._tick_agreement_checkboxes(page)

            # --- Answer anti-bot Q&A fields ---
            await self._answer_bot_qa_fields(page)

            # --- Fill dropdowns (country, timezone, gender, etc.) ---
            await self._fill_dropdowns(page)

            # --- Select radio buttons (gender, age range, etc.) ---
            await self._fill_radio_buttons(page)

            # --- Fill date of birth fields ---
            await self._fill_date_of_birth(page)

            # --- Fill phone number fields ---
            await self._fill_phone_number(page)

            # --- Confirm password by position (fallback if name-based detection failed) ---
            if not confirm_password_filled:
                await self._handle_confirm_password_by_position(page)

            # --- Wait for Turnstile to auto-resolve if present ---
            turnstile_response = await page.query_selector("input[name='cf-turnstile-response']")
            if turnstile_response:
                logger.info("[%s] Turnstile widget present — waiting for auto-resolve (up to 60s with retries)", email)
                token_resolved = False
                for attempt in range(3):
                    wait_timeout = [30_000, 20_000, 10_000][attempt]
                    try:
                        await page.wait_for_function(
                            "() => {\n"
                            "  const el = document.querySelector(\"input[name='cf-turnstile-response']\");\n"
                            "  return el && el.value && el.value.length > 10;\n"
                            "}",
                            timeout=wait_timeout,
                        )
                        logger.info("[%s] Turnstile token resolved (attempt %d)", email, attempt + 1)
                        token_resolved = True
                        break
                    except Exception:
                        logger.warning("[%s] Turnstile not resolved after %ds (attempt %d) — trying iframe click", email, wait_timeout // 1000, attempt + 1)
                        # Try clicking the Turnstile iframe checkbox
                        try:
                            cf_iframes = [
                                "iframe[src*='challenges.cloudflare.com']",
                                "iframe[src*='turnstile']",
                                "iframe[src*='cf-chl']",
                            ]
                            for cf_sel in cf_iframes:
                                cf_iframe = await page.query_selector(cf_sel)
                                if cf_iframe:
                                    frame = await cf_iframe.content_frame()
                                    if frame:
                                        checkbox = await frame.query_selector("input[type='checkbox'], #challenge-stage, .cb-lb")
                                        if checkbox and await checkbox.is_visible():
                                            await checkbox.click()
                                            logger.info("[%s] Clicked Turnstile checkbox in iframe (attempt %d)", email, attempt + 1)
                                            break
                        except Exception as click_err:
                            logger.debug("[%s] Turnstile iframe click failed: %s", email, click_err)
                        # Also try the hidden cf-turnstile response token via page.evaluate
                        try:
                            await page.evaluate("""
                                () => {
                                    if (window.turnstile) {
                                        const widgets = document.querySelectorAll('.cf-turnstile');
                                        widgets.forEach(w => { try { turnstile.execute(w); } catch(e) {} });
                                    }
                                }
                            """)
                        except Exception:
                            pass
                        await asyncio.sleep(3)

                if not token_resolved:
                    logger.warning("[%s] Turnstile did not resolve after all retries — proceeding anyway", email)

            # Submit the form
            submit_btn = None
            for selector in _SUBMIT_SELECTORS:
                el = await page.query_selector(selector)
                if el:
                    submit_btn = el
                    break

            # Also try signup-specific submit buttons
            if not submit_btn:
                signup_submit_selectors = [
                    "button:has-text('Sign up')",
                    "button:has-text('Register')",
                    "button:has-text('Create account')",
                    "button:has-text('Join')",
                    "button:has-text('Submit')",
                    "button:has-text('Complete Registration')",
                    "button:has-text('Register Now')",
                    "input[type='submit'][value*='Register' i]",
                    "input[type='submit'][value*='Sign' i]",
                ]
                for selector in signup_submit_selectors:
                    el = await page.query_selector(selector)
                    if el:
                        submit_btn = el
                        break

            if submit_btn:
                try:
                    async with page.expect_navigation(wait_until="commit", timeout=15_000):
                        await submit_btn.click()
                except Exception:
                    # Some forms submit via JS without navigation
                    await submit_btn.click()
                    await asyncio.sleep(3)
            else:
                result["message"] = "No submit button found"
                return result

            await asyncio.sleep(3)

            # --- Post-submit CAPTCHA / Cloudflare challenge resolution (single attempt with cap) ---
            # Total timeout: 30s max for post-submit CF solving to avoid stalling the job
            _post_submit_start = asyncio.get_event_loop().time()
            is_challenge = await self._is_cloudflare_challenge(page)
            if is_challenge:
                logger.info("[%s] Cloudflare challenge after form submit — solving (30s cap)", email)
                # Single solve attempt with internal retries (handled by _try_solve_captcha)
                await self._try_solve_captcha(page)
                # Wait up to 30s total for token to populate
                for _ in range(6):
                    elapsed = asyncio.get_event_loop().time() - _post_submit_start
                    if elapsed >= 30:
                        break
                    await asyncio.sleep(5)
                    try:
                        token = await page.evaluate("""
                            () => {
                                const el = document.querySelector('input[name="cf-turnstile-response"]');
                                return el ? el.value : '';
                            }
                        """)
                        if token and len(token) > 10:
                            logger.info("[%s] Turnstile token resolved post-submit (%.1fs)", email, asyncio.get_event_loop().time() - _post_submit_start)
                            await asyncio.sleep(5)
                            break
                    except Exception:
                        pass
                else:
                    logger.warning("[%s] Turnstile did not resolve within 30s — checking if form still submitted successfully", email)

            page_text = await self._get_page_text(page)

            # Phone/SMS verification demanded → treat signup as failed (we
            # cannot receive SMS codes). Do not persist the credential.
            if self._check_markers(page_text, _PHONE_VERIFICATION_MARKERS):
                logger.warning(
                    "[%s] Site requires phone/SMS verification — marking signup as FAILED for %s",
                    email, domain,
                )
                result["success"] = False
                result["needs_verification"] = False
                result["message"] = (
                    "Signup failed: site requires phone/SMS verification, which is not supported"
                )
                return result

            # Check for verification needed
            if self._check_markers(page_text, _VERIFICATION_NEEDED_MARKERS):
                result["success"] = True
                result["needs_verification"] = True
                result["message"] = "Signup successful — verification email sent"
                return result

            # Check for errors
            error_markers = [
                "error", "failed", "invalid", "required",
                "taken", "not available", "too short", "weak password",
                "doka is already", "that name is not valid",
            ]
            username_collision_markers = [
                "username already", "username taken", "name already",
                "name is taken", "not available", "already registered",
                "already a member", "already exists",
            ]
            if self._check_markers(page_text, error_markers):
                # Check if it's a username collision — retry with different username
                if self._check_markers(page_text, username_collision_markers):
                    import random
                    _retry_username = _generated_username + str(random.randint(10, 99))
                    logger.info(
                        "[%s] Username collision — retrying with '%s'",
                        email, _retry_username,
                    )
                    # Find and update the username field
                    username_input = None
                    for sel in ["input[name='username']", "input[name='user_name']",
                                "input[id='ctrl_username']", "input[name='register_username']"]:
                        username_input = await page.query_selector(sel)
                        if username_input:
                            break
                    if username_input:
                        await username_input.fill("")
                        await username_input.fill(_retry_username)
                        await asyncio.sleep(0.5)
                        # Re-submit
                        submit_btn = None
                        for sel in _SUBMIT_SELECTORS + [
                            "button:has-text('Register')",
                            "button:has-text('Sign up')",
                            "button:has-text('Create account')",
                        ]:
                            submit_btn = await page.query_selector(sel)
                            if submit_btn:
                                break
                        if submit_btn:
                            try:
                                await submit_btn.click()
                                await asyncio.sleep(5)
                                page_text = await self._get_page_text(page)
                                # Check if retry succeeded
                                if not self._check_markers(page_text, error_markers):
                                    result["success"] = True
                                    result["message"] = f"Signup successful with username '{_retry_username}'"
                                    if self._check_markers(page_text, _VERIFICATION_NEEDED_MARKERS):
                                        result["needs_verification"] = True
                                    return result
                            except Exception as retry_err:
                                logger.debug("Username retry submit error: %s", retry_err)

                result["message"] = "Signup may have failed — error markers detected on page"
                logger.warning("[%s] Signup error markers: %s", email, page_text[:500])
                return result

            # Check for success
            success_markers = [
                "welcome", "dashboard", "account created", "successfully",
                "congratulations", "thank you", "verify your email",
                "your account has been created",
                "registration complete",
            ]
            if self._check_markers(page_text, success_markers):
                result["success"] = True
                result["message"] = "Signup appears successful"
            else:
                # No success text — only believe it worked if the registration
                # form is actually GONE. A still-visible form means we never
                # left the page (e.g. a captcha silently rejected us).
                _form_still_present = bool(
                    await page.query_selector(
                        "input[type='email']:visible, input[type='password']:visible, "
                        "input[name='username']:visible"
                    )
                )
                if _form_still_present:
                    result["success"] = False
                    result["message"] = (
                        "Signup failed: registration form still present after submit "
                        "(no success or error text detected)"
                    )
                    logger.warning(
                        "[%s] Form still present after submit — treating signup as FAILED",
                        email,
                    )
                else:
                    result["success"] = True
                    result["message"] = "Signup submitted (form no longer present, no error detected)"

        except Exception as exc:
            result["message"] = f"Signup error: {exc}"
            logger.error("[%s] Signup exception: %s", email, exc, exc_info=True)

        return result

    async def _fill_missing_required_fields(self, page: Page, email: str, password: str) -> None:
        """Fill any `required` control the field heuristics left empty.

        HTML5 constraint validation blocks submission silently — the browser
        simply refuses to post, nothing navigates, and the page keeps looking
        like the form is still there. Split name fields (firstName + lastName,
        the norm on Facebook/LinkedIn-style forms and on the student-portal
        configs in this repo) hit this constantly because the name pass fills
        only the first name input it recognises.

        Deliberately conservative: only text-like controls get a guessed value.
        Phone, date, number and file inputs are skipped rather than invented,
        because a made-up phone number or DOB would create a genuinely wrong
        account on a real site, which is worse than a clear signup failure.
        """
        candidates = await page.query_selector_all(
            "input[required]:visible, select[required]:visible, textarea[required]:visible"
        )
        for el in candidates:
            try:
                tag = (await el.evaluate("e => e.tagName.toLowerCase()")) or ""

                if tag == "select":
                    if await el.evaluate("e => e.value"):
                        continue
                    options = await el.evaluate(
                        "e => [...e.options].map(o => o.value).filter(v => v !== '')"
                    )
                    if options:
                        await el.select_option(options[0])
                        logger.info("Selected first option for required <select>")
                    continue

                input_type = ((await el.get_attribute("type")) or "text").lower()
                if input_type in ("checkbox", "radio", "hidden", "submit", "button"):
                    continue
                if input_type in ("tel", "date", "datetime-local", "month", "week",
                                  "time", "file", "number"):
                    continue
                if await el.evaluate("e => e.value"):
                    continue

                hints = " ".join(
                    filter(None, [
                        await el.get_attribute("name"),
                        await el.get_attribute("id"),
                        await el.get_attribute("placeholder"),
                    ])
                ).lower()

                if "email" in hints or input_type == "email":
                    value = email
                elif "pass" in hints or input_type == "password":
                    value = password
                elif "last" in hints or "lname" in hints or "surname" in hints:
                    value = _SIGNUP_LAST_NAME
                elif "first" in hints or "fname" in hints:
                    value = _SIGNUP_FIRST_NAME
                elif "name" in hints or "address" in hints or "city" in hints:
                    value = _SIGNUP_FULL_NAME
                else:
                    value = _SIGNUP_FULL_NAME

                await el.fill(value)
                logger.info(
                    "Filled required field '%s' left empty by heuristics (%s)",
                    (await el.get_attribute("name")) or await el.get_attribute("id") or "?",
                    input_type,
                )
            except Exception as exc:
                logger.debug("Could not fill a required field: %s", exc)

    async def _tick_agreement_checkboxes(self, page: Page) -> None:
        """Check agreement / terms-of-service checkboxes on signup forms."""
        checkbox_selectors = [
            # XenForo
            "input[type='checkbox'][name='reg_agree']",
            "input[type='checkbox'][name='agree']",
            "input[type='checkbox'][name*='terms']",
            "input[type='checkbox'][name*='agree']",
            "input[type='checkbox'][name*='tos']",
            "input[type='checkbox'][name*='policy']",
            "#ctrl_agree",
            # Generic
            "label:has-text('I agree') input[type='checkbox']",
            "label:has-text('Terms') input[type='checkbox']",
            "label:has-text('conditions') input[type='checkbox']",
            "label:has-text('privacy') input[type='checkbox']",
        ]
        for sel in checkbox_selectors:
            cb = await page.query_selector(sel)
            if cb:
                try:
                    is_checked = await cb.is_checked()
                    if not is_checked:
                        await cb.check()
                        logger.info("Checked agreement checkbox: %s", sel)
                except Exception:
                    pass

    async def _dismiss_overlay_banners(self, page: Page) -> None:
        """Dismiss cookie consent, GDPR, and other overlay banners that may block form interaction.

        Carefully avoids clicking buttons inside Cloudflare/Turnstile iframes
        or buttons that would navigate away from the registration form.
        """
        banner_selectors = [
            # Common cookie consent buttons
            "button:has-text('Accept All')",
            "button:has-text('Accept Cookies')",
            "button:has-text('I Accept')",
            "button:has-text('Got it')",
            "button:has-text('Allow All')",
            # Common consent IDs
            "#onetrust-accept-btn-handler",
            "#accept-cookies",
            "#cookie-accept",
            "button[id*='accept']",
            "button[id*='consent']",
            # XenForo banner close
            "button.js-notice-close",
            ".notice-dismissButton",
        ]
        dismissed = 0
        for sel in banner_selectors:
            try:
                btn = await page.query_selector(sel)
                if not btn or not await btn.is_visible():
                    continue

                # Safety check: skip buttons inside iframes (Cloudflare Turnstile, reCAPTCHA)
                is_in_iframe = await btn.evaluate("""
                    (el) => {
                        let parent = el.parentElement;
                        while (parent) {
                            if (parent.tagName === 'IFRAME') return true;
                            parent = parent.parentElement;
                        }
                        return false;
                    }
                """)
                if is_in_iframe:
                    continue

                # Safety check: skip buttons with Cloudflare-related attributes
                btn_html = await btn.evaluate("el => el.outerHTML.substring(0, 200)")
                if any(cf in btn_html.lower() for cf in ('cf-', 'turnstile', 'challenge', 'cloudflare')):
                    continue

                await btn.click()
                dismissed += 1
                await asyncio.sleep(0.5)
                logger.info("Dismissed overlay banner: %s", sel)
                if dismissed >= 3:
                    break
            except Exception:
                pass
        if dismissed:
            await asyncio.sleep(1)

    # ------------------------------------------------------------------
    # Enhanced form field handlers (dropdowns, radios, DOB, phone)
    # ------------------------------------------------------------------

    async def _fill_dropdowns(self, page: Page) -> None:
        """Auto-fill dropdown/select fields with sensible defaults."""
        try:
            selects = await page.query_selector_all("select")
            for sel_el in selects:
                name = (await sel_el.get_attribute("name") or "").lower()
                sel_id = (await sel_el.get_attribute("id") or "").lower()
                is_visible = await sel_el.is_visible()
                if not is_visible:
                    continue

                # Skip selects that already have a value selected
                current_value = await sel_el.evaluate("el => el.value")
                if current_value and current_value not in ("", "0", "-1"):
                    continue

                # Match against known dropdown patterns
                matched_value = None
                for key, default_val in _DROPDOWN_AUTO_FILL.items():
                    if key in name or key in sel_id:
                        matched_value = default_val
                        break

                # Also check the label text
                if not matched_value:
                    label_text = await page.evaluate("""
                        (sel) => {
                            const id = sel.id;
                            if (id) {
                                const label = document.querySelector('label[for="' + id + '"]');
                                if (label) return label.innerText.toLowerCase();
                            }
                            const parent = sel.closest('.form-group, .formRow, .ipsFieldRow, div');
                            if (parent) {
                                const lbl = parent.querySelector('label, h3, h4, p, dt');
                                if (lbl) return lbl.innerText.toLowerCase();
                            }
                            return '';
                        }
                    """, sel_el)
                    for key, default_val in _DROPDOWN_AUTO_FILL.items():
                        if key in label_text:
                            matched_value = default_val
                            break

                if matched_value:
                    # Try to select by visible text
                    try:
                        options = await sel_el.evaluate("""
                            (sel) => Array.from(sel.options).map(o => ({
                                text: o.text.trim(),
                                value: o.value
                            }))
                        """)
                        # Find best match
                        for opt in options:
                            if matched_value.lower() in opt["text"].lower():
                                await sel_el.select_option(label=opt["text"])
                                logger.info("Filled dropdown '%s' with '%s'", name, opt["text"])
                                break
                        else:
                            # Fallback: select first non-empty option
                            for opt in options[1:]:  # Skip placeholder
                                if opt["text"].strip() and opt["value"]:
                                    await sel_el.select_option(value=opt["value"])
                                    logger.info("Filled dropdown '%s' with first option '%s'", name, opt["text"])
                                    break
                    except Exception as fill_err:
                        logger.debug("Dropdown fill failed for '%s': %s", name, fill_err)
        except Exception as exc:
            logger.debug("Dropdown handling error: %s", exc)

    async def _fill_radio_buttons(self, page: Page) -> None:
        """Auto-select radio buttons for gender, age range, etc."""
        try:
            radio_groups = await page.evaluate("""
                () => {
                    const radios = document.querySelectorAll('input[type=radio]');
                    const groups = {};
                    radios.forEach(r => {
                        const name = r.name;
                        if (!groups[name]) groups[name] = [];
                        groups[name].push({
                            value: r.value,
                            id: r.id,
                            label: '',
                            checked: r.checked,
                        });
                    });
                    // Get labels for each radio
                    for (const name in groups) {
                        groups[name].forEach(r => {
                            if (r.id) {
                                const lbl = document.querySelector('label[for="' + r.id + '"]');
                                if (lbl) r.label = lbl.innerText.trim().toLowerCase();
                            }
                        });
                    }
                    return groups;
                }
            """)

            for name, options in radio_groups.items():
                # Skip if already checked
                if any(o.get("checked") for o in options):
                    continue

                name_lower = name.lower()
                # Match against known patterns
                for pattern, desired_value in _RADIO_AUTO_SELECT.items():
                    if pattern in name_lower:
                        # Find matching option
                        for opt in options:
                            val = opt.get("value", "").lower()
                            lbl = opt.get("label", "")
                            if desired_value in val or desired_value in lbl:
                                # Click it
                                selector = f"input[type='radio'][name='{name}'][value='{opt['value']}']"
                                if opt.get("id"):
                                    selector = f"#{opt['id']}"
                                radio_el = await page.query_selector(selector)
                                if radio_el:
                                    await radio_el.click()
                                    logger.info("Selected radio '%s' = '%s'", name, opt.get("value"))
                                break
                        else:
                            # No exact match — select first option
                            if options:
                                first = options[0]
                                selector = f"input[type='radio'][name='{name}'][value='{first['value']}']"
                                if first.get("id"):
                                    selector = f"#{first['id']}"
                                radio_el = await page.query_selector(selector)
                                if radio_el:
                                    await radio_el.click()
                                    logger.info("Selected first radio for '%s'", name)
                        break
        except Exception as exc:
            logger.debug("Radio button handling error: %s", exc)

    async def _fill_date_of_birth(self, page: Page) -> None:
        """Auto-fill date of birth fields with a generic adult date."""
        try:
            for selector in _DOB_SELECTORS:
                dob_elements = await page.query_selector_all(selector)
                for dob_el in dob_elements:
                    if not await dob_el.is_visible():
                        continue
                    name = (await dob_el.get_attribute("name") or "").lower()
                    tag = await dob_el.evaluate("el => el.tagName")

                    if tag == "SELECT":
                        # Handle day/month/year dropdowns
                        options = await dob_el.evaluate("""
                            (sel) => Array.from(sel.options).map(o => ({
                                text: o.text.trim(), value: o.value
                            }))
                        """)
                        if not options or len(options) <= 1:
                            continue

                        if "day" in name:
                            # Select day 15
                            for opt in options:
                                if opt["value"] == "15" or opt["text"] == "15":
                                    await dob_el.select_option(value=opt["value"])
                                    break
                        elif "month" in name:
                            # Select a middle month
                            mid = len(options) // 2
                            if mid < len(options):
                                await dob_el.select_option(value=options[mid]["value"])
                        elif "year" in name:
                            # Select a year that makes us ~25 years old
                            import datetime
                            target_year = str(datetime.datetime.now().year - 25)
                            for opt in options:
                                if opt["value"] == target_year or opt["text"] == target_year:
                                    await dob_el.select_option(value=opt["value"])
                                    break
                            else:
                                # Fallback: pick middle option
                                if mid < len(options):
                                    await dob_el.select_option(value=options[mid]["value"])
                        logger.info("Filled DOB field '%s'", name)
                    elif tag == "INPUT":
                        # Date input — fill with 1998-06-15
                        input_type = await dob_el.get_attribute("type") or ""
                        if input_type == "date":
                            await dob_el.fill("1998-06-15")
                        else:
                            # Text input for DOB — try common formats
                            await dob_el.fill("06/15/1998")
                        logger.info("Filled DOB input '%s'", name)
        except Exception as exc:
            logger.debug("DOB handling error: %s", exc)

    async def _fill_phone_number(self, page: Page) -> None:
        """Fill phone fields on a real registration form with a generic number."""
        try:
            for selector in _PHONE_SELECTORS:
                phone_inputs = await page.query_selector_all(selector)
                for phone_el in phone_inputs:
                    if not await phone_el.is_visible():
                        continue
                    # Only fill when a real signup form is present (email or
                    # password field exists) — otherwise skip entirely; the
                    # site has no signup and we must not fabricate data.
                    has_signup_form = bool(
                        await page.query_selector("input[type='email'], input[type='password']")
                    )
                    if not has_signup_form:
                        logger.info("Phone field present but no signup form — not filling")
                        return
                    # Check if already filled
                    current = await phone_el.evaluate("el => el.value")
                    if current:
                        continue
                    await phone_el.fill("2025551234")
                    logger.info("Filled phone number field")
                    break
        except Exception as exc:
            logger.debug("Phone number handling error: %s", exc)

    async def _handle_confirm_password_by_position(self, page: Page) -> bool:
        """Find and fill confirm password by position (second password field)."""
        try:
            password_fields = await page.query_selector_all("input[type='password']")
            visible_passwords = []
            for pf in password_fields:
                if await pf.is_visible():
                    visible_passwords.append(pf)

            # If there are 2+ visible password fields, the second is likely confirm
            if len(visible_passwords) >= 2:
                confirm = visible_passwords[1]
                current = await confirm.evaluate("el => el.value")
                if not current:
                    await confirm.fill(_DEFAULT_SIGNUP_PASSWORD)
                    logger.info("Filled confirm password by position (2nd password field)")
                    return True
        except Exception as exc:
            logger.debug("Confirm password by position error: %s", exc)
        return False

    def _generate_username(self, email: str, domain: str = "") -> str:
        """Generate a signup username: 'duka' + 5 random digits (e.g. duka48213)."""
        import random
        return f"duka{random.randint(10000, 99999)}"

    async def _handle_multi_step_registration(self, page: Page, email: str, password: str) -> dict:
        """Handle multi-step registration wizards (step-by-step forms).

        Many forums and sites use wizard-style registration:
          Step 1: Email + Username
          Step 2: Password + Confirm Password
          Step 3: Profile details (DOB, country, etc.)
          Step 4: Terms agreement

        Returns dict with: success, message, needs_verification
        """
        result = {"success": False, "message": "", "needs_verification": False}

        try:
            _generated_username = self._generate_username(email)
            max_steps = 5  # Safety limit

            for step in range(max_steps):
                current_url = page.url.lower()

                logger.info("Multi-step registration: step %d, url=%s", step + 1, current_url[:80])

                # Detect what this step needs
                has_email_field = bool(
                    await page.query_selector(
                        "input[type='email'], input[name='email'], input[name='register_email']"
                    )
                )
                has_password_field = bool(
                    await page.query_selector("input[type='password']")
                )
                has_username_field = bool(
                    await page.query_selector(
                        "input[name='username'], input[name='register_username'], "
                        "input[id='ctrl_username']"
                    )
                )

                # No registration fields anywhere on this page — this is not a
                # signup flow at all (e.g. a plain news homepage). Bail out
                # instead of pretending the wizard completed.
                if not (has_email_field or has_password_field or has_username_field):
                    logger.info(
                        "Step %d: page has no email/username/password fields — "
                        "no registration form exists on this site",
                        step + 1,
                    )
                    result["message"] = "No signup form found on page"
                    return result

                # Fill available fields
                if has_email_field:
                    email_input = await page.query_selector(
                        "input[type='email'], input[name='email'], input[name='register_email']"
                    )
                    if email_input and not await email_input.evaluate("el => el.value"):
                        await email_input.fill(email)
                        logger.info("Step %d: Filled email", step + 1)

                if has_username_field:
                    username_input = await page.query_selector(
                        "input[name='username'], input[name='register_username'], "
                        "input[id='ctrl_username']"
                    )
                    if username_input and not await username_input.evaluate("el => el.value"):
                        await username_input.fill(_generated_username)
                        logger.info("Step %d: Filled username '%s'", step + 1, _generated_username)

                if has_password_field:
                    password_inputs = await page.query_selector_all("input[type='password']")
                    for i, pw in enumerate(password_inputs):
                        if await pw.is_visible() and not await pw.evaluate("el => el.value"):
                            await pw.fill(password)
                            logger.info("Step %d: Filled password field %d", step + 1, i + 1)

                # Fill name fields if present (Duka S.)
                _name_field_map = (
                    ("input[name*='first_name' i], input[name*='firstname' i], input[placeholder*='first name' i]", _SIGNUP_FIRST_NAME),
                    ("input[name*='last_name' i], input[name*='lastname' i], input[name*='surname' i], input[placeholder*='last name' i]", _SIGNUP_LAST_NAME),
                    ("input[name*='full_name' i], input[name*='fullname' i], input[placeholder*='full name' i], input[name='name' i]", _SIGNUP_FULL_NAME),
                )
                for _name_sel, _name_val in _name_field_map:
                    try:
                        _name_input = await page.query_selector(_name_sel)
                        if _name_input and await _name_input.is_visible() \
                                and not await _name_input.evaluate("el => el.value"):
                            await _name_input.fill(_name_val)
                            logger.info("Step %d: Filled name field '%s'", step + 1, _name_val)
                    except Exception:
                        continue

                # Fill other fields (dropdowns, radios, DOB, phone)
                await self._fill_dropdowns(page)
                await self._fill_radio_buttons(page)
                await self._fill_date_of_birth(page)
                await self._fill_phone_number(page)
                await self._tick_agreement_checkboxes(page)
                await self._answer_bot_qa_fields(page)

                # Find and click Next/Continue/Submit button
                next_selectors = [
                    "button:has-text('Next')",
                    "button:has-text('Continue')",
                    "button:has-text('Proceed')",
                    "input[type='submit'][value*='Next' i]",
                    "input[type='submit'][value*='Continue' i]",
                    "button[type='submit']",
                    "input[type='submit']",
                ]
                submit_btn = None
                for sel in next_selectors:
                    submit_btn = await page.query_selector(sel)
                    if submit_btn and await submit_btn.is_visible():
                        break
                    submit_btn = None

                if not submit_btn:
                    # A wizard only "completes without a submit button" if this
                    # step actually had registration fields to fill. Otherwise
                    # there was never a form here (e.g. a news homepage).
                    if has_email_field or has_password_field or has_username_field:
                        logger.info("Step %d: No next/submit button — registration may be complete", step + 1)
                        break
                    logger.info(
                        "Step %d: No submit button and no registration fields — "
                        "no signup form exists on this site",
                        step + 1,
                    )
                    result["message"] = "No signup form found on page"
                    return result

                await submit_btn.click()
                await asyncio.sleep(3)

                # Wait for navigation
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                except Exception:
                    pass

                # Check if we're done (no more form fields)
                new_text = await self._get_page_text(page)
                still_has_form = bool(
                    await page.query_selector("input[type='email'], input[type='password'], input[type='text']")
                )

                # Phone/SMS verification demanded → treat signup as failed
                if self._check_markers(new_text, _PHONE_VERIFICATION_MARKERS):
                    logger.warning(
                        "[%s] Site requires phone/SMS verification (multi-step) — marking signup as FAILED",
                        email,
                    )
                    result["success"] = False
                    result["needs_verification"] = False
                    result["message"] = (
                        "Signup failed: site requires phone/SMS verification, which is not supported"
                    )
                    return result

                # Check for verification needed
                if self._check_markers(new_text, _VERIFICATION_NEEDED_MARKERS):
                    result["success"] = True
                    result["needs_verification"] = True
                    result["message"] = f"Multi-step registration complete after {step + 1} steps — verification needed"
                    return result

                # Explicit error feedback (e.g. "Wrong solved puzzle") — the
                # site rejected the submission. Never report success on a page
                # that shows an error, no matter what else it says.
                if self._check_markers(new_text, _SUBMIT_ERROR_MARKERS):
                    result["success"] = False
                    result["message"] = (
                        "Signup failed: site rejected the registration "
                        "(error feedback detected on page)"
                    )
                    logger.warning(
                        "[%s] Multi-step submit rejected — error markers on page: %s",
                        email, new_text[:300],
                    )
                    return result

                # Check for success — with stronger evidence than the single
                # word "welcome" (login prompts say "Welcome back" right on
                # the failed registration page).
                if self._check_markers(new_text, _MULTI_STEP_SUCCESS_MARKERS):
                    result["success"] = True
                    result["message"] = f"Multi-step registration complete after {step + 1} steps"
                    return result

                # If no form fields remain, we're done
                if not still_has_form:
                    result["success"] = True
                    result["message"] = f"Multi-step registration complete after {step + 1} steps (no more form fields)"
                    return result

            # If we exhausted all steps and the form is STILL there, the
            # registration was never confirmed — do not report success.
            result["success"] = False
            result["message"] = (
                f"Signup failed: registration form still present after "
                f"{max_steps} steps — submission was never confirmed"
            )

        except Exception as exc:
            result["message"] = f"Multi-step registration error: {exc}"
            logger.error("Multi-step registration exception: %s", exc, exc_info=True)

        return result

    # ------------------------------------------------------------------
    # Q&A handler
    # ------------------------------------------------------------------

    async def _answer_bot_qa_fields(self, page: Page) -> None:
        """Detect and answer anti-bot Q&A fields (e.g., XenForo's "What color is the sky?")."""
        try:
            # Look for Q&A text fields (XenForo uses name='q_and_a' or similar)
            qa_selectors = [
                "input[name='q_and_a']",
                "input[name*='question']",
                "input[name*='qa']",
                "input[name*='captcha_answer']",
                "input[id*='q_and_a']",
            ]

            for sel in qa_selectors:
                qa_input = await page.query_selector(sel)
                if qa_input and await qa_input.is_visible():
                    # Find the associated question text from the DOM
                    question_text = await page.evaluate("""
                        (sel) => {
                            const el = document.querySelector(sel);
                            if (!el) return '';
                            const id = el.id;
                            if (id) {
                                const label = document.querySelector('label[for="' + id + '"]');
                                if (label) return label.innerText;
                            }
                            const parent = el.closest('.ipsFieldRow, .ipsField, .form-group, .formRow, div');
                            if (parent) {
                                const labels = parent.querySelectorAll('label, .ipsFieldRow_title, .ipsType_reset, h3, h4, p, dt');
                                for (const l of labels) {
                                    if (l.innerText.trim().length > 3) return l.innerText;
                                }
                            }
                            return '';
                        }
                    """, sel)

                    logger.info("[%s] Found anti-bot Q&A field, question: '%s'", sel, question_text)

                    # Compute the answer
                    answer = self._compute_qa_answer(question_text)

                    if answer:
                        await qa_input.fill(answer)
                        logger.info("[%s] Answered Q&A: '%s' -> '%s'", sel, question_text, answer)
                    else:
                        # Smart fallback: try common answers in order of likelihood
                        _FALLBACKS = ['blue', '4', 'paris', '2', '1', 'yes']
                        fallback = _FALLBACKS[0]
                        await qa_input.fill(fallback)
                        logger.warning(
                            "[%s] Unknown Q&A question: '%s' — using fallback '%s'",
                            sel, question_text, fallback,
                        )

                    break  # Only fill the first Q&A field

        except Exception as exc:
            logger.debug("Q&A field handling error: %s", exc)

    def _compute_qa_answer(self, question_text: str) -> str | None:
        """Compute the answer to an anti-bot Q&A question.

        Handles math (numeric + word-based), colors, capitals,
        general knowledge, and days-of-the-week.
        """
        if not question_text:
            return None

        text_lower = question_text.lower().strip()

        # ------------------------------------------------------------------
        # 1. Numeric math: "5+2", "5 + 2 = ?", "compute 3/2", "10 % 3"
        # ------------------------------------------------------------------
        for pattern, op in [
            (r'(\d+)\s*\+\s*(\d+)', '+'),
            (r'(\d+)\s*-\s*(\d+)', '-'),
            (r'(\d+)\s*\*\s*(\d+)', '*'),
            (r'(\d+)\s*/\s*(\d+)', '/'),
            (r'(\d+)\s*÷\s*(\d+)', '/'),
            (r'(\d+)\s*%\s*(\d+)', '%'),
        ]:
            m = re.search(pattern, text_lower)
            if m:
                a, b = int(m.group(1)), int(m.group(2))
                if op == '+':
                    return str(a + b)
                elif op == '-':
                    return str(a - b)
                elif op == '*':
                    return str(a * b)
                elif op == '/':
                    return str(a // b) if b != 0 else None
                elif op == '%':
                    return str(a % b) if b != 0 else None

        # ------------------------------------------------------------------
        # 2. Word-to-number math
        #    "What is five plus two?" / "twelve minus three"
        # ------------------------------------------------------------------
        _WORD_NUMS = {
            'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4,
            'five': 5, 'six': 6, 'seven': 7, 'eight': 8, 'nine': 9,
            'ten': 10, 'eleven': 11, 'twelve': 12, 'thirteen': 13,
            'fourteen': 14, 'fifteen': 15, 'sixteen': 16, 'seventeen': 17,
            'eighteen': 18, 'nineteen': 19, 'twenty': 20,
        }
        word_ops = [
            (r'(\w+)\s+plus\s+(\w+)', '+'),
            (r'(\w+)\s+minus\s+(\w+)', '-'),
            (r'(\w+)\s+times\s+(\w+)', '*'),
            (r'(\w+)\s+multiplied\s+by\s+(\w+)', '*'),
            (r'(\w+)\s+divided\s+by\s+(\w+)', '/'),
            (r'(\w+)\s+over\s+(\w+)', '/'),
        ]
        for pattern, op in word_ops:
            m = re.search(pattern, text_lower)
            if m:
                a = _WORD_NUMS.get(m.group(1))
                b = _WORD_NUMS.get(m.group(2))
                if a is not None and b is not None:
                    if op == '+':
                        return str(a + b)
                    elif op == '-':
                        return str(a - b)
                    elif op == '*':
                        return str(a * b)
                    elif op == '/':
                        return str(a // b) if b != 0 else None

        # ------------------------------------------------------------------
        # 3. Q&A dictionary — two-pass matching:
        #    Pass 1: specific object / phrase keywords (sky, grass, capital …)
        #    Pass 2: generic fallbacks ("what color" → blue)
        #    This ensures "What color is grass?" matches "grass" → green,
        #    not "what color" → blue.
        # ------------------------------------------------------------------
        _QA_SPECIFIC = {
            # --- Colors (specific objects) ---
            'sky': 'blue', 'sun': 'yellow', 'grass': 'green',
            'snow': 'white', 'fire': 'red', 'sea': 'blue',
            'ocean': 'blue', 'cloud': 'white', 'banana': 'yellow',
            'apple': 'red', 'leaf': 'green', 'night': 'black',
            'coal': 'black', 'milk': 'white', 'cherry': 'red',
            'orange': 'orange', 'lemon': 'yellow', 'lime': 'green',
            'tomato': 'red', 'carrot': 'orange', 'eggplant': 'purple',
            'lavender': 'purple', 'ruby': 'red', 'emerald': 'green',
            'sapphire': 'blue', 'gold': 'yellow', 'silver': 'gray',
            # --- World capitals ---
            'capital of france': 'paris',
            'capital of ethiopia': 'addis ababa',
            'capital of japan': 'tokyo',
            'capital of usa': 'washington dc',
            'capital of us': 'washington dc',
            'capital of united states': 'washington dc',
            'capital of united kingdom': 'london',
            'capital of uk': 'london',
            'capital of england': 'london',
            'capital of germany': 'berlin',
            'capital of italy': 'rome',
            'capital of spain': 'madrid',
            'capital of portugal': 'lisbon',
            'capital of russia': 'moscow',
            'capital of china': 'beijing',
            'capital of india': 'new delhi',
            'capital of brazil': 'brasilia',
            'capital of canada': 'ottawa',
            'capital of australia': 'canberra',
            'capital of egypt': 'cairo',
            'capital of south africa': 'pretoria',
            'capital of nigeria': 'abuja',
            'capital of kenya': 'nairobi',
            'capital of ghana': 'accra',
            'capital of tanzania': 'dodoma',
            'capital of sudan': 'khartoum',
            'capital of morocco': 'rabat',
            'capital of algeria': 'algiers',
            'capital of mexico': 'mexico city',
            'capital of argentina': 'buenos aires',
            'capital of colombia': 'bogota',
            'capital of peru': 'lima',
            'capital of turkey': 'ankara',
            'capital of south korea': 'seoul',
            'capital of north korea': 'pyongyang',
            'capital of thailand': 'bangkok',
            'capital of vietnam': 'hanoi',
            'capital of indonesia': 'jakarta',
            'capital of saudi arabia': 'riyadh',
            'capital of iran': 'tehran',
            'capital of iraq': 'baghdad',
            'capital of uae': 'abu dhabi',
            'capital of united arab emirates': 'abu dhabi',
            'capital of israel': 'jerusalem',
            'capital of greece': 'athens',
            'capital of poland': 'warsaw',
            'capital of ukraine': 'kyiv',
            'capital of sweden': 'stockholm',
            'capital of norway': 'oslo',
            'capital of finland': 'helsinki',
            'capital of denmark': 'copenhagen',
            'capital of netherlands': 'amsterdam',
            'capital of belgium': 'brussels',
            'capital of switzerland': 'berne',
            'capital of austria': 'vienna',
            'capital of ireland': 'dublin',
            'capital of scotland': 'edinburgh',
            'capital of new zealand': 'wellington',
            'capital of singapore': 'singapore',
            'capital of malaysia': 'kuala lumpur',
            'capital of philippines': 'manila',
            # --- General knowledge ---
            'largest ocean': 'pacific',
            'biggest ocean': 'pacific',
            'smallest ocean': 'arctic',
            'largest continent': 'asia',
            'smallest continent': 'australia',
            'largest desert': 'sahara',
            'longest river': 'nile',
            'highest mountain': 'everest',
            'tallest mountain': 'everest',
            'how many days in a week': '7',
            'how many days in a year': '365',
            'how many months': '12',
            'how many continents': '7',
            'how many oceans': '5',
            'how many planets': '8',
            'how many bones': '206',
            'days in a week': '7',
            'months in a year': '12',
            '2 + 2': '4', '2+2': '4',
            '2 * 2': '4', '2*2': '4',
        }

        _QA_GENERIC = {
            'what color': 'blue',
            'colour': 'blue',
        }

        # ------------------------------------------------------------------
        # 4. Day-of-week extraction: "What day comes after Monday?"
        #    Runs BEFORE Q&A dictionary to avoid false matches like "sun"
        #    inside "sunday".
        # ------------------------------------------------------------------
        _DAYS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        after_match = re.search(r'after\s+(\w+)', text_lower)
        if after_match:
            day = after_match.group(1)
            if day in _DAYS:
                idx = (_DAYS.index(day) + 1) % 7
                return _DAYS[idx]

        # Pass 1: specific matches (longest key first)
        for pattern, answer in sorted(_QA_SPECIFIC.items(), key=lambda kv: -len(kv[0])):
            if pattern in text_lower:
                return answer

        # Pass 2: generic fallbacks
        for pattern, answer in _QA_GENERIC.items():
            if pattern in text_lower:
                return answer

        # ------------------------------------------------------------------
        # 5. Smart fallback — try to extract any number from the question
        #    as a last resort (many anti-bot sites accept any number)
        # ------------------------------------------------------------------
        numbers = re.findall(r'\d+', text_lower)
        if numbers:
            # Return the first number found (common pattern in verification)
            return numbers[0]

        return None

    async def _is_cloudflare_challenge(self, page: Page) -> bool:
        """Detect if the page is a Cloudflare challenge."""
        cf_markers = [
            "Performing security verification",
            "Just a moment",
            "Checking your browser",
            "Verifying you are human",
            "_cf_chl_opt",
            "challenges.cloudflare.com",
        ]
        try:
            html_snippet = await page.evaluate(
                "() => document.title + ' ' + (document.body?.innerText || '').substring(0, 2000)"
            )
            for marker in cf_markers:
                if marker.lower() in html_snippet.lower():
                    return True
        except Exception:
            pass
        return False

    async def _try_solve_captcha(self, page: Page) -> bool:
        """Try to detect and solve CAPTCHA challenges on the page.

        Uses the same solver pipeline as the deep worker:
          1. Cloudflare Turnstile (iframe checkbox) with retries
          2. Turnstile token extraction + forced trigger
          3. reCAPTCHA v2 (audio challenge)
        """
        # --- Cloudflare Turnstile with retries ---
        cf_iframe_selectors = [
            "iframe[src*='challenges.cloudflare.com']",
            "iframe[src*='turnstile']",
            "iframe[src*='cf-chl']",
        ]

        for attempt in range(3):
            for sel in cf_iframe_selectors:
                iframe_el = await page.query_selector(sel)
                if not iframe_el:
                    continue
                try:
                    frame = await iframe_el.content_frame()
                    if not frame:
                        continue
                    # Try multiple checkbox selectors within the iframe
                    checkbox = None
                    for cb_sel in ["input[type='checkbox']", "#challenge-stage", ".cb-lb", "label.cb-lb", "#cf-hcaptcha-container"]:
                        checkbox = await frame.query_selector(cb_sel)
                        if checkbox:
                            break
                    if checkbox:
                        await asyncio.sleep(1)
                        await checkbox.click()
                        logger.info("Clicked Cloudflare Turnstile checkbox (attempt %d)", attempt + 1)
                        # Wait for token to populate after click
                        for _ in range(4):  # up to 20s post-click
                            await asyncio.sleep(5)
                            token = await page.evaluate("""
                                () => {
                                    const el = document.querySelector('input[name="cf-turnstile-response"]');
                                    return el ? el.value : '';
                                }
                            """)
                            if token and len(token) > 10:
                                logger.info("Turnstile token populated after click (attempt %d)", attempt + 1)
                                return True
                        logger.warning("Turnstile token not populated after attempt %d — retrying", attempt + 1)
                except Exception as exc:
                    logger.debug("Turnstile interaction error (attempt %d): %s", attempt + 1, exc)
            # Brief pause before retry
            if attempt < 2:
                await asyncio.sleep(2)

        # --- Try triggering Turnstile via JS (evaluate on page) ---
        try:
            triggered = await page.evaluate("""
                () => {
                    if (window.turnstile) {
                        const widgets = document.querySelectorAll('.cf-turnstile, [data-sitekey]');
                        for (const w of widgets) {
                            try { turnstile.execute(w); } catch(e) {}
                        }
                        return widgets.length;
                    }
                    return 0;
                }
            """)
            if triggered:
                logger.info("Triggered turnstile.execute() on %d widgets", triggered)
                await asyncio.sleep(8)
                # Check if token resolved
                token = await page.evaluate("""
                    () => {
                        const el = document.querySelector('input[name="cf-turnstile-response"]');
                        return el ? el.value : '';
                    }
                """)
                if token and len(token) > 10:
                    logger.info("Turnstile token resolved after JS trigger")
                    return True
        except Exception as exc:
            logger.debug("Turnstile JS trigger failed: %s", exc)

        # --- reCAPTCHA v2 (audio challenge) ---
        recaptcha_iframe = await page.query_selector("iframe[src*='recaptcha']")
        if recaptcha_iframe:
            logger.info("reCAPTCHA detected — trying audio solver")
            try:
                from playwright_recaptcha import recaptchav2
                async with recaptchav2.AsyncSolver(page) as solver:
                    await solver.solve_recaptcha()
                logger.info("reCAPTCHA solved")
                await asyncio.sleep(3)
                return True
            except Exception as exc:
                logger.debug("reCAPTCHA solver error (library may not be installed): %s", exc)

        return False

    async def _handle_verification(
        self,
        page: Page,
        email: str,
        domain: str,
    ) -> bool:
        """Handle email verification by polling the inbox and navigating to the
        verification link (or filling the code input).

        Backend selection:
          - Gmail API when the account has Gmail OAuth secrets configured
          - IMAP fallback (seed inbox credentials from settings) otherwise —
            this covers Gmail plus-aliases (seed+dukaXXXXX@gmail.com) and any
            IMAP provider configured in .env
        """
        logger.info("[%s] Polling for verification email (address=%s)...", domain, email)

        verification: dict[str, Any] = {"found": False, "link": None, "code": None, "body": None}

        # --- Try Gmail API when OAuth is available for this account ---
        try:
            verification = await gmail_handler.poll_for_verification(
                email=email,
                sender_domain=domain,
                timeout_seconds=60,
                poll_interval=5.0,
            )
        except Exception:
            logger.debug("[%s] Gmail API polling failed — falling back to IMAP", domain, exc_info=True)
            verification = {"found": False, "link": None, "code": None, "body": None}

        # --- IMAP fallback (seed inbox) when Gmail API found nothing ---
        if not verification.get("found"):
            from app.services.gmail_verification import ImapVerificationReader, imap_poll_for_code
            try:
                reader = ImapVerificationReader()
                # Validate config before attempting a connection
                reader._connect()
                reader._disconnect()
                verification = await imap_poll_for_code(
                    sender_domain=domain,
                    timeout_seconds=90,
                )
            except Exception as imap_err:
                logger.warning(
                    "[%s] IMAP fallback unavailable (%s) — cannot read verification email",
                    domain, imap_err,
                )

        if not verification.get("found"):
            logger.warning("[%s] No verification email received", domain)
            return False

        # Navigate to verification link
        if verification["link"]:
            logger.info("[%s] Navigating to verification link", domain)
            try:
                await page.goto(verification["link"], wait_until="commit", timeout=15_000)
                await asyncio.sleep(3)

                # Check if verification succeeded
                page_text = await self._get_page_text(page)
                success_markers = [
                    "verified", "confirmed", "activated", "success",
                    "account is ready", "welcome", "dashboard",
                ]
                if self._check_markers(page_text, success_markers):
                    logger.info("[%s] Email verification successful", domain)
                    return True

                logger.info("[%s] Verification link visited — assuming success", domain)
                return True

            except Exception as exc:
                logger.error("[%s] Failed to navigate to verification link: %s", domain, exc)
                return False

        # If we got a code instead of a link, try to find and fill the code input
        if verification["code"]:
            logger.info("[%s] Using verification code: %s", domain, verification["code"])
            code_input = await page.query_selector(
                "input[name*='code' i], input[name*='otp' i], input[name*='verify' i], "
                "input[placeholder*='code' i], input[placeholder*='otp' i]"
            )
            if code_input:
                await code_input.fill(verification["code"])
                # Try to submit
                submit = await page.query_selector("button[type='submit']")
                if submit:
                    await submit.click()
                    await asyncio.sleep(3)
                    return True

        return False

    async def _get_page_text(self, page: Page) -> str:
        """Get visible text from the current page."""
        try:
            return await page.evaluate(
                "() => document.body ? document.body.innerText : ''"
            )
        except Exception:
            return ""

    def _check_markers(self, text: str, markers: list[str]) -> bool:
        """Check if any marker is present in the text."""
        text_lower = text.lower()
        return any(marker in text_lower for marker in markers)


# Singleton
auto_signup_handler = AutoSignupHandler()
