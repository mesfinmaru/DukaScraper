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
    "a:has-text('Join')",
    "a[href*='signup']",
    "a[href*='register']",
    "a[href*='create-account']",
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
    ) -> dict[str, Any]:
        """Main entry point: detect login vs signup and handle accordingly.

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
        signup_result = await self._try_signup(page, email, password, portal_config)
        result.update(signup_result)

        if signup_result["success"]:
            result["action"] = "signup"
            await credential_service.record_usage(
                email, domain, "signup", "success"
            )

            if signup_result.get("needs_verification"):
                result["needs_verification"] = True
                # Handle verification
                verified = await self._handle_verification(
                    page, email, domain
                )
                result["verified"] = verified
                if verified:
                    await credential_service.record_usage(
                        email, domain, "verified", "success"
                    )
                else:
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
        self, page: Page, email: str, password: str, portal_config: dict | None = None
    ) -> dict[str, Any]:
        """Attempt to find and complete a signup form."""
        result: dict[str, Any] = {"success": False, "message": "", "needs_verification": False}

        try:
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

            # --- CAPTCHA solving (before form fill) ---
            # Only run CAPTCHA solving if the page is a Cloudflare challenge,
            # NOT if we're on the actual registration form with CAPTCHA widgets.
            # The Turnstile/reCAPTCHA on registration pages is a form element,
            # not a blocking challenge — solving it prematurely can navigate away.
            is_cf = await self._is_cloudflare_challenge(page)
            if is_cf:
                logger.info("[%s] Cloudflare challenge detected before form fill — solving", email)
                await self._try_solve_captcha(page)

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

            # Find all input fields — include both visible and hidden (some forms use JS to reveal)
            all_inputs = all_raw_inputs
            username_filled = False
            email_filled = False
            password_filled = False
            confirm_password_filled = False

            # Generate a username from the email for forum registrations
            _generated_username = email.split("@")[0].replace(".", "").replace("+", "_")

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
                result["message"] = f"Could not find all signup fields (email={email_filled}, password={password_filled})"
                return result

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

            # Check for verification needed
            if self._check_markers(page_text, _VERIFICATION_NEEDED_MARKERS):
                result["success"] = True
                result["needs_verification"] = True
                result["message"] = "Signup successful — verification email sent"
                return result

            # Check for errors
            error_markers = [
                "error", "failed", "invalid", "required", "already",
                "taken", "not available", "too short", "weak password",
                "doka is already", "that name is not valid",
            ]
            if self._check_markers(page_text, error_markers):
                # Narrow down: check if error is from a specific field
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
                # Assume success if no clear error
                result["success"] = True
                result["message"] = "Signup submitted (no clear error detected)"

        except Exception as exc:
            result["message"] = f"Signup error: {exc}"
            logger.error("[%s] Signup exception: %s", email, exc, exc_info=True)

        return result

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
        """Handle email verification by polling Gmail and navigating to verification link."""
        logger.info("[%s] Polling for verification email...", domain)

        verification = await gmail_handler.poll_for_verification(
            email=email,
            sender_domain=domain,
            timeout_seconds=90,
            poll_interval=5.0,
        )

        if not verification["found"]:
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
