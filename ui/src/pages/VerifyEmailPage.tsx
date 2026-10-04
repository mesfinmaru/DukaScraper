import { useCallback, useEffect, useRef, useState } from "react"

import { emptyOtpDigits } from "../utils"
import { Loader2, MailCheck } from "lucide-react"
import { useLocation, useNavigate } from "react-router-dom"
import { api, ApiError } from "../api"
import { ThemeToggle } from "../theme"
import { Logo } from "../components/Logo"
import { OtpInput } from "../components/OtpInput"
import { Msg } from "../components/ui"

/**
 * Email verification reached from the "Verify Email" button in the welcome
 * email.
 *
 * The button only navigates here — it carries the account id, not a secret, and
 * verifies nothing on its own. Confirming the address requires the 6-digit code
 * from the same email, which is what this page collects.
 *
 * Reaching the page without an account id (someone typed the URL, or the id was
 * stripped) is not an error: the field below accepts it, so the user is never
 * locked out of verifying.
 */
export default function VerifyEmailPage() {
  const navigate = useNavigate()
  const location = useLocation()
  const mounted = useRef(true)

  const [userId, setUserId] = useState("")
  const [digits, setDigits] = useState<string[]>(emptyOtpDigits)
  const [error, setError] = useState<string | null>(null)
  const [done, setDone] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
    }
  }, [])

  // The link supplies ?user=<user_id>; also accept it as a query param so the
  // page works from either router style.
  useEffect(() => {
    const fromSearch = new URLSearchParams(location.search).get("user")
    const fromHash = new URLSearchParams(window.location.hash.split("?")[1] ?? "").get("user")
    const found = fromSearch || fromHash
    if (found) setUserId(found)
  }, [location.search])

  const submitCode = useCallback(
    async (code: string) => {
      const id = userId.trim()
      if (!id) {
        setError("Enter the user ID from your welcome email.")
        return
      }
      setBusy(true)
      setError(null)
      try {
        const res = await api.verifyEmailConfirm({ user_id: id, code })
        if (!mounted.current) return
        setDone(res.message)
        // Verified but not signed in: this page is reached from the email where
        // no password was entered, so the user still signs in themselves.
        setTimeout(() => navigate("/login", { replace: true }), 1400)
      } catch (err) {
        if (!mounted.current) return
        setError(
          err instanceof ApiError ? err.message : "That code did not work. Try again.",
        )
        setDigits(emptyOtpDigits())
      } finally {
        if (mounted.current) setBusy(false)
      }
    },
    [userId, navigate],
  )

  if (done) {
    return (
      <div className="relative flex min-h-screen items-center justify-center bg-black px-4">
        <div className="absolute top-4 right-4">
          <ThemeToggle />
        </div>
        <div className="w-full max-w-sm">
          <div className="mb-8 flex flex-col items-center gap-3 text-center">
            <Logo className="h-12 w-auto" />
          </div>
          <div className="rounded-2xl border border-slate-800 bg-[#050b08] p-6 shadow-2xl">
            <div className="flex flex-col items-center gap-3 text-center">
              <MailCheck className="h-8 w-8 text-emerald-500" />
              <p className="text-sm text-slate-300">{done}</p>
              <p className="text-xs text-slate-500">Taking you to sign in...</p>
            </div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="relative flex min-h-screen items-center justify-center bg-black px-4">
      <div className="absolute top-4 right-4">
        <ThemeToggle />
      </div>
      <div className="w-full max-w-sm">
        <div className="mb-8 flex flex-col items-center gap-3 text-center">
          <Logo className="h-12 w-auto" />
          <p className="mt-1 text-xs text-slate-400">Verify your email address</p>
        </div>

        <div className="space-y-5 rounded-2xl border border-slate-800 bg-[#050b08] p-6 shadow-2xl">
          <div className="flex flex-col items-center gap-1.5 text-center">
            <MailCheck className="h-8 w-8 text-emerald-500" />
            <p className="text-sm text-slate-300">
              Enter the 6-digit code from your welcome email
            </p>
          </div>

          {error && <Msg tone="error">{error}</Msg>}

          <div>
            <label htmlFor="verify-user" className="label">
              User ID
            </label>
            <input
              id="verify-user"
              className="input font-mono"
              value={userId}
              onChange={(e) => setUserId(e.target.value)}
              placeholder="USR12345"
              autoComplete="off"
            />
            <p className="mt-1.5 text-[11px] leading-relaxed text-slate-500">
              Your welcome email shows this. It is not your username.
            </p>
          </div>

          <OtpInput
            digits={digits}
            onDigitsChange={setDigits}
            onComplete={(code) => void submitCode(code)}
            disabled={busy}
            invalid={!!error}
          />

          <div className="text-center">
            <button
              type="button"
              onClick={() => void submitCode(digits.join(""))}
              disabled={busy || !digits.every(Boolean)}
              className="btn-primary w-full"
            >
              {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
              {busy ? "Checking..." : "Verify email"}
            </button>
          </div>

          <div className="text-center">
            <button
              type="button"
              onClick={() => navigate("/login", { replace: true })}
              className="cursor-pointer text-xs text-slate-500 transition hover:text-slate-300"
            >
              Back to sign in
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}