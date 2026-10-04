import { useEffect, useMemo, useState } from "react"
import { Loader2, MailCheck, RotateCw } from "lucide-react"
import { useLocation, useNavigate } from "react-router-dom"
import { api, ApiError } from "../api"
import { useAuth } from "../config"
import { ThemeToggle } from "../theme"
import { Logo } from "../components/Logo"
import { OtpInput } from "../components/OtpInput"
import { Msg } from "../components/ui"
import { emptyOtpDigits } from "../utils"


interface OtpLocationState {
  userId?: string
  maskedEmail?: string
  expiresIn?: number
}

/**
 * First-login email verification for admin-provisioned accounts.
 *
 * Reached straight from the login form (the router transitions here the
 * moment POST /login answers REQUIRES_VERIFICATION — the user never leaves
 * the app). Six single-digit boxes with auto-focus, auto-advance and
 * auto-submit: the request fires by itself once the sixth digit lands.
 * A 10-minute countdown mirrors the backend code expiry.
 */
export default function VerifyOtpPage() {
  const navigate = useNavigate()
  const location = useLocation()
  const { setVerifiedSession } = useAuth()
  const state = (location.state ?? {}) as OtpLocationState

  const [userId, setUserId] = useState(state.userId ?? "")
  const [maskedEmail, setMaskedEmail] = useState(state.maskedEmail ?? "")
  const [digits, setDigits] = useState<string[]>(emptyOtpDigits())
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [resendIn, setResendIn] = useState(30)
  const [expiresAt, setExpiresAt] = useState(() => Date.now() + (state.expiresIn ?? 600) * 1000)
  const [now, setNow] = useState(Date.now())
  // Deep link support: /verify-otp?user_id=usr_123 (also re-arms the timer).
  useEffect(() => {
    const qs = new URLSearchParams(window.location.hash.split("?")[1] ?? "")
    const fromQuery = qs.get("user_id")
    if (fromQuery) {
      setUserId(fromQuery)
      setExpiresAt(Date.now() + 600 * 1000)
    }
  }, [])

  // 1s tick for the countdowns.
  useEffect(() => {
    const t = setInterval(() => {
      setNow(Date.now())
      setResendIn((s) => (s > 0 ? s - 1 : 0))
    }, 1000)
    return () => clearInterval(t)
  }, [])

  const secondsLeft = Math.max(0, Math.floor((expiresAt - now) / 1000))
  const countdown = useMemo(() => {
    const m = Math.floor(secondsLeft / 60)
    const s = secondsLeft % 60
    return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
  }, [secondsLeft])

  const submitCode = async (code: string) => {
    if (!userId) {
      setError("This link has expired. Sign in again to get a new code.")
      return
    }
    setBusy(true)
    setError(null)
    try {
      const res = await api.verifyOtp(userId, code)
      // Verified + logged in: land straight in the app (password change is
      // enforced next by the router when must_change_password is set).
      setVerifiedSession({ token: res.access_token, refreshToken: res.refresh_token, user: res.user })
      navigate("/", { replace: true })
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "That code did not work. Try again.")
      // Bad code: clear and restart entry at the first box.
      setDigits(emptyOtpDigits())
    } finally {
      setBusy(false)
    }
  }

  const resend = async () => {
    setError(null)
    setBusy(true)
    try {
      const res = await api.resendOtp(userId)
      setMaskedEmail(res.masked_email)
      setExpiresAt(Date.now() + (res.expires_in_seconds ?? 600) * 1000)
      setResendIn(30)
      setDigits(emptyOtpDigits())
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Could not send a new code.")
    } finally {
      setBusy(false)
    }
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

        <div className="rounded-2xl border border-slate-800 bg-[#050b08] p-6 shadow-2xl space-y-5">
          <div className="flex flex-col items-center gap-1.5 text-center">
            <MailCheck className="h-8 w-8 text-emerald-500" />
            <p className="text-sm text-slate-300">
              We sent a 6-digit verification code
              {maskedEmail ? <> to <span className="font-semibold text-slate-100">{maskedEmail}</span></> : null}
            </p>
          </div>

          {error && <Msg tone="error">{error}</Msg>}

          <OtpInput
            digits={digits}
            onDigitsChange={setDigits}
            onComplete={(code) => void submitCode(code)}
            disabled={busy}
            invalid={!!error}
          />

          <p className="text-center text-xs text-slate-500">
            {secondsLeft > 0 ? (
              <>
                Code expires in <span className="font-mono font-semibold text-slate-300">{countdown}</span>
              </>
            ) : (
              <span className="text-amber-500">Code expired — request a new one below.</span>
            )}
          </p>

          <div className="text-center">
            <button
              type="button"
              onClick={() => void resend()}
              disabled={busy || resendIn > 0 || !userId}
              className="inline-flex cursor-pointer items-center gap-1.5 text-xs text-emerald-500 transition hover:text-emerald-400 disabled:cursor-not-allowed disabled:opacity-40"
            >
              {busy ? <Loader2 className="h-3 w-3 animate-spin" /> : <RotateCw className="h-3 w-3" />}
              {resendIn > 0 ? `Resend code available in ${resendIn}s` : "Didn't receive a code? Resend Code"}
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
