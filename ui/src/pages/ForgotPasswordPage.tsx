import { useState } from "react"
import { ArrowLeft, KeyRound, Loader2, Mail, ShieldCheck } from "lucide-react"
import { Link, useNavigate } from "react-router-dom"
import { api, ApiError } from "../api"
import { ThemeToggle } from "../theme"
import { Logo } from "../components/Logo"

export default function ForgotPasswordPage() {
  const navigate = useNavigate()
  const [step, setStep] = useState<1 | 2 | 3>(1)
  const [email, setEmail] = useState("")
  const [code, setCode] = useState("")
  const [newPassword, setNewPassword] = useState("")
  const [confirmPassword, setConfirmPassword] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const requestCode = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    const normalized = email.trim().toLowerCase()
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(normalized)) {
      setError("Enter a valid email address.")
      return
    }
    setBusy(true)
    try {
      const res = await api.forgotPassword(normalized)
      setNotice(res.message)
      setStep(2)
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to request a reset code.")
    } finally {
      setBusy(false)
    }
  }

  const verifyCode = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    if (!/^\d{6}$/.test(code.trim())) {
      setError("Enter the 6-digit reset code.")
      return
    }
    setStep(3)
  }

  const submitNewPassword = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    if (newPassword.length < 8) {
      setError("New password must be at least 8 characters.")
      return
    }
    if (newPassword !== confirmPassword) {
      setError("New passwords do not match.")
      return
    }
    setBusy(true)
    try {
      const res = await api.resetPassword(email.trim().toLowerCase(), code.trim(), newPassword)
      setNotice(res.message)
      navigate("/", { replace: true })
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to reset password.")
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
          <div>
            <h1 className="text-lg font-bold tracking-tight text-slate-100">Reset your password</h1>
            <p className="mt-1 text-xs text-slate-400">
              {step === 1
                ? "Enter your account email to receive a reset code."
                : step === 2
                  ? "Enter the 6-digit code we emailed you."
                  : "Choose a new password for your account."}
            </p>
          </div>
        </div>

        <form
          onSubmit={(e) => {
            if (step === 1) void requestCode(e)
            else if (step === 2) void verifyCode(e)
            else void submitNewPassword(e)
          }}
          className="rounded-2xl border border-slate-800 bg-[#050b08] p-6 shadow-2xl space-y-4"
        >
          {error && (
            <p className="rounded-lg border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-xs font-medium text-rose-400">
              {error}
            </p>
          )}
          {notice && step !== 3 && (
            <p className="rounded-lg border border-emerald-500/40 bg-emerald-500/10 px-3 py-2 text-xs font-medium text-emerald-600">
              {notice}
            </p>
          )}

          {step === 1 && (
            <>
              <div>
                <label htmlFor="fp-email" className="label">
                  Email
                </label>
                <input
                  id="fp-email"
                  type="email"
                  className="input font-mono"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="alice@example.com"
                  autoComplete="email"
                  autoFocus
                  required
                />
              </div>
            </>
          )}

          {step === 2 && (
            <>
              <div>
                <label htmlFor="fp-code" className="label">
                  Reset code
                </label>
                <input
                  id="fp-code"
                  className="input text-center font-mono text-lg tracking-[0.4em]"
                  value={code}
                  onChange={(e) => setCode(e.target.value.replace(/\D/g, "").slice(0, 6))}
                  placeholder="••••••"
                  inputMode="numeric"
                  autoComplete="one-time-code"
                  maxLength={6}
                  pattern="[0-9]{6}"
                  autoFocus
                  required
                />
              </div>
            </>
          )}

          {step === 3 && (
            <>
              <div>
                <label htmlFor="fp-new" className="label">
                  New password
                </label>
                <input
                  id="fp-new"
                  type="password"
                  className="input"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  placeholder="Min 8 characters"
                  minLength={8}
                  autoComplete="new-password"
                  autoFocus
                  required
                />
              </div>
              <div>
                <label htmlFor="fp-confirm" className="label">
                  Confirm new password
                </label>
                <input
                  id="fp-confirm"
                  type="password"
                  className="input"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  placeholder="••••••••"
                  minLength={8}
                  autoComplete="new-password"
                  required
                />
              </div>
            </>
          )}

          <button type="submit" disabled={busy} className="btn-primary w-full py-2.5">
            {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <KeyRound className="h-4 w-4" />}
            {busy
              ? "Please wait..."
              : step === 1
                ? "Send reset code"
                : step === 2
                  ? "Continue"
                  : "Reset password"}
          </button>

          <Link
            to="/"
            className="flex w-full items-center justify-center gap-1.5 text-xs text-slate-500 transition hover:text-slate-300"
          >
            <ArrowLeft className="h-3.5 w-3.5" />
            Back to sign in
          </Link>
        </form>

        <p className="mt-5 flex items-center justify-center gap-1.5 text-[11px] text-slate-500">
          <Mail className="h-3.5 w-3.5" />
          The reset code expires after a few minutes.
        </p>

        <p className="mt-2 flex items-center justify-center gap-1.5 text-[11px] text-slate-500">
          <ShieldCheck className="h-3.5 w-3.5" />
          For security, we never reveal whether an email is registered.
        </p>
      </div>
    </div>
  )
}
