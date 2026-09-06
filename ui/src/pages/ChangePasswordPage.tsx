import { useState } from "react"
import { KeyRound, Loader2, Lock, LogOut, ShieldCheck } from "lucide-react"
import { ApiError } from "../api"
import { useAuth } from "../config"
import { ThemeToggle } from "../theme"

export default function ChangePasswordPage() {
  const { changePassword, logout } = useAuth()
  const [currentPassword, setCurrentPassword] = useState("")
  const [newPassword, setNewPassword] = useState("")
  const [confirmPassword, setConfirmPassword] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const submit = async (e: React.FormEvent) => {
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
      await changePassword(currentPassword, newPassword)
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to change password.")
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
          <div className="flex h-12 w-12 items-center justify-center rounded-xl bg-indigo-600">
            <Lock className="h-6 w-6 text-white" />
          </div>
          <div>
            <h1 className="text-lg font-bold tracking-tight text-slate-100">Set a new password</h1>
            <p className="mt-1 text-xs text-slate-400">
              You must change your password before continuing.
            </p>
          </div>
        </div>

        <form
          onSubmit={(e) => void submit(e)}
          className="rounded-2xl border border-slate-800 bg-[#050b08] p-6 shadow-2xl space-y-4"
        >
          {error && (
            <p className="rounded-lg border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-xs font-medium text-rose-400">
              {error}
            </p>
          )}

          <div>
            <label htmlFor="cp-current" className="label">
              Current password
            </label>
            <input
              id="cp-current"
              type="password"
              className="input"
              value={currentPassword}
              onChange={(e) => setCurrentPassword(e.target.value)}
              placeholder="••••••••"
              autoComplete="current-password"
              autoFocus
              required
            />
          </div>

          <div>
            <label htmlFor="cp-new" className="label">
              New password
            </label>
            <input
              id="cp-new"
              type="password"
              className="input"
              value={newPassword}
              onChange={(e) => setNewPassword(e.target.value)}
              placeholder="Min 8 characters"
              minLength={8}
              autoComplete="new-password"
              required
            />
          </div>

          <div>
            <label htmlFor="cp-confirm" className="label">
              Confirm new password
            </label>
            <input
              id="cp-confirm"
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

          <button type="submit" disabled={busy} className="btn-primary w-full py-2.5">
            {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <KeyRound className="h-4 w-4" />}
            {busy ? "Saving..." : "Change password"}
          </button>

          <button
            type="button"
            onClick={logout}
            className="flex w-full cursor-pointer items-center justify-center gap-1.5 text-xs text-slate-500 transition hover:text-slate-300"
          >
            <LogOut className="h-3.5 w-3.5" />
            Sign out
          </button>
        </form>

        <p className="mt-5 flex items-center justify-center gap-1.5 text-[11px] text-slate-500">
          <ShieldCheck className="h-3.5 w-3.5" />
          Your password is stored using PBKDF2 hashing and never shared.
        </p>
      </div>
    </div>
  )
}
