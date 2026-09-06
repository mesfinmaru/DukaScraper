import { useState } from "react"
import { Loader2, LogIn, Radar } from "lucide-react"
import { Link } from "react-router-dom"
import { ApiError } from "../api"
import { useAuth } from "../config"
import { ThemeToggle } from "../theme"

export default function LoginPage() {
  const { login } = useAuth()
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const submit = async (e: React.FormEvent) => {
    e.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await login(username.trim().toLowerCase(), password)
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Login failed. Please try again.")
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
            <Radar className="h-6 w-6 text-white" />
          </div>
          <div>
            <h1 className="text-lg font-bold tracking-tight text-slate-100">Duka Scraper</h1>
            <p className="mt-1 text-xs text-slate-400">Intelligent web intelligence, at your command.</p>
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
            <label htmlFor="login-user" className="label">
              Username
            </label>
            <input
              id="login-user"
              className="input"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="admin"
              autoComplete="username"
              autoFocus
              required
            />
          </div>

          <div>
            <label htmlFor="login-pass" className="label">
              Password
            </label>
            <input
              id="login-pass"
              type="password"
              className="input"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
              autoComplete="current-password"
              required
            />
          </div>

          <button type="submit" disabled={busy} className="btn-primary w-full py-2.5">
            {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <LogIn className="h-4 w-4" />}
            {busy ? "Signing in..." : "Sign in"}
          </button>

          <div className="text-center">
            <Link
              to="/forgot-password"
              className="cursor-pointer text-xs text-slate-500 transition hover:text-slate-300"
            >
              Forgot your password?
            </Link>
          </div>

        </form>
      </div>
    </div>
  )
}
