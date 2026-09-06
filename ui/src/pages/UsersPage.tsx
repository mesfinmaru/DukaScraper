import { useCallback, useEffect, useState } from "react"
import {
  Ban,
  CheckCircle2,
  KeyRound,
  Loader2,
  Mail,
  Power,
  RefreshCcw,
  RotateCw,
  ShieldCheck,
  Trash2,
  UserPlus,
  Users,
} from "lucide-react"
import { api, ApiError } from "../api"
import { useAuth } from "../config"
import type { AdminUserRow, VerifyEmailResponse } from "../types"
import { cn, formatDateTime } from "../utils"
import { EmptyState, ErrorBanner, LoadingBlock, PageHeader } from "../components/ui"

export default function UsersPage() {
  const { session } = useAuth()
  const [users, setUsers] = useState<AdminUserRow[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [step, setStep] = useState<1 | 2 | 3>(1)
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [code, setCode] = useState("")
  const [verifyInfo, setVerifyInfo] = useState<VerifyEmailResponse | null>(null)
  const [token, setToken] = useState<string | null>(null)

  const [newUsername, setNewUsername] = useState("")
  const [newFullName, setNewFullName] = useState("")
  const [newRole, setNewRole] = useState<"user" | "admin">("user")
  const [forceReset, setForceReset] = useState(true)

  const [busy, setBusy] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const [createdMsg, setCreatedMsg] = useState<string | null>(null)

  const [toggling, setToggling] = useState<string | null>(null)
  const [resetting, setResetting] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const res = await api.listUsers()
      setUsers(res.users)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load users")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const resetFlow = () => {
    setStep(1)
    setEmail("")
    setPassword("")
    setCode("")
    setVerifyInfo(null)
    setToken(null)
    setFormError(null)
    setForceReset(true)
  }

  const sendCode = async () => {
    setFormError(null)
    const normalized = email.trim().toLowerCase()
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(normalized)) {
      setFormError("Enter a valid email address.")
      return
    }
    if (password.length < 8) {
      setFormError("Password must be at least 8 characters.")
      return
    }
    setBusy(true)
    try {
      const res = await api.sendVerificationCode(normalized, password)
      setVerifyInfo(res)
      setToken(null)
      setStep(2)
    } catch (err) {
      setFormError(err instanceof ApiError ? err.message : "Failed to send verification code")
    } finally {
      setBusy(false)
    }
  }

  const confirmCode = async () => {
    setFormError(null)
    if (!/^\d{6}$/.test(code.trim())) {
      setFormError("Enter the 6-digit verification code.")
      return
    }
    setBusy(true)
    try {
      const res = await api.confirmVerificationCode(email.trim().toLowerCase(), code.trim())
      setToken(res.token)
      setStep(3)
    } catch (err) {
      setFormError(err instanceof ApiError ? err.message : "Failed to confirm the verification code")
    } finally {
      setBusy(false)
    }
  }

  const createAccount = async () => {
    setFormError(null)
    if (!token) {
      setFormError("Email verification is incomplete. Start over.")
      return
    }
    setBusy(true)
    try {
      const res = await api.completeUserCreation({
        email: email.trim().toLowerCase(),
        username: newUsername.trim(),
        full_name: newFullName.trim(),
        role: newRole,
        token,
        must_change_password: forceReset,
      })
      setCreatedMsg(res.message || `Account "${res.user.username}" created and active.`)
      resetFlow()
      await load()
    } catch (err) {
      setFormError(err instanceof ApiError ? err.message : "Failed to create the account")
    } finally {
      setBusy(false)
    }
  }

  const remove = async (username: string) => {
    if (!window.confirm(`Delete user "${username}" and all of their jobs?`)) return
    try {
      await api.deleteUser(username)
      await load()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to delete user")
    }
  }

  const toggleActive = async (username: string, active: boolean) => {
    if (!window.confirm(`Are you sure you want to ${active ? "enable" : "disable"} user "${username}"?`)) return
    setToggling(username)
    try {
      await api.setUserActive(username, active)
      await load()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : `Failed to ${active ? "enable" : "disable"} user`)
    } finally {
      setToggling(null)
    }
  }

  const forceResetPassword = async (username: string) => {
    if (!window.confirm(`Force "${username}" to change their password on next login?`)) return
    setResetting(username)
    try {
      await api.forceResetPassword(username)
      await load()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Failed to force password reset")
    } finally {
      setResetting(null)
    }
  }

  return (
    <div>
      <PageHeader title="Users" />

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {/* Create form */}
        <form
          onSubmit={(e) => {
            e.preventDefault()
            if (step === 1) void sendCode()
            else if (step === 2) void confirmCode()
            else void createAccount()
          }}
          className="card h-fit space-y-4 p-5"
        >
          <h2 className="flex items-center gap-2 text-sm font-semibold text-slate-100">
            <UserPlus className="h-4 w-4 text-indigo-500" />
            Create user
          </h2>

          {formError && (
            <p className="rounded-lg border border-rose-500/40 bg-rose-500/10 px-3 py-2 text-xs font-medium break-words text-rose-400">
              {formError}
            </p>
          )}
          {createdMsg && (
            <p className="rounded-lg border border-emerald-500/40 bg-emerald-500/10 px-3 py-2 text-xs font-medium text-emerald-600">
              {createdMsg}
            </p>
          )}

          {/* Step 1 - email + password */}
          {step === 1 && (
            <div className="space-y-4">
              <div>
                <label htmlFor="nu-email" className="label">
                  Email
                </label>
                <input
                  id="nu-email"
                  type="email"
                  className="input font-mono"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="alice@example.com"
                  autoComplete="off"
                  required
                />
              </div>
              <div>
                <label htmlFor="nu-password" className="label">
                  Password
                </label>
                <input
                  id="nu-password"
                  type="password"
                  className="input"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Min 8 characters"
                  minLength={8}
                  autoComplete="new-password"
                  required
                />
              </div>
            </div>
          )}

          {/* Step 2 - enter the emailed code */}
          {step === 2 && verifyInfo && (
            <div className="space-y-4">
              <div className="rounded-lg border border-sky-500/30 bg-sky-500/5 p-3">
                <p className="flex items-center gap-1.5 text-[11px] font-semibold text-sky-400">
                  <Mail className="h-3.5 w-3.5" />
                  Code sent to {verifyInfo.email}
                </p>
              </div>

              <div>
                <label htmlFor="nu-code" className="label">
                  Verification code
                </label>
                <input
                  id="nu-code"
                  className="input text-center font-mono text-lg tracking-[0.4em]"
                  value={code}
                  onChange={(e) => setCode(e.target.value.replace(/\D/g, "").slice(0, 6))}
                  placeholder="••••••"
                  inputMode="numeric"
                  autoComplete="one-time-code"
                  maxLength={6}
                  pattern="[0-9]{6}"
                  required
                />
              </div>

              <div className="flex items-center justify-between text-xs">
                <button
                  type="button"
                  onClick={() => void sendCode()}
                  disabled={busy}
                  className="inline-flex cursor-pointer items-center gap-1 text-sky-500 transition hover:text-sky-400 disabled:opacity-50"
                >
                  <RotateCw className="h-3 w-3" />
                  Resend code
                </button>
                <button
                  type="button"
                  onClick={resetFlow}
                  disabled={busy}
                  className="cursor-pointer text-slate-500 transition hover:text-slate-300 disabled:opacity-50"
                >
                  Change email
                </button>
              </div>
            </div>
          )}

          {/* Step 3 - username, full name, role */}
          {step === 3 && (
            <div className="space-y-4">
              <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/5 p-3">
                <p className="flex items-center gap-1.5 text-[11px] font-semibold text-emerald-400">
                  <CheckCircle2 className="h-3.5 w-3.5" />
                  {verifyInfo?.email ?? email} verified
                </p>
              </div>

              <div>
                <label htmlFor="nu-username" className="label">
                  Username *
                </label>
                <input
                  id="nu-username"
                  className="input font-mono"
                  value={newUsername}
                  onChange={(e) => setNewUsername(e.target.value.toLowerCase())}
                  placeholder="alice"
                  pattern="[a-z0-9][a-z0-9_.\-]{2,31}"
                  title="3-32 chars: lowercase letters, numbers, dot, dash or underscore"
                  required
                />
              </div>

              <div>
                <label htmlFor="nu-fullname" className="label">
                  Full name
                </label>
                <input
                  id="nu-fullname"
                  className="input"
                  value={newFullName}
                  onChange={(e) => setNewFullName(e.target.value)}
                  placeholder="Alice Abebe"
                />
              </div>

              <div>
                <span className="label">Role</span>
                <div className="grid grid-cols-2 gap-2">
                  {(["user", "admin"] as const).map((r) => (
                    <button
                      key={r}
                      type="button"
                      onClick={() => setNewRole(r)}
                      className={cn(
                        "cursor-pointer rounded-lg border px-3 py-2 text-xs font-semibold capitalize transition",
                        newRole === r
                          ? "border-indigo-600 bg-indigo-600 text-white"
                          : "border-slate-700 bg-black text-slate-300 hover:border-indigo-500",
                      )}
                    >
                      {r}
                    </button>
                  ))}
                </div>
              </div>

              <label className="flex cursor-pointer items-start gap-3 rounded-lg border border-slate-700 bg-slate-950 p-3">
                <input
                  type="checkbox"
                  checked={forceReset}
                  onChange={(e) => setForceReset(e.target.checked)}
                  className="mt-0.5 h-4 w-4 cursor-pointer accent-indigo-600"
                />
                <span className="text-xs leading-relaxed text-slate-300">
                  <span className="font-semibold text-slate-100">
                    Require password change on first login
                  </span>
                  <br />
                  The user can only set a temporary password now; they must choose their own on
                  first sign-in.
                </span>
              </label>
            </div>
          )}

          <button type="submit" className="btn-primary w-full" disabled={busy}>
            {busy ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : step === 1 ? (
              <Mail className="h-4 w-4" />
            ) : step === 2 ? (
              <KeyRound className="h-4 w-4" />
            ) : (
              <UserPlus className="h-4 w-4" />
            )}
            {busy
              ? "Please wait..."
              : step === 1
                ? "Send verification code"
                : step === 2
                  ? "Confirm email"
                  : "Create account"}
          </button>
        </form>

        {/* Users table */}
        <div className="card overflow-hidden lg:col-span-2">
          {error && (
            <div className="p-5">
              <ErrorBanner message={error} onRetry={() => void load()} />
            </div>
          )}
          {loading && !users ? (
            <LoadingBlock label="Loading users..." />
          ) : !users || users.length === 0 ? (
            <EmptyState icon={<Users className="h-8 w-8" />} title="No users found" />
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[680px]">
                <thead className="border-b border-slate-800 bg-slate-950">
                  <tr>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      User
                    </th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      User ID
                    </th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Role
                    </th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Status
                    </th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Enable / Disable
                    </th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Created
                    </th>
                    <th className="w-14 px-5 py-3" />
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/70">
                  {users.map((u) => (
                    <tr key={u.user_id} className="transition hover:bg-slate-800/40">
                      <td className="px-5 py-3.5">
                        <p className="text-sm font-medium text-slate-100">{u.full_name}</p>
                        <p className="font-mono text-xs text-slate-500">@{u.username}</p>
                        <p className="mt-0.5 font-mono text-[10px] text-slate-600">{u.email}</p>
                      </td>
                      <td className="px-5 py-3.5 font-mono text-xs text-slate-400">{u.user_id}</td>
                      <td className="px-5 py-3.5">
                        <span
                          className={cn(
                            "inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium",
                            u.role === "admin"
                              ? "border border-violet-500/40 bg-violet-500/10 text-violet-500"
                              : "border border-sky-500/40 bg-sky-500/10 text-sky-600",
                          )}
                        >
                          {u.role === "admin" && <ShieldCheck className="h-3 w-3" />}
                          {u.role}
                        </span>
                      </td>
                      <td className="px-5 py-3.5">
                        <div className="flex items-center gap-1.5">
                          <span
                            className={cn(
                              "inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium",
                              u.is_email_verified
                                ? "border border-emerald-500/40 bg-emerald-500/10 text-emerald-500"
                                : "border border-amber-500/40 bg-amber-500/10 text-amber-500",
                            )}
                          >
                            <span
                              className={cn(
                                "h-1.5 w-1.5 rounded-full",
                                u.is_email_verified ? "bg-emerald-500" : "bg-amber-500",
                              )}
                            />
                            {u.is_email_verified ? "verified" : "pending"}
                          </span>
                          {u.must_change_password && (
                            <span
                              title="Must change password on next login"
                              className="inline-flex items-center gap-1 rounded-full border border-rose-500/40 bg-rose-500/10 px-2.5 py-0.5 text-xs font-medium text-rose-500"
                            >
                              <KeyRound className="h-3 w-3" />
                              reset
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-5 py-3.5">
                        {u.username === "admin" ? (
                          <span className="text-xs text-slate-600">—</span>
                        ) : (
                          <button
                            type="button"
                            disabled={toggling === u.username}
                            onClick={() => void toggleActive(u.username, !u.is_active)}
                            title={u.is_active ? `Disable ${u.username}` : `Enable ${u.username}`}
                            aria-label={`${u.is_active ? "Disable" : "Enable"} ${u.username}`}
                            className={cn(
                              "inline-flex cursor-pointer items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium transition disabled:opacity-50",
                              u.is_active
                                ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-500 hover:bg-emerald-500/20"
                                : "border-rose-500/40 bg-rose-500/10 text-rose-500 hover:bg-rose-500/20",
                            )}
                          >
                            {toggling === u.username ? (
                              <Loader2 className="h-3 w-3 animate-spin" />
                            ) : u.is_active ? (
                              <Power className="h-3 w-3" />
                            ) : (
                              <Ban className="h-3 w-3" />
                            )}
                            {u.is_active ? "Enabled" : "Disabled"}
                          </button>
                        )}
                      </td>
                      <td className="px-5 py-3.5 text-xs whitespace-nowrap text-slate-500">
                        {formatDateTime(u.created_at)}
                      </td>
                      <td className="px-5 py-3.5">
                        <div className="flex items-center justify-end gap-1">
                          {u.username !== "admin" && u.username !== session?.user.username && (
                            <>
                              <button
                                type="button"
                                disabled={resetting === u.username}
                                onClick={() => void forceResetPassword(u.username)}
                                title={`Force ${u.username} to change password`}
                                aria-label={`Force ${u.username} to change password`}
                                className="cursor-pointer rounded-md p-1.5 text-slate-500 transition hover:bg-amber-500/10 hover:text-amber-500 disabled:opacity-50"
                              >
                                {resetting === u.username ? (
                                  <Loader2 className="h-4 w-4 animate-spin" />
                                ) : (
                                  <RefreshCcw className="h-4 w-4" />
                                )}
                              </button>
                              <button
                                type="button"
                                onClick={() => void remove(u.username)}
                                title={`Delete ${u.username}`}
                                aria-label={`Delete ${u.username}`}
                                className="cursor-pointer rounded-md p-1.5 text-slate-500 transition hover:bg-rose-500/10 hover:text-rose-500"
                              >
                                <Trash2 className="h-4 w-4" />
                              </button>
                            </>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}