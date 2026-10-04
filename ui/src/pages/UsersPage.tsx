import { useCallback, useEffect, useState } from "react"
import {
  Ban,
  ChevronDown,
  Loader2,
  Power,
  RefreshCcw,
  ShieldCheck,
  Trash2,
  UserPlus,
  Users,
} from "lucide-react"
import { api, ApiError } from "../api"
import { useAuth } from "../config"
import type { AdminUserRow } from "../types"
import { cn, formatDateOnly } from "../utils"
import {
  EmptyState,
  ErrorBanner,
  LoadingBlock,
  Msg,
  PageHeader,
  useConfirm,
} from "../components/ui"
import { useAutoRefresh } from "../useAutoRefresh"

const FORM_OPEN_KEY = "dukascraper.console.users.formOpen.v1"

/** Expanded by default: creating a user is the main job on this page, and the
 *  25/75 split is what an admin sees on arrival. */
function getInitialFormOpen(): boolean {
  try {
    return localStorage.getItem(FORM_OPEN_KEY) !== "collapsed"
  } catch {
    return true
  }
}

export default function UsersPage() {
  const { session } = useAuth()
  const [users, setUsers] = useState<AdminUserRow[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [newFullName, setNewFullName] = useState("")
  const [newUsername, setNewUsername] = useState("")
  const [newEmail, setNewEmail] = useState("")
  const [newPassword, setNewPassword] = useState("")
  const [newRole, setNewRole] = useState<"user" | "admin">("user")

  const [formOpen, setFormOpen] = useState(getInitialFormOpen)
  const [busy, setBusy] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const [createdMsg, setCreatedMsg] = useState<string | null>(null)

  const { confirm, confirmDialog } = useConfirm()
  const [toggling, setToggling] = useState<string | null>(null)
  const [resetting, setResetting] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const res = await api.listUsers()
      setUsers(res.users)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load the user list.")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  // Account state changes when an admin edits it, possibly in another tab.
  useAutoRefresh({ load })

  useEffect(() => {
    try {
      localStorage.setItem(FORM_OPEN_KEY, formOpen ? "expanded" : "collapsed")
    } catch {
      /* ignore */
    }
  }, [formOpen])

  const resetForm = () => {
    setNewFullName("")
    setNewUsername("")
    setNewEmail("")
    setNewPassword("")
    setNewRole("user")
    setFormError(null)
  }

  const createAccount = async (e: React.FormEvent) => {
    e.preventDefault()
    setFormError(null)
    const fullName = newFullName.trim()
    const username = newUsername.trim().toLowerCase()
    const emailAddr = newEmail.trim().toLowerCase()
    if (fullName.length < 2) {
      setFormError("Enter the user's full name.")
      return
    }
    if (!/^[a-z0-9][a-z0-9_.\-]{2,31}$/.test(username)) {
      setFormError("Username: 3 to 32 letters, numbers, dot, dash or underscore.")
      return
    }
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(emailAddr)) {
      setFormError("Enter a valid email address.")
      return
    }
    if (newPassword.length < 8) {
      setFormError("Password must be at least 8 characters.")
      return
    }
    setBusy(true)
    try {
      await api.createUser({
        full_name: fullName,
        username,
        email: emailAddr,
        password: newPassword,
        role: newRole,
      })
      setCreatedMsg(`User created. We emailed the login details to ${emailAddr}.`)
      resetForm()
      await load()
    } catch (err) {
      setFormError(err instanceof ApiError ? err.message : "Could not create the user.")
    } finally {
      setBusy(false)
    }
  }

  const remove = async (username: string) => {
    const ok = await confirm({
      title: "Delete this user?",
      message: `${username} and all of their jobs will be deleted. This cannot be undone.`,
      confirmLabel: "Delete",
      tone: "danger",
    })
    if (!ok) return
    try {
      await api.deleteUser(username)
      await load()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Could not delete this user.")
    }
  }

  const toggleActive = async (username: string, active: boolean) => {
    const ok = await confirm({
      title: active ? "Allow this user to sign in?" : "Stop this user signing in?",
      message: active
        ? `${username} will be able to sign in again.`
        : `${username} will not be able to sign in. Their past jobs are kept.`,
      confirmLabel: active ? "Allow" : "Stop access",
      tone: active ? "default" : "danger",
    })
    if (!ok) return
    setToggling(username)
    try {
      await api.setUserActive(username, active)
      await load()
    } catch (err) {
      setError(
        err instanceof ApiError
          ? err.message
          : `Could not ${active ? "enable" : "disable"} this user.`,
      )
    } finally {
      setToggling(null)
    }
  }

  const forceResetPassword = async (username: string) => {
    const ok = await confirm({
      title: "Ask for a new password?",
      message: `${username} will have to choose a new password next time they sign in.`,
      confirmLabel: "Ask for new password",
    })
    if (!ok) return
    setResetting(username)
    try {
      await api.forceResetPassword(username)
      await load()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Could not reset this password.")
    } finally {
      setResetting(null)
    }
  }

  return (
    <div>
      <PageHeader title="Users" />

      <div className="grid grid-cols-1 items-start gap-5 xl:grid-cols-4">
        {/* Create form - single step: admin fills everything, job ends on submit.
            Expanded it takes one of four columns (25%), matching the table's 75%.
            Collapsed it spans the full width as a slim bar so the table below
            gets the whole page. */}
        <form
          onSubmit={(e) => void createAccount(e)}
          className={cn(
            "card p-3",
            formOpen ? "h-fit space-y-3 xl:col-span-1" : "xl:col-span-4",
          )}
        >
          <h2>
            <button
              type="button"
              onClick={() => setFormOpen((v) => !v)}
              aria-expanded={formOpen}
              aria-controls="create-user-fields"
              title={formOpen ? "Hide the create user form" : "Show the create user form"}
              className="flex w-full cursor-pointer items-center gap-2 text-left text-sm font-semibold text-slate-100"
            >
              <UserPlus className="h-4 w-4 text-indigo-500" />
              Create user
              <ChevronDown
                className={cn(
                  "ml-auto h-4 w-4 shrink-0 text-slate-500 transition-transform",
                  formOpen && "rotate-180",
                )}
                aria-hidden
              />
            </button>
          </h2>

          {/* Unmounted rather than hidden, so collapsed leaves no unreachable
              inputs or a hidden submit button in the tab order. */}
          {formOpen && (
            <div id="create-user-fields" className="space-y-2.5">
            {formError && <Msg tone="error">{formError}</Msg>}
            {createdMsg && <Msg tone="success">{createdMsg}</Msg>}

            <div>
              <label htmlFor="nu-fullname" className="label">
                Full name
              </label>
              <input
                id="nu-fullname"
                className="input"
                value={newFullName}
                onChange={(e) => setNewFullName(e.target.value)}
                placeholder="Abebe Kebede"
                autoComplete="off"
                required
              />
            </div>

            <div>
              <label htmlFor="nu-username" className="label">
                Username
              </label>
              <input
                id="nu-username"
                className="input font-mono"
                value={newUsername}
                onChange={(e) => setNewUsername(e.target.value.toLowerCase())}
                placeholder="abebe123"
                pattern="[a-z0-9][a-z0-9_.\-]{2,31}"
                title="3-32 chars: lowercase letters, numbers, dot, dash or underscore"
                required
              />
            </div>

            <div>
              <label htmlFor="nu-email" className="label">
                Email
              </label>
              <input
                id="nu-email"
                type="email"
                className="input font-mono"
                value={newEmail}
                onChange={(e) => setNewEmail(e.target.value)}
                placeholder="abebe@example.com"
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
                type="text"
                className="input font-mono"
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                placeholder="password"
                minLength={8}
                autoComplete="new-password"
                required
              />
              <p className="mt-1.5 text-[11px] leading-relaxed text-slate-500">
                We email this to the user. They confirm it, then sign in.
              </p>
            </div>

            <div>
              <span className="label">Role</span>
              <select
                id="nu-role"
                className="input font-mono"
                value={newRole}
                onChange={(e) => setNewRole(e.target.value as "user" | "admin")}
              >
                <option value="user">user</option>
                <option value="admin">admin</option>
              </select>
            </div>

            <button type="submit" className="btn-primary w-full" disabled={busy}>
              {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <UserPlus className="h-4 w-4" />}
              {busy ? "Creating account..." : "Create user"}
            </button>
            </div>
          )}
        </form>

        {/* Users table */}
        <div className={cn("card overflow-hidden", formOpen ? "xl:col-span-3" : "xl:col-span-4")}>
          {error && (
            <div className="p-4">
              <ErrorBanner message={error} onRetry={() => void load()} />
            </div>
          )}
          {loading && !users ? (
            <LoadingBlock label="Loading users..." />
          ) : !users || users.length === 0 ? (
            <EmptyState icon={<Users className="h-8 w-8" />} title="No users found" />
          ) : (
            <div className="table-scroll">
              <table className="w-full">
                <thead className="border-b border-slate-800 bg-slate-950">
                  <tr>
                    <th className="px-2.5 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      User
                    </th>
                    <th className="px-2.5 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      User ID
                    </th>
                    <th className="px-2.5 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Role
                    </th>
                    <th className="px-2.5 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Status
                    </th>
                    <th className="px-2.5 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Access
                    </th>
                    <th className="px-2.5 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Created
                    </th>
                    <th className="w-10 px-1.5 py-2" />
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/70">
                  {users.map((u) => (
                    <tr key={u.user_id} className="transition hover:bg-slate-800/40">
                      <td data-label="User" className="max-w-[150px] px-2.5 py-2">
                        <p className="truncate text-[13px] leading-tight font-medium text-slate-100" title={u.full_name}>
                          {u.full_name}
                        </p>
                        <p className="truncate font-mono text-[11px] leading-tight text-slate-500">@{u.username}</p>
                        <p className="mt-0.5 truncate font-mono text-[10px] leading-tight text-slate-600" title={u.email}>
                          {u.email}
                        </p>
                      </td>
                      <td data-label="User ID" className="px-2.5 py-2 font-mono text-[11px] whitespace-nowrap text-slate-400">{u.user_id}</td>
                      <td data-label="Role" className="px-2.5 py-2 whitespace-nowrap">
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
                      <td data-label="Status" className="px-2.5 py-2 whitespace-nowrap">
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
                          {u.must_change_password && (                                <span
                              title="Must change password on next login"
                              className="inline-flex items-center gap-1 rounded-full border border-rose-500/40 bg-rose-500/10 px-2.5 py-0.5 text-xs font-medium text-rose-500"
                            >
                              reset
                            </span>
                          )}
                        </div>
                      </td>
                      <td data-label="Access" className="px-2.5 py-2 whitespace-nowrap">
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
                      <td data-label="Created" className="px-2.5 py-2 text-xs whitespace-nowrap text-slate-500">
                        {formatDateOnly(u.created_at)}
                      </td>
                      <td className="px-2.5 py-2 whitespace-nowrap">
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
      {confirmDialog}
    </div>
  )
}