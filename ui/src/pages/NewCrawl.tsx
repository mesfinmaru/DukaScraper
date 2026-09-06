import { useEffect, useState } from "react"
import type { FormEvent } from "react"
import { Link } from "react-router-dom"
import {
  CheckCircle2,
  ChevronDown,
  Loader2,
  Plus,
  Rocket,
  Trash2,
  Waypoints,
  XCircle,
} from "lucide-react"
import { api, ApiError } from "../api"
import { useAuth } from "../config"
import type { RecursiveConfig, SiteCredentialStatus, TriggerJobResponse, WorkerType } from "../types"
import { cn } from "../utils"
import { CopyButton, PageHeader, WorkerBadge } from "../components/ui"
import { KeyRound, ShieldCheck } from "lucide-react"

function domainOf(url: string): string | null {
  try {
    return new URL(url.trim()).hostname.toLowerCase()
  } catch {
    return null
  }
}

interface SiteCredState {
  status: SiteCredentialStatus | null
  checking: boolean
  saving: boolean
  saved: boolean
  error: string | null
  email: string
  password: string
  showForm: boolean
}

function formatTimestamp(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  const pad = (n: number) => String(n).padStart(2, "0")
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
}

function isValidHttpUrl(value: string): boolean {
  try {
    const u = new URL(value)
    return u.protocol === "http:" || u.protocol === "https:"
  } catch {
    return false
  }
}

interface UrlEntry {
  key: number
  value: string
  type: "" | WorkerType
}

interface SubmitOutcome {
  url: string
  result?: TriggerJobResponse
  error?: string
}

const inputCls = "input"

const WORKER_TYPES: { value: "" | WorkerType; label: string }[] = [
  { value: "", label: "Auto" },
  { value: "surface", label: "Surface" },
  { value: "deep", label: "Deep" },
  { value: "dark", label: "Dark" },
]

export default function NewCrawl() {
  const { session } = useAuth()

  const [urls, setUrls] = useState<UrlEntry[]>([
    { key: Date.now(), value: "", type: "" },
  ])
  const [language, setLanguage] = useState("am")
  const [maxDepth, setMaxDepth] = useState(5)
  const [allowLogin, setAllowLogin] = useState(false)
  const [allowSignup, setAllowSignup] = useState(false)
  const [allowEmailVerification, setAllowEmailVerification] = useState(false)
  const [credentialEmail, setCredentialEmail] = useState("")
  const [credentials, setCredentials] = useState<{ email: string; display_name?: string | null }[]>([])
  const [enableExtraction, setEnableExtraction] = useState(true)
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [restrictToDomain, setRestrictToDomain] = useState(true)

  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [outcomes, setOutcomes] = useState<SubmitOutcome[] | null>(null)

  const [history, setHistory] = useState<{ url: string; lastCrawled: string | null }[]>([])
  const [showHistory, setShowHistory] = useState(false)
  const [historyLoading, setHistoryLoading] = useState(true)
  const [selectedHistoryUrls, setSelectedHistoryUrls] = useState<Set<string>>(new Set())

  // Per-domain site credential state, keyed by domain of the first URL
  // (logins are resolved per-domain by the worker; the common case is one target).
  const [siteCred, setSiteCred] = useState<SiteCredState>({
    status: null,
    checking: false,
    saving: false,
    saved: false,
    error: null,
    email: "",
    password: "",
    showForm: false,
  })
  const primaryDomain = urls.map((u) => domainOf(u.value)).find(Boolean) ?? null

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      if (!session) return
      try {
        const isAdmin = session.user.role === "admin"
        const res = isAdmin
          ? await api.getAllJobs()
          : await api.getUserJobs(session.user.user_id)
        if (cancelled) return
        const seen = new Map<string, string | null>()
        for (const j of res.jobs) {
          const url = j.url.trim().replace(/\/+$/, "")
          if (url && !seen.has(url)) seen.set(url, j.created_at)
        }
        setHistory(
          [...seen.entries()].map(([url, lastCrawled]) => ({ url, lastCrawled })),
        )
        try {
          const available = await api.listCredentials()
          if (!cancelled) setCredentials(available.filter((item) => item.status === "active"))
        } catch {
          if (!cancelled) setCredentials([])
        }
      } catch {
        if (!cancelled) setHistory([])
      } finally {
        if (!cancelled) setHistoryLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [session])

  const toggleHistoryUrl = (url: string) => {
    setSelectedHistoryUrls((prev) => {
      const next = new Set(prev)
      if (next.has(url)) next.delete(url)
      else next.add(url)
      return next
    })
  }

  // Look up any stored site credential whenever the primary domain changes.
  useEffect(() => {
    setSiteCred((prev) => ({ ...prev, status: null, saved: false, error: null, showForm: false }))
    if (!primaryDomain) return
    let cancelled = false
    ;(async () => {
      setSiteCred((prev) => ({ ...prev, checking: true }))
      try {
        const status = await api.getSiteCredential(primaryDomain)
        if (!cancelled) setSiteCred((prev) => ({ ...prev, status, checking: false }))
      } catch {
        if (!cancelled) setSiteCred((prev) => ({ ...prev, status: null, checking: false }))
      }
    })()
    return () => {
      cancelled = true
    }
  }, [primaryDomain])

  const saveSiteCredential = async () => {
    if (!primaryDomain || !siteCred.email.trim() || !siteCred.password) return
    setSiteCred((prev) => ({ ...prev, saving: true, error: null }))
    try {
      await api.saveSiteCredential(primaryDomain, {
        email: siteCred.email.trim(),
        password: siteCred.password,
      })
      setSiteCred((prev) => ({
        ...prev,
        saving: false,
        saved: true,
        showForm: false,
        status: { exists: true, email: prev.email.trim(), username: prev.email.trim(), source: "stored" },
      }))
    } catch (reason) {
      setSiteCred((prev) => ({
        ...prev,
        saving: false,
        error: reason instanceof ApiError ? reason.message : "Failed to save credential",
      }))
    }
  }

  const toggleAllHistoryUrls = () => {
    if (selectedHistoryUrls.size === history.length) {
      setSelectedHistoryUrls(new Set())
    } else {
      setSelectedHistoryUrls(new Set(history.map((h) => h.url)))
    }
  }

  const applySelectedHistory = () => {
    const selectedUrls = Array.from(selectedHistoryUrls)
    setUrls((currentUrls) => {
      const existing = new Set(
        currentUrls.map((u) => u.value.trim().replace(/\/+$/, "")).filter(Boolean),
      )
      const fresh = selectedUrls.filter((url) => {
        const normalized = url.replace(/\/+$/, "")
        if (existing.has(normalized)) return false
        existing.add(normalized)
        return true
      })
      if (!fresh.length) return currentUrls

      let result = [...currentUrls]
      for (const url of fresh) {
        const emptyIndex = result.findIndex((u) => !u.value.trim())
        if (emptyIndex !== -1) {
          result[emptyIndex] = { ...result[emptyIndex], value: url }
        } else {
          result.push({ key: Date.now() + result.length, value: url, type: "" })
        }
      }
      return result
    })
    setSelectedHistoryUrls(new Set())
  }

  const addUrlRow = () => {
    setUrls((prev) => [...prev, { key: Date.now() + prev.length, value: "", type: "" }])
  }

  const removeUrlRow = (key: number) => {
    setUrls((prev) => (prev.length > 1 ? prev.filter((u) => u.key !== key) : prev))
  }

  const updateUrlRow = (key: number, patch: Partial<UrlEntry>) => {
    setUrls((prev) => prev.map((u) => (u.key === key ? { ...u, ...patch } : u)))
  }

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setError(null)
    setOutcomes(null)

    const entries = urls.map((u) => ({ ...u, trimmed: u.value.trim() })).filter((u) => u.trimmed)
    if (!entries.length) {
      setError("Add at least one target URL starting with http:// or https://")
      return
    }
    const invalid = entries.find((u) => !isValidHttpUrl(u.trimmed))
    if (invalid) {
      setError(`Invalid URL: ${invalid.trimmed} - every entry must start with http:// or https://`)
      return
    }

    const recursiveConfig: RecursiveConfig = {}
    if (maxDepth > 0) {
      recursiveConfig.enable_extraction = enableExtraction

      if (restrictToDomain && enableExtraction) {
        const domainPatterns = entries
          .map((entry) => {
            try {
              const url = new URL(entry.trimmed)
              return url.hostname
            } catch {
              return null
            }
          })
          .filter((d): d is string => d !== null)
        if (domainPatterns.length) {
          recursiveConfig.link_filter_patterns = [
            ...(recursiveConfig.link_filter_patterns ?? []),
            ...domainPatterns,
          ]
        }
      }
    }

    setSubmitting(true)
    try {
      const batch = await api.triggerBatch({
        urls: entries.map((entry) => entry.trimmed),
        user_id: session?.user.user_id ?? "",
        language,
        worker_override: entries.every((entry) => entry.type === entries[0].type)
          ? entries[0].type || null
          : null,
        max_depth: maxDepth,
        recursive_config: recursiveConfig,
        allow_login: allowLogin,
        allow_signup: allowSignup,
        allow_email_verification: allowEmailVerification,
        credential_email: credentialEmail || null,
      })
      const results: SubmitOutcome[] = batch.jobs.map((job, index) => ({
        url: entries[index]?.trimmed ?? "unknown",
        result: job,
      }))
      setOutcomes(results)
    } catch (reason) {
      setError(
        reason instanceof ApiError ? reason.message : "Failed to submit crawl batch",
      )
    } finally {
      setSubmitting(false)
    }
  }

  const okCount = outcomes?.filter((o) => o.result).length ?? 0
  const failCount = outcomes?.filter((o) => o.error).length ?? 0

  return (
    <div>
      <PageHeader title="New Crawl" />

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
        <form onSubmit={(e) => void submit(e)} className="card space-y-5 p-6 xl:col-span-2">
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="label mb-0">Target URLs *</span>
              <button
                type="button"
                onClick={addUrlRow}
                className="inline-flex cursor-pointer items-center gap-1.5 rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs font-medium text-slate-300 transition hover:border-sky-500/50 hover:text-sky-300"
              >
                <Plus className="h-3.5 w-3.5" />
                Add URL
              </button>
            </div>

            {urls.map((entry) => (
              <div key={entry.key} className="flex flex-col gap-2 sm:flex-row sm:items-center">
                <input
                  className={cn(inputCls, "min-w-0 flex-1 font-mono")}
                  value={entry.value}
                  onChange={(e) => updateUrlRow(entry.key, { value: e.target.value })}
                  placeholder="https://www.bbc.com/amharic"
                  spellCheck={false}
                  required={urls.length === 1}
                />
                <select
                  aria-label={`Worker type for ${entry.value || "new row"}`}
                  className={cn(inputCls, "sm:w-36")}
                  value={entry.type}
                  onChange={(e) =>
                    updateUrlRow(entry.key, { type: e.target.value as "" | WorkerType })
                  }
                >
                  {WORKER_TYPES.map((t) => (
                    <option key={t.label} value={t.value}>
                      {t.label}
                    </option>
                  ))}
                </select>
                <button
                  type="button"
                  onClick={() => removeUrlRow(entry.key)}
                  disabled={urls.length === 1}
                  title={urls.length === 1 ? "At least one URL is required" : "Remove URL"}
                  className="inline-flex shrink-0 cursor-pointer items-center justify-center rounded-lg border border-slate-800 bg-slate-950/60 p-2.5 text-slate-500 transition enabled:hover:border-rose-500/40 enabled:hover:text-rose-300 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <label className="label" htmlFor="crawl-lang">
                Language
              </label>
              <select
                id="crawl-lang"
                className={inputCls}
                value={language}
                onChange={(e) => setLanguage(e.target.value)}
              >
                <option value="am">Amharic (am)</option>
                <option value="en">English (en)</option>
              </select>
            </div>
            <div>
              <label className="label" htmlFor="crawl-depth">
                Max depth: <span className="text-indigo-600 normal-case">{maxDepth}</span>
              </label>
              <input
                id="crawl-depth"
                type="range"
                min={0}
                max={10}
                step={1}
                value={maxDepth}
                onChange={(e) => setMaxDepth(Number(e.target.value))}
                className="mt-2.5 w-full accent-sky-500"
              />
              <p className="mt-1 text-[11px] text-slate-500">
                {maxDepth === 0 ? "Single page only - no recursion" : `Recursion up to depth ${maxDepth}`}
              </p>
            </div>
          </div>

          <div className="grid grid-cols-1 gap-3 rounded-lg border border-slate-800 bg-slate-950/40 p-4 sm:grid-cols-3">
            <label className="flex cursor-pointer items-center gap-3 text-sm text-slate-300">
              <input type="checkbox" checked={allowLogin} onChange={(e) => setAllowLogin(e.target.checked)} className="h-4 w-4 accent-sky-500" />
              Allow login
            </label>
            <label className="flex cursor-pointer items-center gap-3 text-sm text-slate-300">
              <input type="checkbox" checked={allowSignup} onChange={(e) => setAllowSignup(e.target.checked)} className="h-4 w-4 accent-sky-500" />
              Allow signup
            </label>
            <label className={cn("flex items-center gap-3 text-sm", allowSignup ? "cursor-pointer text-slate-300" : "text-slate-600")}>
              <input type="checkbox" checked={allowEmailVerification} onChange={(e) => setAllowEmailVerification(e.target.checked)} disabled={!allowSignup} className="h-4 w-4 accent-sky-500" />
              Email verification
            </label>
            {allowSignup && allowEmailVerification && (
              <p className="text-[11px] text-slate-500 sm:col-span-3">
                Signups use a <code className="rounded bg-slate-900 px-1 py-0.5 font-mono text-slate-400">seed+dukaXXXXX@gmail.com</code>{" "}
                alias of your configured seed inbox — verification emails are read automatically (Gmail API or IMAP) and confirmed without manual steps.
              </p>
            )}
          </div>

          {(allowLogin || allowSignup) && primaryDomain && (
            <div className="rounded-lg border border-slate-800 bg-slate-950/40 p-4">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-2 text-sm">
                  <KeyRound className="h-4 w-4 text-sky-400" />
                  <span className="font-medium text-slate-200">Site credential for</span>
                  <code className="rounded bg-slate-900 px-1.5 py-0.5 font-mono text-xs text-sky-300">{primaryDomain}</code>
                </div>
                {siteCred.checking ? (
                  <span className="flex items-center gap-2 text-xs text-slate-500">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" /> Checking stored credentials…
                  </span>
                ) : siteCred.status?.exists ? (
                  <span className="flex items-center gap-1.5 text-xs font-medium text-emerald-400">
                    <ShieldCheck className="h-4 w-4" />
                    Stored ({siteCred.status.email}) — reused automatically
                  </span>
                ) : (
                  <button
                    type="button"
                    onClick={() => setSiteCred((prev) => ({ ...prev, showForm: !prev.showForm }))}
                    className="cursor-pointer text-xs font-medium text-sky-400 transition hover:text-sky-300"
                  >
                    {siteCred.showForm ? "Cancel" : "+ Save credential for this site"}
                  </button>
                )}
              </div>

              {siteCred.error && (
                <p className="mt-2 rounded border border-rose-500/40 bg-rose-500/10 px-2.5 py-1.5 text-xs text-rose-400">{siteCred.error}</p>
              )}

              {siteCred.saved && (
                <p className="mt-2 text-xs font-medium text-emerald-400">
                  Saved — future crawls of {primaryDomain} will log in automatically.
                </p>
              )}

              {siteCred.showForm && (
                <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2">
                  <div>
                    <label className="label" htmlFor="sitecred-email">Email / username</label>
                    <input
                      id="sitecred-email"
                      className={inputCls}
                      value={siteCred.email}
                      onChange={(e) => setSiteCred((prev) => ({ ...prev, email: e.target.value }))}
                      placeholder="admin@example.com"
                      autoComplete="off"
                    />
                  </div>
                  <div>
                    <label className="label" htmlFor="sitecred-pass">Password</label>
                    <input
                      id="sitecred-pass"
                      type="password"
                      className={inputCls}
                      value={siteCred.password}
                      onChange={(e) => setSiteCred((prev) => ({ ...prev, password: e.target.value }))}
                      placeholder="Site password"
                      autoComplete="new-password"
                    />
                  </div>
                  <div className="sm:col-span-2">
                    <button
                      type="button"
                      onClick={() => void saveSiteCredential()}
                      disabled={siteCred.saving || !siteCred.email.trim() || !siteCred.password}
                      className="btn-primary w-full py-2 disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {siteCred.saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <KeyRound className="h-4 w-4" />}
                      {siteCred.saving ? "Saving…" : `Save credential for ${primaryDomain}`}
                    </button>
                    <p className="mt-1.5 text-[11px] text-slate-500">
                      Stored encrypted. The worker fills these automatically on the next crawl — no manual login needed.
                    </p>
                  </div>
                </div>
              )}
            </div>
          )}

          {(allowLogin || allowSignup) && (
            <div>
              <label className="label" htmlFor="credential-profile">Credential profile</label>
              <select
                id="credential-profile"
                className={inputCls}
                value={credentialEmail}
                onChange={(e) => setCredentialEmail(e.target.value)}
              >
                <option value="">Use stored domain credential when available</option>
                {credentials.map((credential) => (
                  <option key={credential.email} value={credential.email}>
                    {credential.display_name ? `${credential.display_name} - ` : ""}{credential.email}
                  </option>
                ))}
              </select>
            </div>
          )}

          <div>
            <button
              type="button"
              onClick={() => setShowAdvanced((v) => !v)}
              className="inline-flex cursor-pointer items-center gap-1.5 text-sm font-medium text-slate-400 transition hover:text-slate-200"
            >
              <ChevronDown className={cn("h-4 w-4 transition-transform", showAdvanced && "rotate-180")} />
              Advanced options
            </button>

            {showAdvanced && (
              <div className="mt-4 space-y-4 rounded-lg border border-slate-800 bg-slate-950/40 p-4">
                <label className="flex cursor-pointer items-center gap-3">
                  <input
                    type="checkbox"
                    checked={enableExtraction}
                    onChange={(e) => setEnableExtraction(e.target.checked)}
                    disabled={maxDepth === 0}
                    className="h-4 w-4 accent-sky-500"
                  />
                  <span className="text-sm text-slate-300">
                    Extract child links for recursive crawling
                    <span className="block text-xs text-slate-500">
                      Follow links found on the page and crawl them too, up to the max depth
                    </span>
                  </span>
                </label>

                <label className="flex cursor-pointer items-center gap-3">
                  <input
                    type="checkbox"
                    checked={restrictToDomain}
                    onChange={(e) => setRestrictToDomain(e.target.checked)}
                    disabled={maxDepth === 0 || !enableExtraction}
                    className="h-4 w-4 accent-sky-500"
                  />
                  <span className="text-sm text-slate-300">
                    Restrict to same domain only
                    <span className="block text-xs text-slate-500">
                      Only follow links within the same domain as the seed URL
                    </span>
                  </span>
                </label>

              </div>
            )}
          </div>

          {error && (
            <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm break-words text-rose-200">
              {error}
            </div>
          )}

          <div className="flex items-center gap-3 border-t border-slate-800/80 pt-5">
            <button type="submit" className="btn-primary min-w-44" disabled={submitting}>
              {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Rocket className="h-4 w-4" />}
              {submitting
                ? "Submitting..."
                : urls.filter((u) => u.value.trim()).length > 1
                  ? `Start ${urls.filter((u) => u.value.trim()).length} crawls`
                  : "Start crawl"}
            </button>
          </div>
        </form>

        <div className="space-y-6">
          {!historyLoading && history.length > 0 && (
            <div className="card p-5">
              <div className="flex items-center justify-between">
                <span className="label mb-0">Previously scraped URLs</span>
                <button
                  type="button"
                  onClick={() => setShowHistory((v) => !v)}
                  className="inline-flex cursor-pointer items-center gap-1.5 text-xs font-medium text-slate-400 transition hover:text-slate-200"
                >
                  <ChevronDown className={cn("h-4 w-4 transition-transform", showHistory && "rotate-180")} />
                  {showHistory ? "Hide" : `Show (${history.length})`}
                </button>
              </div>
              {showHistory && (
                <div className="mt-3 space-y-2">
                  <div className="flex items-center gap-2 rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-2">
                    <label className="flex cursor-pointer items-center gap-2">
                      <input
                        type="checkbox"
                        checked={selectedHistoryUrls.size === history.length && history.length > 0}
                        onChange={toggleAllHistoryUrls}
                        className="h-4 w-4 accent-sky-500"
                      />
                      <span className="text-xs font-medium text-slate-300">Select all</span>
                    </label>
                    {selectedHistoryUrls.size > 0 && (
                      <button
                        type="button"
                        onClick={applySelectedHistory}
                        className="ml-auto btn-secondary text-xs min-w-0"
                        disabled={selectedHistoryUrls.size === 0}
                      >
                        Apply selected ({selectedHistoryUrls.size})
                      </button>
                    )}
                  </div>
                  <ul className="max-h-96 space-y-1.5 overflow-y-auto">
                    {history.map((h) => (
                      <li key={h.url}>
                        <label
                          className="flex w-full cursor-pointer items-start gap-2 rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-2 text-left transition hover:border-sky-500/50"
                        >
                          <input
                            type="checkbox"
                            checked={selectedHistoryUrls.has(h.url)}
                            onChange={() => toggleHistoryUrl(h.url)}
                            className="mt-0.5 h-4 w-4 accent-sky-500 shrink-0"
                          />
                          <span className="min-w-0 flex-1">
                            <span className="block break-all font-mono text-xs text-slate-300">
                              {h.url}
                            </span>
                            {h.lastCrawled && (
                              <span className="mt-0.5 block whitespace-nowrap overflow-hidden text-[10px] text-slate-500">
                                Last crawled: {formatTimestamp(h.lastCrawled)}
                              </span>
                            )}
                          </span>
                        </label>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

          {outcomes && (
            <div className="card p-5">
              <div className="mb-4 flex items-center gap-2.5">
                {failCount === 0 ? (
                  <CheckCircle2 className="h-5 w-5 text-emerald-400" />
                ) : okCount === 0 ? (
                  <XCircle className="h-5 w-5 text-rose-400" />
                ) : (
                  <CheckCircle2 className="h-5 w-5 text-amber-400" />
                )}
                <p className="text-sm font-semibold text-emerald-200">
                  {okCount} job{okCount === 1 ? "" : "s"} submitted
                  {failCount > 0 ? `, ${failCount} failed` : " - workers are scraping now"}
                </p>
              </div>

              <ul className="space-y-3">
                {outcomes.map((o) => (
                  <li key={o.url} className="rounded-lg border border-slate-800 bg-slate-950/40 p-3">
                    {o.result ? (
                      <>
                        <div className="flex items-center justify-between gap-2">
                          <WorkerBadge worker={o.result.assigned_worker} />
                          <Link
                            to={`/jobs/${encodeURIComponent(o.result.job_id)}`}
                            className="inline-flex items-center gap-1 text-[11px] font-medium text-sky-600 hover:text-sky-400"
                          >
                            Track
                            <Waypoints className="h-3 w-3" />
                          </Link>
                        </div>
                        <p className="mt-2 truncate font-mono text-[11px] text-slate-400" title={o.url}>
                          {o.url}
                        </p>
                        <div className="mt-2 flex items-center justify-between gap-2">
                          <span className="font-mono text-xs text-sky-600">{o.result.job_id}</span>
                          <CopyButton value={o.result.job_id} label="" />
                        </div>
                      </>
                    ) : (
                      <>
                        <p className="truncate font-mono text-[11px] text-slate-400" title={o.url}>
                          {o.url}
                        </p>
                        <p className="mt-1 break-words text-xs text-rose-300">{o.error}</p>
                      </>
                    )}
                  </li>
                ))}
              </ul>

              {okCount > 0 && (
                <Link to="/jobs" className="btn-secondary mt-4 w-full justify-center">
                  <Waypoints className="h-4 w-4" />
                  View all jobs
                </Link>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
