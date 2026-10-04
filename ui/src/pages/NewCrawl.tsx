import { useEffect, useState } from "react"
import type { FormEvent } from "react"
import { Link } from "react-router-dom"
import {
  CheckCircle2,
  ChevronDown,
  Loader2,
  Plus,
  Rocket,
  Search,
  Trash2,
  Waypoints,
  XCircle,
} from "lucide-react"
import { api, ApiError } from "../api"
import { useAuth } from "../config"
import type {
  DiscoverResponse,
  CrawlDatatype,
  DiscoveryNetwork,
  RecursiveConfig,
  SiteCredentialStatus,
  TriggerJobResponse,
  WorkerType,
} from "../types"
import { cn } from "../utils"
import { CopyButton, Msg, PageHeader, WorkerBadge } from "../components/ui"
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
  error: string | null
  email: string
  password: string
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

/**
 * Content types the pipeline can actually fetch and extract. These mirror the
 * backend's extension sets - HTML, PDF, DOCX/ODT, and audio (which is handed to
 * the transcriber) - so the list cannot drift from what the crawler supports.
 */
const DATATYPES: { value: CrawlDatatype; label: string; hint: string }[] = [
  { value: "all", label: "All types", hint: "Anything the crawler can extract text from" },
  { value: "html", label: "Web pages", hint: "Ordinary HTML pages" },
  { value: "pdf", label: "PDF", hint: "PDF documents" },
  { value: "document", label: "Documents", hint: "Word and OpenDocument files" },
  { value: "audio", label: "Audio", hint: "Audio files, transcribed to text" },
]

export default function NewCrawl() {
  const { session } = useAuth()

  const [urls, setUrls] = useState<UrlEntry[]>([
    { key: Date.now(), value: "", type: "" },
  ])
  const [language, setLanguage] = useState("en")
  const [maxDepth, setMaxDepth] = useState(5)
  const [allowLogin, setAllowLogin] = useState(false)
  const [allowSignup, setAllowSignup] = useState(false)
  const [allowEmailVerification, setAllowEmailVerification] = useState(false)
  const [enableExtraction, setEnableExtraction] = useState(true)
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [restrictToDomain, setRestrictToDomain] = useState(true)

  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [outcomes, setOutcomes] = useState<SubmitOutcome[] | null>(null)

  // --- Topic discovery mode (query -> seed URLs via the discovery-worker) ---
  const [mode, setMode] = useState<"urls" | "discover">("urls")
  const [discoverQuery, setDiscoverQuery] = useState("")
  // "all" is the default: a topic search should reach the whole system rather
  // than requiring three separate submissions to cover the three layers.
  const [discoverNetwork, setDiscoverNetwork] = useState<DiscoveryNetwork>("all")
  const [datatype, setDatatype] = useState<CrawlDatatype>("all")
  /** One line explaining what the current choice actually does. */
  const datatypeHint =
    DATATYPES.find((d) => d.value === datatype)?.hint ?? ""
  const [discoverMaxResults, setDiscoverMaxResults] = useState(10)
  const [discovering, setDiscovering] = useState(false)
  const [discoverOutcome, setDiscoverOutcome] = useState<DiscoverResponse | null>(null)

  const [history, setHistory] = useState<{ url: string; lastCrawled: string | null }[]>([])
  const [showHistory, setShowHistory] = useState(false)
  const [historyLoading, setHistoryLoading] = useState(true)
  const [selectedHistoryUrls, setSelectedHistoryUrls] = useState<Set<string>>(new Set())

  // Per-domain site credential state — one entry per unique target domain so
  // each site can be checked/configured separately.
  const [siteCreds, setSiteCreds] = useState<Record<string, SiteCredState>>({})
  // Domain whose credential popup is open (null = closed).
  const [credModal, setCredModal] = useState<string | null>(null)
  const domains = Array.from(
    new Set(urls.map((u) => domainOf(u.value)).filter((d): d is string => Boolean(d))),
  )

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

  // Look up stored site credentials whenever the target domains change.
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      setSiteCreds((prev) => {
        const next: Record<string, SiteCredState> = {}
        for (const domain of domains) {
          next[domain] = prev[domain] ?? {
            status: null,
            checking: false,
            saving: false,
            error: null,
            email: "",
            password: "",
          }
        }
        return next
      })
      await Promise.all(
        domains.map(async (domain) => {
          setSiteCreds((prev) => ({
            ...prev,
            [domain]: { ...prev[domain], status: null, error: null, checking: true },
          }))
          try {
            const status = await api.getSiteCredential(domain)
            if (!cancelled) {
              setSiteCreds((prev) => ({
                ...prev,
                [domain]: { ...prev[domain], status, checking: false },
              }))
            }
          } catch {
            if (!cancelled) {
              setSiteCreds((prev) => ({
                ...prev,
                [domain]: { ...prev[domain], status: null, checking: false },
              }))
            }
          }
        }),
      )
    })()
    return () => {
      cancelled = true
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urls.map((u) => domainOf(u.value)).join("|")])

  const updateSiteCred = (domain: string, patch: Partial<SiteCredState>) => {
    setSiteCreds((prev) => ({
      ...prev,
      [domain]: { ...prev[domain], ...patch },
    }))
  }

  const saveSiteCredential = async (domain: string) => {
    const cred = siteCreds[domain]
    if (!domain || !cred?.email.trim() || !cred.password) return
    updateSiteCred(domain, { saving: true, error: null })
    try {
      await api.saveSiteCredential(domain, {
        email: cred.email.trim(),
        password: cred.password,
      })
      updateSiteCred(domain, {
        saving: false,
        status: { exists: true, email: cred.email.trim(), username: cred.email.trim(), source: "stored" },
      })
      setCredModal(null)
    } catch (reason) {
      updateSiteCred(domain, {
        saving: false,
        error: reason instanceof ApiError ? reason.message : "Failed to save credential",
      })
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
      setError("Add at least one link, starting with http:// or https://")
      return
    }
    const invalid = entries.find((u) => !isValidHttpUrl(u.trimmed))
    if (invalid) {
      setError(`This link does not look right: ${invalid.trimmed}`)
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
        datatype,
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

  const submitDiscover = async (e: FormEvent) => {
    e.preventDefault()
    setError(null)
    setDiscoverOutcome(null)

    const query = discoverQuery.trim()
    if (!query) {
      setError("Enter a topic or search to find links.")
      return
    }

    setDiscovering(true)
    try {
      const res = await api.discoverJob({
        query,
        network: discoverNetwork,
        user_id: session?.user.user_id ?? null,
        language,
        max_results: discoverMaxResults,
        max_depth: maxDepth,
        recursive_config: { enable_extraction: maxDepth > 0 },
        datatype,
      })
      setDiscoverOutcome(res)
    } catch (reason) {
      setError(
        reason instanceof ApiError ? reason.message : "Failed to start topic discovery",
      )
    } finally {
      setDiscovering(false)
    }
  }

  const okCount = outcomes?.filter((o) => o.result).length ?? 0
  const failCount = outcomes?.filter((o) => o.error).length ?? 0

  return (
    <div>
      <PageHeader title="New Crawl" />

      <div className="mb-6 inline-flex rounded-lg border border-slate-800 bg-slate-950/60 p-1">
        <button
          type="button"
          onClick={() => { setMode("urls"); setError(null) }}
          className={cn(
            "inline-flex cursor-pointer items-center gap-1.5 rounded-md px-4 py-1.5 text-sm font-medium transition",
            mode === "urls"
              ? "bg-sky-500/15 text-sky-300"
              : "text-slate-400 hover:text-slate-200",
          )}
        >
          <Rocket className="h-4 w-4" />
          Crawl URLs
        </button>
        <button
          type="button"
          onClick={() => { setMode("discover"); setError(null) }}
          className={cn(
            "inline-flex cursor-pointer items-center gap-1.5 rounded-md px-4 py-1.5 text-sm font-medium transition",
            mode === "discover"
              ? "bg-sky-500/15 text-sky-300"
              : "text-slate-400 hover:text-slate-200",
          )}
        >
          <Search className="h-4 w-4" />
          Discover by topic
        </button>
      </div>

      {mode === "discover" && (
        <form
          onSubmit={(e) => void submitDiscover(e)}
          className="card max-w-3xl space-y-5 p-6"
        >
          <div>
            <label className="label" htmlFor="discover-query">
              Topic / search query *
            </label>
            <input
              id="discover-query"
              className={cn(inputCls, "font-mono")}
              value={discoverQuery}
              onChange={(e) => setDiscoverQuery(e.target.value)}
              placeholder="ethiopian telecom news"
              spellCheck={false}
              autoFocus
              required
            />
            <p className="mt-1 text-[11px] text-slate-500">
              The discovery worker searches the selected network and fans the results out as
              crawl seeds — no starting URL needed.
            </p>
          </div>

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
            <div>
              <label className="label" htmlFor="discover-network">
                Network
              </label>
              <select
                id="discover-network"
                className={inputCls}
                value={discoverNetwork}
                onChange={(e) => setDiscoverNetwork(e.target.value as DiscoveryNetwork)}
              >
                <option value="all">All layers (surface, deep and dark)</option>
                <option value="surface">Surface only (clear web)</option>
                <option value="deep">Deep only (JavaScript / login)</option>
                <option value="dark">Dark only (Tor)</option>
              </select>
            </div>
            <div>
              <label className="label" htmlFor="crawl-datatype">
                Content type
              </label>
              <select
                id="crawl-datatype"
                className={inputCls}
                value={datatype}
                onChange={(e) => setDatatype(e.target.value as CrawlDatatype)}
                title="What kind of content to look for. Pages that turn out to be a different type are still captured."
              >
                {DATATYPES.map((d) => (
                  <option key={d.value} value={d.value} title={d.hint}>
                    {d.label}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="label" htmlFor="discover-results">
                Max seed URLs
              </label>
              <input
                id="discover-results"
                type="number"
                min={1}
                max={100}
                className={inputCls}
                value={discoverMaxResults}
                onChange={(e) => setDiscoverMaxResults(Number(e.target.value))}
              />
            </div>
            <div>
              <label className="label" htmlFor="discover-lang">
                Language
              </label>
              <select
                id="discover-lang"
                className={inputCls}
                value={language}
                onChange={(e) => setLanguage(e.target.value)}
              >
                <option value="en">English (EN)</option>
                <option value="am">Amharic (AM)</option>
                <option value="all">Both (AM + EN)</option>
              </select>
            </div>
            {/* Max depth sits beside Language in the same grid rather than in
                its own full-width block below, so the two crawl-shaping
                controls read together. */}
            <div>
              <label className="label" htmlFor="discover-depth">
                Max depth per seed: <span className="text-indigo-600 normal-case">{maxDepth}</span>
              </label>
              <input
                id="discover-depth"
                type="range"
                min={0}
                max={10}
                step={1}
                value={maxDepth}
                onChange={(e) => setMaxDepth(Number(e.target.value))}
                className="mt-2.5 w-full accent-sky-500"
              />
              <p className="mt-1 text-[11px] text-slate-500">
                {maxDepth === 0
                  ? "Each seed is crawled once - no recursion"
                  : `Each seed recurses up to depth ${maxDepth}`}
              </p>
            </div>
          </div>

          {error && (
            <Msg tone="error" className="text-sm">{error}</Msg>
          )}

          <div className="flex items-center gap-3 border-t border-slate-800/80 pt-5">
            <button type="submit" className="btn-primary min-w-44" disabled={discovering}>
              {discovering ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
              {discovering ? "Discovering..." : "Start discovery"}
            </button>
          </div>

          {discoverOutcome && (
            <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/10 p-4">
              <div className="flex items-center justify-between gap-2">
                <p className="text-sm font-semibold text-emerald-300">
                  Discovery job queued for {discoverOutcome.max_results} seed(s)
                </p>
                <Link
                  to={`/jobs/${encodeURIComponent(discoverOutcome.job_id)}`}
                  className="inline-flex items-center gap-1 text-[11px] font-medium text-sky-600 hover:text-sky-400"
                >
                  Track
                  <Waypoints className="h-3 w-3" />
                </Link>
              </div>
              <p className="mt-2 break-all font-mono text-[11px] text-slate-400">
                search://{discoverOutcome.network}/{discoverOutcome.query}
              </p>
              <div className="mt-2 flex items-center justify-between gap-2">
                <span className="font-mono text-xs text-sky-600">{discoverOutcome.job_id}</span>
                <CopyButton value={discoverOutcome.job_id} label="" />
              </div>
            </div>
          )}
        </form>
      )}

      <div
        className={cn(
          "grid grid-cols-1 gap-6 xl:grid-cols-3",
          mode === "discover" && "hidden",
        )}
      >
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
                {(() => {
                  const rowDomain = domainOf(entry.value)
                  if (!(allowLogin || allowSignup) || !rowDomain) return null
                  const cred = siteCreds[rowDomain]
                  const hasCred = Boolean(cred?.status?.exists)
                  return (
                    <button
                      type="button"
                      onClick={() => setCredModal(rowDomain)}
                      title={cred?.checking
                        ? "Checking stored credentials…"
                        : hasCred
                          ? `Credential saved for ${rowDomain} — click to update`
                          : `Submit credential for ${rowDomain}`}
                      className={cn(
                        "inline-flex shrink-0 cursor-pointer items-center justify-center rounded-lg border p-2.5 transition",
                        hasCred
                          ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-400 hover:border-emerald-400"
                          : "border-slate-800 bg-slate-950/60 text-slate-500 hover:border-sky-500/50 hover:text-sky-300",
                      )}
                    >
                      {cred?.checking ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : hasCred ? (
                        <ShieldCheck className="h-4 w-4" />
                      ) : (
                        <KeyRound className="h-4 w-4" />
                      )}
                    </button>
                  )
                })()}
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

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
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
                <option value="en">English (EN)</option>
                <option value="am">Amharic (AM)</option>
                <option value="all">Both (AM + EN)</option>
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
            {/* Same picker as topic mode, so the two forms offer identical
                choices. Advisory: a link that turns out to be a PDF or an audio
                file is still captured, whatever is selected here. */}
            <div className="sm:col-span-2 xl:col-span-1">
              <label className="label" htmlFor="crawl-datatype-url">
                Content type
              </label>
              <select
                id="crawl-datatype-url"
                className={inputCls}
                value={datatype}
                onChange={(e) => setDatatype(e.target.value as CrawlDatatype)}
                title="What kind of content to look for. Links that turn out to be a different type are still captured."
              >
                {DATATYPES.map((d) => (
                  <option key={d.value} value={d.value} title={d.hint}>
                    {d.label}
                  </option>
                ))}
              </select>
              <p className="mt-1 text-[11px] text-slate-500">{datatypeHint}</p>
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
                Signups use your configured seed inbox; usernames and display names are generated from server settings.
                Verification emails are read and confirmed automatically — no manual steps. Sites requiring
                phone/SMS verification are marked failed.
              </p>
            )}
          </div>

          {(allowLogin || allowSignup) && (
            <p className="text-[11px] text-slate-500">
              Need a login for a target site? Use the key icon next to each URL to submit that site's
              username and password — stored encrypted per domain and filled in automatically by the worker.
            </p>
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
            <Msg tone="error" className="text-sm">{error}</Msg>
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
                <p className="text-sm font-semibold text-emerald-300">
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
                        <p className="msg msg-error mt-1 text-xs">{o.error}</p>
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

      {/* Site credential popup — one per target domain, opened via the key icon. */}
      {credModal && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
          onClick={() => setCredModal(null)}
        >
          {(() => {
            const domain = credModal
            const cred = siteCreds[domain] ?? {
              status: null,
              checking: false,
              saving: false,
              error: null,
              email: "",
              password: "",
            }
            return (
              <div
                className="w-full max-w-md rounded-xl border border-slate-800 bg-slate-950 p-6 shadow-2xl"
                onClick={(e) => e.stopPropagation()}
              >
                <div className="mb-4 flex items-center gap-2">
                  <KeyRound className="h-4 w-4 text-sky-400" />
                  <span className="text-sm font-semibold text-slate-100">Site credential</span>
                  <code className="rounded bg-slate-900 px-1.5 py-0.5 font-mono text-xs text-sky-300">{domain}</code>
                  {cred.status?.exists && (
                    <span className="ml-auto flex items-center gap-1 text-[11px] font-medium text-emerald-400">
                      <ShieldCheck className="h-3.5 w-3.5" />
                      Saved
                    </span>
                  )}
                </div>

                {cred.status?.exists && (
                  <p className="mb-3 text-[11px] text-slate-500">
                    A credential is already stored for this site — submit new values to update it.
                  </p>
                )}

                {cred.error && (
                  <Msg tone="error" className="mb-3">{cred.error}</Msg>
                )}

                <div className="space-y-3">
                  <div>
                    <label className="label" htmlFor={`sitecred-email-${domain}`}>Email / username</label>
                    <input
                      id={`sitecred-email-${domain}`}
                      className={inputCls}
                      value={cred.email}
                      onChange={(e) => updateSiteCred(domain, { email: e.target.value })}
                      placeholder="admin@example.com"
                      autoComplete="off"
                      autoFocus
                    />
                  </div>
                  <div>
                    <label className="label" htmlFor={`sitecred-pass-${domain}`}>Password</label>
                    <input
                      id={`sitecred-pass-${domain}`}
                      type="password"
                      className={inputCls}
                      value={cred.password}
                      onChange={(e) => updateSiteCred(domain, { password: e.target.value })}
                      placeholder="Site password"
                      autoComplete="new-password"
                    />
                  </div>
                </div>

                <div className="mt-5 flex items-center justify-end gap-2">
                  <button
                    type="button"
                    onClick={() => setCredModal(null)}
                    className="btn-secondary px-4 py-2 text-sm"
                  >
                    Cancel
                  </button>
                  <button
                    type="button"
                    onClick={() => void saveSiteCredential(domain)}
                    disabled={cred.saving || !cred.email.trim() || !cred.password}
                    className="btn-primary px-4 py-2 text-sm disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {cred.saving ? <Loader2 className="h-4 w-4 animate-spin" /> : <KeyRound className="h-4 w-4" />}
                    {cred.saving ? "Saving…" : "Save credential"}
                  </button>
                </div>
                <p className="mt-3 text-[11px] text-slate-500">
                  Stored encrypted. The worker fills these in automatically on the next crawl — no manual login needed.
                </p>
              </div>
            )
          })()}
        </div>
      )}
    </div>
  )
}
