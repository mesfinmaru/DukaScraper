import { useCallback, useEffect, useRef, useState } from "react"
import { Link, useParams } from "react-router-dom"
import {
  Activity,
  ArrowLeft,
  Check,
  ChevronRight,
  CircleX,
  Clock3,
  FileText,
  Globe,
  Loader2,
} from "lucide-react"
import { api, ApiError, getJobStatusSocketUrl } from "../api"
import { useRealtimeSocket } from "../useRealtime"
import type { ArticleItem, JobDetail as JobDetailType } from "../types"
import {
  cn,
  formatDateTime,
  formatDuration,
  formatNumber,
  languageLabel,
  relativeTime,
} from "../utils"
import {
  EmptyState,
  ErrorBanner,
  InfoRow,
  LoadingBlock,
  StatusBadge,
} from "../components/ui"

const TERMINAL_STATUSES = new Set(["completed", "failed", "skipped"])

interface StageEvent {
  stage: string
  state: "active" | "passed" | "failed" | "info"
  detail?: string | null
  ts?: string
  url?: string
  itemId?: string
}

interface SiteProgress {
  url: string
  hostname: string
  itemId?: string
  stages: Record<string, StageEvent>
  state: "active" | "passed" | "failed"
  startedTs?: string
  finishedTs?: string
  /** Timestamp of the most recent stage event for this site. */
  lastTs?: string
}

function hostnameOf(url: string): string {
  try {
    return new URL(url).hostname
  } catch {
    return url
  }
}

interface LogLine {
  ts: string
  level: string
  logger: string
  message: string
}

const STAGE_ORDER = [
  "queued",
  "fetching",
  "challenge_detected",
  "challenge",
  "camoufox_fallback",
  "login",
  "signup",
  "verification",
  "parsing",
]

const STAGE_LABELS: Record<string, string> = {
  queued: "Queued",
  fetching: "Fetching page",
  challenge_detected: "Challenge detected",
  challenge: "Challenge",
  camoufox_fallback: "Stealth browser",
  login: "Login",
  signup: "Signup",
  verification: "Email verification",
  parsing: "Parsing",
  completed: "Completed",
}

function stageLabel(stage: string): string {
  return STAGE_LABELS[stage] ?? stage.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
}

/**
 * Turn raw browser/navigation errors into short, human-readable reasons.
 * Playwright strings like "Page.goto: net::ERR_TIMED_OUT at http://…\nCall log:…"
 * become "Site took too long to respond (timeout)" so users can see WHAT
 * happened at a glance instead of parsing an exception dump.
 */
function friendlyNavError(raw: string | null | undefined): string {
  if (!raw) return ""
  const text = String(raw)
  if (/net::ERR_TIMED_OUT|ERR_CONNECTION_TIMED_OUT|TimeoutError|timed out/i.test(text))
    return "Site took too long to respond (timeout)"
  if (/net::ERR_CONNECTION_REFUSED/i.test(text)) return "Connection refused by the site"
  if (/net::ERR_NAME_NOT_RESOLVED|ERR_NAME_RESOLUTION_FAILED/i.test(text))
    return "Site address could not be resolved (DNS)"
  if (/net::ERR_CONNECTION_(RESET|CLOSED|ABORTED)/i.test(text)) return "Connection was reset by the site"
  if (/net::ERR_TUNNEL_CONNECTION_FAILED|proxy/i.test(text)) return "Proxy/Tunnel connection failed"
  if (/net::ERR_HTTP2_PROTOCOL_ERROR/i.test(text)) return "Protocol error (HTTP/2)"
  if (/net::ERR_CERT_|SSL|TLS/i.test(text)) return "Certificate/SSL error"
  if (/robots\.txt|robots_disallowed/i.test(text)) return "Blocked by robots.txt"
  if (/challenge|cloudflare|captcha/i.test(text)) return "Anti-bot challenge could not be cleared"
  if (/ERR_NO_SUPPORTED_PROXIES/i.test(text)) return "No working proxy available"
  // Fallback: strip the noisy Playwright wrapper and keep the first line.
  const firstLine = text.split("\n").find((l) => l.trim().length > 0) ?? text
  return firstLine.replace(/^(Page\.goto|Page\.evaluate)?:?\s*/i, "").trim() || text
}

function StageTimeline({
  job,
  stages,
}: {
  job: JobDetailType
  stages: Record<string, StageEvent>
}) {
  const failed = job.status === "failed"
  const jobTerminal = TERMINAL_STATUSES.has(job.status)

  // When the job has reached a terminal state, no stage may keep spinning:
  // resolve any lingering "active" stage from the final job status. Derived
  // at render time so it also applies to WebSocket replays.
  const effectiveState = (state: StageEvent["state"]): StageEvent["state"] => {
    if (state !== "active" || !jobTerminal) return state
    return failed ? "failed" : "passed"
  }

  // Build the ordered list of stages to show: fixed prefixes plus any dynamic
  // stages that actually arrived, in canonical order.
  const present = STAGE_ORDER.filter((s) => stages[s])
  const nodes: { key: string; label: string; ev: StageEvent }[] = [
    { key: "queued", label: stageLabel("queued"), ev: stages.queued ?? { stage: "queued", state: "passed" } },
    ...present
      .filter((s) => s !== "queued")
      .map((s) => ({ key: s, label: stageLabel(s), ev: { ...stages[s], state: effectiveState(stages[s].state) } })),
  ]
  if (TERMINAL_STATUSES.has(job.status)) {
    nodes.push({
      key: "completed",
      label: failed ? "Failed" : job.status === "skipped" ? "Skipped" : "Completed",
      ev: { stage: "completed", state: failed ? "failed" : "passed" },
    })
  }

  const stateStyle = (state: StageEvent["state"], isLast: boolean) => {
    if (state === "passed") return "border-emerald-500/40 bg-emerald-500/10 text-emerald-500"
    if (state === "failed") return "border-rose-500/50 bg-rose-500/10 text-rose-400"
    if (state === "active") return "border-sky-500/50 bg-sky-500/10 text-sky-300"
    return isLast
      ? "border-indigo-500/40 bg-indigo-500/10 text-indigo-300"
      : "border-slate-800 bg-slate-900 text-slate-600"
  }

  return (
    <div className="flex flex-wrap items-start gap-y-3">
      {nodes.map((node, i) => {
        const isLast = i === nodes.length - 1
        const spinner = node.ev.state === "active"
        return (
          <div key={node.key} className="flex items-start">
            <div className="flex min-w-[64px] flex-col items-center gap-1">
              <div
                className={cn(
                  "flex h-6 w-6 items-center justify-center rounded-full border",
                  stateStyle(node.ev.state, isLast),
                  node.ev.state === "active" && "animate-pulse",
                )}
                title={node.ev.detail ?? node.label}
              >
                {node.ev.state === "failed" ? (
                  <CircleX className="h-3 w-3" />
                ) : node.ev.state === "passed" ? (
                  <Check className="h-3 w-3" />
                ) : spinner ? (
                  <Loader2 className="h-3 w-3 animate-spin" />
                ) : (
                  <Clock3 className="h-3 w-3" />
                )}
              </div>
              <span
                className={cn(
                  "max-w-[80px] text-center text-[10px] font-medium leading-tight",
                  node.ev.state === "passed"
                    ? "text-slate-300"
                    : node.ev.state === "failed"
                      ? "text-rose-300"
                      : node.ev.state === "active"
                        ? "text-sky-300"
                        : "text-slate-500",
                )}
              >
                {node.label}
              </span>
              {node.ev.detail && (
                <span
                  className="max-w-[88px] truncate text-center text-[9px] leading-tight text-slate-600"
                  title={node.ev.state === "failed" ? friendlyNavError(node.ev.detail) : node.ev.detail}
                >
                  {node.ev.state === "failed" ? friendlyNavError(node.ev.detail) : node.ev.detail}
                </span>
              )}
            </div>
            {i < nodes.length - 1 && (
              <div
                className={cn(
                  "mx-1.5 mt-3 h-px w-6",
                  node.ev.state === "passed"
                    ? "bg-emerald-500/40"
                    : node.ev.state === "failed"
                      ? "bg-rose-500/40"
                      : "bg-slate-800",
                )}
              />
            )}
          </div>
        )
      })}
    </div>
  )
}

function SitesPanel({
  sites,
  jobTerminal,
  jobFailed,
}: {
  sites: Record<string, SiteProgress>
  jobTerminal: boolean
  jobFailed: boolean
}) {
  const [expanded, setExpanded] = useState<string | null>(null)
  const list = Object.values(sites).sort((a, b) => {
    if (a.state === "active" && b.state !== "active") return -1
    if (b.state === "active" && a.state !== "active") return 1
    return (a.startedTs ?? "").localeCompare(b.startedTs ?? "")
  })
  // Stall guard: a site whose worker stopped sending updates (crash without
  // cleanup, lost Kafka message) must not spin forever. After STALL_AFTER_MS
  // with no stage event it is shown as "stalled" instead of "active". The
  // backend watchdog still owns the real settlement — this is display only.
  const STALL_AFTER_MS = 10 * 60 * 1000
  const lastActivityTs = (site: SiteProgress): number | null => {
    const raw = site.lastTs ?? site.startedTs
    if (!raw) return null
    const t = new Date(raw).getTime()
    return Number.isNaN(t) ? null : t
  }
  const stalled = (site: SiteProgress): boolean => {
    if (site.state !== "active" || jobTerminal) return false
    const last = lastActivityTs(site)
    return last !== null && Date.now() - last > STALL_AFTER_MS
  }
  const stallMinutes = (site: SiteProgress): number => {
    const last = lastActivityTs(site)
    const ms = last === null ? STALL_AFTER_MS : Date.now() - last
    return Math.max(1, Math.floor(ms / 60000))
  }
  const displayState = (site: SiteProgress): SiteProgress["state"] => {
    if (site.state === "active" && !jobTerminal && stalled(site)) return "failed"
    if (site.state !== "active" || !jobTerminal) return site.state
    return jobFailed ? "failed" : "passed"
  }
  const activeSite = list.find((s) => displayState(s) === "active")
  const done = list.filter((s) => displayState(s) === "passed").length
  const failedCount = list.filter((s) => displayState(s) === "failed" && !stalled(s)).length
  const stalledCount = list.filter((s) => stalled(s)).length

  // When the job reached a terminal state, resolve lingering "active" stages
  // so no spinner keeps spinning on a finished job.
  const resolve = (ev: StageEvent): StageEvent => {
    if (ev.state !== "active" || !jobTerminal) return ev
    return { ...ev, state: jobFailed ? "failed" : "passed" }
  }

  // Live elapsed clock — only ticks while something is still running.
  const [, setTick] = useState(0)
  // Keep the clock ticking while any site is raw-active so stalled badges
  // and their "no updates for X min" labels keep updating too.
  const hasActive = list.some((s) => s.state === "active")
  useEffect(() => {
    if (!hasActive) return
    const t = window.setInterval(() => setTick((v) => v + 1), 1000)
    return () => window.clearInterval(t)
  }, [hasActive])

  const activeStageOf = (site: SiteProgress): StageEvent | null => {
    const present = STAGE_ORDER.filter((s) => site.stages[s])
    for (let i = present.length - 1; i >= 0; i--) {
      const ev = resolve(site.stages[present[i]])
      if (ev.state === "active") return ev
    }
    return null
  }

  const elapsedLabel = (site: SiteProgress): string => {
    const start = site.startedTs ? new Date(site.startedTs).getTime() : null
    if (start === null || Number.isNaN(start)) return "-"
    // Stalled sites freeze the clock at their last update instead of ticking.
    const stalledEnd = stalled(site) ? lastActivityTs(site) : null
    const end = site.finishedTs
      ? new Date(site.finishedTs).getTime()
      : stalledEnd ?? (site.state === "active" ? Date.now() : null)
    if (end === null) return "-"
    const sec = Math.max(0, (end - start) / 1000)
    if (sec < 60) return `${sec.toFixed(0)} s`
    return `${Math.floor(sec / 60)}m ${Math.round(sec % 60)}s`
  }

  const badgeStyle = (state: SiteProgress["state"]) =>
    state === "active"
      ? "border-sky-500/50 bg-sky-500/10 text-sky-300"
      : state === "failed"
        ? "border-rose-500/50 bg-rose-500/10 text-rose-300"
        : "border-emerald-500/40 bg-emerald-500/10 text-emerald-600"

  return (
    <div className="card mb-6 overflow-hidden">
      <div className="flex flex-col gap-2 px-5 py-3 sm:flex-row sm:items-center sm:justify-between">
        <h2 className="flex items-center gap-2 text-sm font-semibold text-slate-200">
          <Globe className="h-4 w-4 text-emerald-400" />
          Site progress
          <span className="font-normal text-slate-500">({list.length})</span>
        </h2>
        <span className="text-xs text-slate-500">
          {done} done{failedCount > 0 ? ` · ${failedCount} failed` : ""}
          {stalledCount > 0 ? ` · ${stalledCount} stalled` : ""} ·{" "}
          {activeSite ? 1 : 0} active · {list.length} discovered
        </span>
      </div>

      <div className="divide-y divide-slate-800/60 border-t border-slate-800/70">
        {list.map((site) => {
          const isOpen = expanded === site.url
          const visibleState = displayState(site)
          const isStalled = stalled(site)
          const present = STAGE_ORDER.filter((s) => site.stages[s])
          const activeSt = visibleState === "active" ? activeStageOf(site) : null
          return (
            <div key={site.url}>
              <button
                onClick={() => setExpanded(isOpen ? null : site.url)}
                className="flex w-full items-center gap-3 px-5 py-3 text-left transition hover:bg-slate-800/30"
                title={isOpen ? "Collapse details" : "Expand site timeline"}
              >
                <ChevronRight
                  className={cn(
                    "h-4 w-4 shrink-0 text-slate-600 transition-transform",
                    isOpen && "rotate-90",
                  )}
                />
                {isStalled ? (
                  <Clock3 className="h-4 w-4 shrink-0 text-amber-400" />
                ) : visibleState === "active" ? (
                  <Loader2 className="h-4 w-4 shrink-0 animate-spin text-sky-400" />
                ) : visibleState === "failed" ? (
                  <CircleX className="h-4 w-4 shrink-0 text-rose-400" />
                ) : (
                  <Check className="h-4 w-4 shrink-0 text-emerald-500" />
                )}
                <span className="min-w-0 flex-1 break-all text-left font-mono text-xs text-slate-200">
                  {site.url}
                </span>
                {activeSt && (
                  <span className="hidden max-w-[280px] truncate text-xs text-sky-300 sm:block">
                    {stageLabel(activeSt.stage)}
                    {activeSt.detail ? ` — ${activeSt.detail}` : ""}
                  </span>
                )}
                {visibleState === "failed" && !isStalled && (
                  <span className="hidden max-w-[420px] truncate text-xs font-medium text-rose-600 sm:block">
                    {friendlyNavError(site.stages["site_finished"]?.detail || site.stages["fetching"]?.detail) || "Site failed"}
                  </span>
                )}
                {isStalled && (
                  <span className="hidden max-w-[280px] truncate text-xs text-amber-300 sm:block">
                    No worker updates for {stallMinutes(site)} min
                  </span>
                )}
                <span className="shrink-0 text-[11px] text-slate-500">
                  {elapsedLabel(site)}
                </span>
                <span
                  className={cn(
                    "shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-medium capitalize",
                    isStalled
                      ? "border-amber-500/50 bg-amber-500/10 text-amber-300"
                      : badgeStyle(visibleState),
                  )}
                >
                  {isStalled ? "stalled" : visibleState}
                </span>
              </button>
              {isOpen && (
                <div className="border-t border-slate-800/40 bg-slate-950/60 px-5 py-3">
                  {present.length === 0 ? (
                    <p className="text-xs text-slate-600">
                      No stage activity recorded for this site yet.
                    </p>
                  ) : (
                    <div className="flex flex-wrap items-start gap-y-2">
                      {present.map((s, i) => {
                        const ev = resolve(site.stages[s])
                        return (
                          <div key={s} className="flex items-start">
                            <div className="flex min-w-[64px] flex-col items-center gap-1">
                              <div
                                className={cn(
                                  "flex h-5 w-5 items-center justify-center rounded-full border",
                                  ev.state === "passed"
                                    ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-500"
                                    : ev.state === "failed"
                                      ? "border-rose-500/50 bg-rose-500/10 text-rose-400"
                                      : ev.state === "active"
                                        ? "border-sky-500/50 bg-sky-500/10 text-sky-300"
                                        : "border-slate-800 bg-slate-900 text-slate-600",
                                  ev.state === "active" && "animate-pulse",
                                )}
                                title={ev.detail ?? stageLabel(s)}
                              >
                                {ev.state === "failed" ? (
                                  <CircleX className="h-3 w-3" />
                                ) : ev.state === "passed" ? (
                                  <Check className="h-3 w-3" />
                                ) : ev.state === "active" ? (
                                  <Loader2 className="h-3 w-3 animate-spin" />
                                ) : (
                                  <Clock3 className="h-3 w-3" />
                                )}
                              </div>
                              <span
                                className={cn(
                                  "max-w-[80px] text-center text-[9px] font-medium leading-tight",
                                  ev.state === "passed"
                                    ? "text-slate-300"
                                    : ev.state === "failed"
                                      ? "text-rose-300"
                                      : ev.state === "active"
                                        ? "text-sky-300"
                                        : "text-slate-500",
                                )}
                              >
                                {stageLabel(s)}
                              </span>
                              {ev.detail && (
                                <span
                                  className="max-w-[88px] truncate text-center text-[9px] leading-tight text-slate-600"
                                  title={ev.detail}
                                >
                                  {ev.detail}
                                </span>
                              )}
                            </div>
                            {i < present.length - 1 && (
                              <div
                                className={cn(
                                  "mx-1.5 mt-2.5 h-px w-6",
                                  ev.state === "passed"
                                    ? "bg-emerald-500/40"
                                    : ev.state === "failed"
                                      ? "bg-rose-500/40"
                                      : "bg-slate-800",
                                )}
                              />
                            )}
                          </div>
                        )
                      })}
                    </div>
                  )}
                  <p className="mt-3 break-all font-mono text-[10px] text-slate-600">
                    {site.url}
                  </p>
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

function ItemRow({
  item,
  expanded,
  onToggle,
}: {
  item: ArticleItem
  expanded: boolean
  onToggle: () => void
}) {
  return (
    <>
      <tr
        onClick={onToggle}
        className="cursor-pointer transition hover:bg-slate-800/30"
        title={expanded ? "Collapse details" : "Expand details"}
      >
        <td className="px-5 py-3">
          <ChevronRight
            className={cn("h-4 w-4 text-slate-600 transition-transform", expanded && "rotate-90")}
          />
        </td>
        <td className="px-5 py-3 font-mono text-xs text-sky-600">{item.item_id}</td>
        <td className="max-w-xs px-5 py-3">
          <p className="truncate text-sm text-slate-200">{item.title ?? "(untitled)"}</p>
          <p className="truncate text-[11px] text-slate-500">{item.source_url}</p>
        </td>
        <td className="px-5 py-3 text-xs text-slate-400">{languageLabel(item.language)}</td>
        <td className="px-5 py-3 text-xs text-slate-400">
          {formatNumber(item.word_count)}
        </td>
        <td className="px-5 py-3 text-xs text-slate-400">
          {formatNumber(item.character_count)}
        </td>
        <td className="px-5 py-3">
          <div className="flex gap-1.5">
            {item.is_exported && (
              <span className="rounded-full border border-emerald-500/40 bg-emerald-500/10 px-2 py-0.5 text-[11px] font-medium text-emerald-600">
                Exported
              </span>
            )}
            {item.intelligence_processed && (
              <span className="rounded-full border border-violet-500/40 bg-violet-500/10 px-2 py-0.5 text-[11px] font-medium text-violet-500">
                Analyzed
              </span>
            )}
          </div>
        </td>
        <td className="px-5 py-3 text-xs whitespace-nowrap text-slate-500">
          {relativeTime(item.parsed_at)}
        </td>
      </tr>
      {expanded && (
        <tr className="border-t border-slate-800/40 bg-slate-950/60">
          <td />
          <td colSpan={7} className="px-5 py-4">
            <dl className="grid grid-cols-1 gap-x-8 gap-y-4 sm:grid-cols-2 lg:grid-cols-3">
              <InfoRow label="Source URL">
                <a
                  href={item.source_url}
                  target="_blank"
                  rel="noreferrer"
                  className="break-all text-indigo-400 hover:text-indigo-300 hover:underline"
                >
                  {item.source_url}
                </a>
              </InfoRow>
              <InfoRow label="Publish date">{formatDateTime(item.publish_date)}</InfoRow>
              <InfoRow label="Parsed at">{formatDateTime(item.parsed_at)}</InfoRow>
            </dl>
          </td>
        </tr>
      )}
    </>
  )
}

export default function JobDetail() {
  const { jobId = "" } = useParams()
  const [job, setJob] = useState<JobDetailType | null>(null)
  const [jobError, setJobError] = useState<string | null>(null)
  const [items, setItems] = useState<ArticleItem[] | null>(null)
  const [itemsError, setItemsError] = useState<string | null>(null)
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const [stages, setStages] = useState<Record<string, StageEvent>>({})
  const [sites, setSites] = useState<Record<string, SiteProgress>>({})
  const [logLines, setLogLines] = useState<LogLine[]>([])
  const logBoxRef = useRef<HTMLDivElement | null>(null)

  const load = useCallback(async () => {
    try {
      const res = await api.getJob(jobId)
      setJob(res)
      setJobError(null)
    } catch (e) {
      if (e instanceof ApiError && e.status === 404) {
        setJobError(`Job ${jobId} was not found.`)
      } else {
        setJobError(e instanceof Error ? e.message : "Failed to load job")
      }
      return
    }

    try {
      const res = await api.getJobArticles(jobId)
      setItems(res.items)
      setItemsError(null)
    } catch (e) {
      if (e instanceof ApiError && e.status === 404) {
        setItems([])
        setItemsError(null)
      } else {
        setItemsError(e instanceof Error ? e.message : "Failed to load parsed items")
      }
    }
  }, [jobId])

  // Load once on mount; live updates arrive over the job WebSocket instead of
  // a fixed 5s poll. Refreshes triggered by push events are coalesced so a
  // burst of item_parsed events results in a single reload.
  const refreshQueued = useRef(false)
  const scheduleRefresh = useCallback(() => {
    if (refreshQueued.current) return
    refreshQueued.current = true
    window.setTimeout(() => {
      refreshQueued.current = false
      void load()
    }, 750)
  }, [load])

  useEffect(() => {
    void load()
  }, [load])

  // WebSocket push events are best-effort (Redis pub/sub); if they never
  // arrive the page would sit on "queued/running" forever. Poll while the
  // job is still active so the status always converges to completed/failed.
  useEffect(() => {
    if (!job) return
    const active = !TERMINAL_STATUSES.has(job.status) && job.status !== "needs_review"
    const waitingForParser = TERMINAL_STATUSES.has(job.status) && items !== null && items.length === 0
    if (!active && !waitingForParser) return
    const timer = window.setInterval(() => {
      if (document.visibilityState !== "visible") return
      void load()
    }, 5000)
    return () => window.clearInterval(timer)
  }, [items, job, load])

  // Keep the log console pinned to the newest line.
  useEffect(() => {
    if (logBoxRef.current) {
      logBoxRef.current.scrollTop = logBoxRef.current.scrollHeight
    }
  }, [logLines])

  useRealtimeSocket({
    url: jobId ? getJobStatusSocketUrl(jobId) : null,
    enabled: true,
    onEvent: (payload) => {
      if (payload.event === "job_stage" && typeof payload.stage === "string") {
        const ev: StageEvent = {
          stage: payload.stage,
          state: (payload.state as StageEvent["state"]) ?? "info",
          detail: typeof payload.detail === "string" ? payload.detail : null,
          ts: typeof payload.ts === "string" ? payload.ts : undefined,
          url: typeof payload.url === "string" ? payload.url : undefined,
          itemId: typeof payload.item_id === "string" ? payload.item_id : undefined,
        }
        const siteUrl = ev.url
        // --- Item-level (per-site) progress ---
        if (siteUrl && ev.stage === "site_started") {
          setSites((prev) => ({
            ...prev,
            [siteUrl]: {
              url: siteUrl,
              hostname: hostnameOf(siteUrl),
              itemId: ev.itemId,
              stages: {},
              state: "active",
              startedTs: ev.ts,
              lastTs: ev.ts,
            },
          }))
          return
        }
        if (siteUrl && ev.stage === "site_finished") {
          setSites((prev) => {
            const site = prev[siteUrl]
            if (!site) return prev
            return {
              ...prev,
              [siteUrl]: {
                ...site,
                state: ev.state === "failed" ? "failed" : "passed",
                finishedTs: ev.ts,
                lastTs: ev.ts,
              },
            }
          })
          // A site that finished as failed must also close the job-level
          // "Fetching page" spinner as failed — otherwise the timeline above
          // showed a green check while the site below was red (the screenshot
          // bug: onion site that timed out but displayed "Passed").
          if (ev.state === "failed") {
            setStages((prev) => {
              const fetching = prev["fetching"]
              if (!fetching || fetching.state !== "active") return prev
              return {
                ...prev,
                fetching: {
                  ...fetching,
                  state: "failed",
                  detail: ev.detail ?? fetching.detail,
                },
              }
            })
          }
          return
        }
        if (siteUrl) {
          // Site-scoped pipeline stage: record it on that site's own
          // timeline (and keep feeding the job-level summary below).
          setSites((prev) => {
            const site = prev[siteUrl] ?? {
              url: siteUrl,
              hostname: hostnameOf(siteUrl),
              itemId: ev.itemId,
              stages: {},
              state: "active",
              startedTs: ev.ts,
              lastTs: ev.ts,
            }
            const nextStages = { ...site.stages, [ev.stage]: ev }
            if (ev.state === "active") {
              for (const key of Object.keys(nextStages)) {
                if (key !== ev.stage && nextStages[key].state === "active") {
                  nextStages[key] = { ...nextStages[key], state: "passed" }
                }
              }
            }
            return {
              ...prev,
              [siteUrl]: { ...site, stages: nextStages, state: "active", lastTs: ev.ts },
            }
          })
          // A site-scoped failure must also mark the matching job-level stage
          // failed — the main timeline may never show a green step for
          // something that actually broke.
          if (ev.state === "failed") {
            setStages((prev) => {
              const jobStage = prev[ev.stage]
              if (!jobStage || jobStage.state === "failed") return prev
              return { ...prev, [ev.stage]: { ...jobStage, state: "failed" } }
            })
          }
        }
        setStages((prev) => {
          const next = { ...prev, [ev.stage]: ev }
          // When the pipeline moves on to a new stage, any other stage still
          // "active" finished without emitting an explicit event — resolve it
          // so its spinner stops the moment the next step begins.
          if (ev.state === "active") {
            for (const key of Object.keys(next)) {
              if (key !== ev.stage && next[key].state === "active") {
                next[key] = { ...next[key], state: "passed" }
              }
            }
          }
          return next
        })
        // The final stage transition doubles as a status refresh trigger.
        if (ev.stage === "completed" || ev.stage === "parsing") scheduleRefresh()
        return
      }
      if (payload.event === "job_log") {
        const line: LogLine = {
          ts: typeof payload.ts === "string" ? payload.ts : new Date().toISOString(),
          level: typeof payload.level === "string" ? payload.level : "INFO",
          logger: typeof payload.logger === "string" ? payload.logger : "worker",
          message: typeof payload.message === "string" ? payload.message : "",
        }
        setLogLines((prev) => {
          const next = [...prev, line]
          return next.length > 500 ? next.slice(next.length - 500) : next
        })
        return
      }
      if (typeof payload.event === "string") scheduleRefresh()
    },
    onOpen: () => scheduleRefresh(),
    // If the socket is down (e.g. Redis unavailable) keep a slow REST fallback
    // while the job is still active.
    onFallback: () => {
      const settled =
        job !== null && (TERMINAL_STATUSES.has(job.status) || job.status === "needs_review")
      if (!settled) void load()
    },
    fallbackMs: 15000,
  })

  if (jobError) {
    return (
      <div>
        <Link to="/jobs" className="mb-6 inline-flex items-center gap-2 text-sm text-slate-400 transition hover:text-slate-200">
          <ArrowLeft className="h-4 w-4" /> All jobs
        </Link>
        <ErrorBanner message={jobError} />
      </div>
    )
  }

  if (!job) {
    return <LoadingBlock label={`Loading ${jobId}...`} />
  }

  return (
    <div>
      <Link
        to="/jobs"
        className="mb-6 inline-flex items-center gap-2 text-sm text-slate-400 transition hover:text-slate-200"
      >
        <ArrowLeft className="h-4 w-4" /> All jobs
      </Link>

      <div className="mb-6 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 items-center gap-3">
          <h1 className="font-mono text-2xl font-bold tracking-tight text-slate-50">
            {job.job_id}
          </h1>
          <StatusBadge status={job.status} />
        </div>
      </div>

      <div className="mb-6 grid grid-cols-1 gap-6 lg:grid-cols-3">
        <div className="card p-6 lg:col-span-2">
          <StageTimeline job={job} stages={stages} />
          {job.failure_reason && (
            <div className="mt-4 rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3">
              <p className="text-xs font-semibold text-rose-600">Failure reason</p>
              <p className="mt-1 break-words text-sm text-rose-500">{friendlyNavError(job.failure_reason)}</p>
            </div>
          )}
        </div>
        <div className="card p-6">
          <dl className="space-y-4">
            <InfoRow label="Target URL">
              <a
                href={job.url}
                target="_blank"
                rel="noreferrer"
                className="break-all text-indigo-400 hover:text-indigo-300 hover:underline"
              >
                {job.url}
              </a>
            </InfoRow>
            <div className="grid grid-cols-2 gap-4">
              <InfoRow label="Language">{languageLabel(job.language)}</InfoRow>
              <InfoRow label="User ID">
                <span className="font-mono text-xs">{job.user_id}</span>
              </InfoRow>
              <InfoRow label="Created">{formatDateTime(job.created_at)}</InfoRow>
              <InfoRow label="Completed">{formatDateTime(job.completed_at)}</InfoRow>
            </div>
            <InfoRow label="Duration">
              {formatDuration(job.created_at, job.completed_at)}
            </InfoRow>
          </dl>
        </div>
      </div>

      {(Object.keys(sites).length > 0 ||
        (!TERMINAL_STATUSES.has(job.status) && job.status !== "needs_review")) && (
        <SitesPanel
          sites={sites}
          jobTerminal={TERMINAL_STATUSES.has(job.status)}
          jobFailed={job.status === "failed"}
        />
      )}

      <div className="card mb-6 overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3">
          <h2 className="flex items-center gap-2 text-sm font-semibold text-slate-200">
            <Activity className="h-4 w-4 text-emerald-400" />
            Logs
          </h2>
        </div>
        <div
          ref={logBoxRef}
          className="max-h-80 overflow-y-auto border-t border-slate-800/70 bg-black/95 px-4 py-3 font-mono text-[11px] leading-relaxed"
        >
          {logLines.length === 0 ? (
            <p className="text-slate-600">
              Waiting for worker output… (log lines tagged with this job id stream in real time)
            </p>
          ) : (
            logLines.map((line, i) => (
              <div key={i} className="flex gap-2 whitespace-pre-wrap break-all">
                <span className="shrink-0 text-slate-600">
                  {line.ts ? new Date(line.ts).toLocaleTimeString() : ""}
                </span>
                <span
                  className={cn(
                    "w-[64px] shrink-0 font-semibold",
                    line.level === "ERROR"
                      ? "text-rose-500"
                      : line.level === "WARNING"
                        ? "text-amber-500"
                        : "text-sky-500",
                  )}
                >
                  {line.level}
                </span>
                <span
                  className={cn(
                    "break-words",
                    line.level === "ERROR"
                      ? "text-rose-300"
                      : line.level === "WARNING"
                        ? "text-amber-300"
                        : "text-slate-300",
                  )}
                >
                  {line.message}
                </span>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="card overflow-hidden">
        <div className="flex items-center justify-between px-5 py-4">
          <h2 className="flex items-center gap-2 text-sm font-semibold text-slate-200">
            <FileText className="h-4 w-4 text-sky-400" />
            Parsed items{" "}
            <span className="font-normal text-slate-500">
              {items ? `(${items.length})` : ""}
            </span>
          </h2>
          {items && items.length > 0 && (
            <span className="text-xs text-slate-500">Click a row for metadata</span>
          )}
        </div>

        {itemsError && (
          <div className="px-5 pb-4">
            <ErrorBanner message={itemsError} onRetry={() => void load()} />
          </div>
        )}

        {items === null ? (
          <LoadingBlock label="Checking for parsed items..." />
        ) : items.length === 0 ? (
          <EmptyState
            title={
              TERMINAL_STATUSES.has(job.status)
                ? "No parsed items were produced by this job"
                : "No parsed items yet"
            }
            hint={
              TERMINAL_STATUSES.has(job.status)
                ? undefined
                : "Raw HTML is stored in MinIO first; the parser worker will publish results here shortly."
            }
            icon={<FileText className="h-8 w-8" />}
          />
        ) : (
          <div className="overflow-x-auto border-t border-slate-800/70">
            <table className="w-full min-w-[820px]">
              <thead className="bg-slate-900/40">
                <tr>
                  {[
                    "",
                    "Item ID",
                    "Title / URL",
                    "Lang",
                    "Words",
                    "Chars",
                    "Flags",
                    "Parsed",
                  ].map((h, i) => (
                    <th
                      key={i}
                      className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase"
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/60">
                {items.map((item) => (
                  <ItemRow
                    key={item.item_id}
                    item={item}
                    expanded={expandedId === item.item_id}
                    onToggle={() =>
                      setExpandedId((cur) => (cur === item.item_id ? null : item.item_id))
                    }
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
