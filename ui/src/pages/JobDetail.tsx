import React, { useCallback, useEffect, useRef, useState } from "react"
import { Link, useParams } from "react-router-dom"
import {
  Activity,
  ArrowLeft,
  Check,
  ChevronDown,
  ChevronRight,
  CircleX,
  Clock3,
  FileText,
  Globe,
  Loader2,
  RefreshCw,
  ShieldAlert,
  ShieldCheck,
  Zap,
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
  Msg,
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
  "transcribing",
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
  transcribing: "Transcribing audio",
  parsing: "Parsing",
  completed: "Completed",
}

/**
 * Topic-discovery jobs use a synthetic ``search://<network>/<query>`` seed URL
 * instead of a real target. Parse it so the header can show the query/network
 * rather than an unclickable pseudo-URL.
 */
function parseDiscoveryUrl(url: string): { network: string; query: string } | null {
  if (!url.startsWith("search://")) return null
  const rest = url.slice("search://".length)
  const slash = rest.indexOf("/")
  if (slash === -1) return { network: rest, query: "" }
  return { network: rest.slice(0, slash), query: decodeURIComponent(rest.slice(slash + 1)) }
}

function stageLabel(stage: string): string {
  return STAGE_LABELS[stage] ?? stage.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
}

// ── Overall job lifecycle ────────────────────────────────────────────────────
type LifecyclePhase = "started" | "working" | "retrying" | "finished"

function deriveLifecycle(
  job: JobDetailType,
  stages: Record<string, StageEvent>,
): LifecyclePhase {
  if (TERMINAL_STATUSES.has(job.status)) return "finished"
  if (stages["camoufox_fallback"]) return "retrying"
  if (
    stages["fetching"] ||
    stages["challenge"] ||
    stages["transcribing"] ||
    stages["parsing"]
  )
    return "working"
  return "started"
}

const LIFECYCLE_STEPS: { key: LifecyclePhase; label: string; icon: React.ReactNode }[] = [
  { key: "started",  label: "Started",  icon: <Zap className="h-3.5 w-3.5" /> },
  { key: "working",  label: "Working",  icon: <Globe className="h-3.5 w-3.5" /> },
  { key: "retrying", label: "Retrying", icon: <RefreshCw className="h-3.5 w-3.5" /> },
  { key: "finished", label: "Finished", icon: <Check className="h-3.5 w-3.5" /> },
]

const PHASE_ORDER: LifecyclePhase[] = ["started", "working", "retrying", "finished"]

function OverallProgressCard({
  job,
  stages,
}: {
  job: JobDetailType
  stages: Record<string, StageEvent>
}) {
  const phase = deriveLifecycle(job, stages)
  const phaseIdx = PHASE_ORDER.indexOf(phase)
  const failed = job.status === "failed"

  /**
   * Steps up to and including the current one. Later phases are not rendered
   * yet: showing "Retrying" and "Finished" greyed out before a job has reached
   * them read as a claim about the job rather than as its current state. They
   * appear as the crawl actually gets to them.
   */
  const visibleSteps = LIFECYCLE_STEPS.filter(
    (s) => PHASE_ORDER.indexOf(s.key) <= phaseIdx,
  )

  // Elapsed clock
  const [, setTick] = useState(0)
  const isActive = !TERMINAL_STATUSES.has(job.status)
  useEffect(() => {
    if (!isActive) return
    const t = window.setInterval(() => setTick((v) => v + 1), 1000)
    return () => window.clearInterval(t)
  }, [isActive])

  const elapsed = formatDuration(job.created_at, job.completed_at ?? new Date().toISOString())

  // Current activity label
  const activityLabel = (() => {
    if (failed) return "Job failed"
    if (job.status === "completed") return "All done — content extracted"
    if (job.status === "skipped") return "Skipped"
    if (stages["camoufox_fallback"]) {
      const ev = stages["camoufox_fallback"]
      return ev.detail ?? "Stealth browser bypass in progress…"
    }
    if (stages["transcribing"]?.state === "active") return "Transcribing audio…"
    if (stages["parsing"]?.state === "active") return "Parsing extracted content…"
    if (stages["challenge"]?.state === "active") return "Solving anti-bot challenge…"
    if (stages["challenge_detected"]) return "Challenge detected — attempting bypass…"
    if (stages["fetching"]?.state === "active") return "Fetching page content…"
    if (stages["login"]?.state === "active") return "Logging in…"
    if (stages["signup"]?.state === "active") return "Signing up…"
    if (stages["verification"]?.state === "active") return "Verifying email…"
    return "Job queued — waiting for worker…"
  })()

  return (
    <div className="card p-5">
      {/* Header row */}
      <div className="mb-4 flex items-center justify-between">
        <h2 className="flex items-center gap-2 text-sm font-semibold text-slate-200">
          <Activity className="h-4 w-4 text-indigo-400" />
          Overall progress
        </h2>
        {elapsed && (
          <span className="flex items-center gap-1 text-xs text-slate-500">
            <Clock3 className="h-3 w-3" />
            {elapsed}
          </span>
        )}
      </div>

      {/* Lifecycle stepper */}
      <div className="mb-5 flex items-center gap-0">
        {visibleSteps.map((step, i) => {
          const stepIdx = PHASE_ORDER.indexOf(step.key)
          // Once the job reaches a terminal state the "finished" step is
          // complete, not in progress. Left as "current" it kept rendering the
          // animate-spin loader forever on completed jobs.
          const terminalStep =
            phase === "finished" && step.key === "finished"
          const isDone = stepIdx < phaseIdx || terminalStep
          const isCurrent = stepIdx === phaseIdx && !terminalStep
          // Skip "retrying" step if it was never reached and job is finished
          if (step.key === "retrying" && phase === "finished" && !stages["camoufox_fallback"]) {
            return null
          }
          const dotStyle = cn(
            "flex h-7 w-7 shrink-0 items-center justify-center rounded-full border transition-all duration-300",
            isDone
              ? "border-emerald-500/50 bg-emerald-500/15 text-emerald-400"
              : isCurrent && failed
                ? "border-rose-500/50 bg-rose-500/15 text-rose-400"
                : isCurrent
                  ? "border-indigo-500/60 bg-indigo-500/15 text-indigo-300 animate-pulse"
                  : "border-slate-800 bg-slate-900/50 text-slate-700",
          )
          return (
            <div key={step.key} className="flex flex-1 items-center">
              <div className="flex flex-col items-center gap-1">
                <div className={dotStyle}>
                  {isCurrent && !failed ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : isCurrent && failed ? (
                    <CircleX className="h-3.5 w-3.5" />
                  ) : isDone ? (
                    <Check className="h-3.5 w-3.5" />
                  ) : (
                    step.icon
                  )}
                </div>
                <span
                  className={cn(
                    "text-[10px] font-medium whitespace-nowrap",
                    isDone
                      ? "text-emerald-400"
                      : isCurrent && failed
                        ? "text-rose-400"
                        : isCurrent
                          ? "text-indigo-300"
                          : "text-slate-500",
                  )}
                >
                  {step.label}
                </span>
              </div>
              {i < visibleSteps.length - 1 && (
                <div
                  className={cn(
                    "mx-2 mb-4 h-px flex-1 transition-all duration-500",
                    isDone ? "bg-emerald-500/40" : "bg-slate-800",
                  )}
                />
              )}
            </div>
          )
        })}
      </div>

      {/* Current activity */}
      <div
        className={cn(
          "flex items-center gap-2 rounded-lg border px-3 py-2 text-xs",
          failed
            ? "border-rose-500/40 bg-rose-500/10 text-rose-300"
            : job.status === "completed"
              ? "border-emerald-500/30 bg-emerald-500/8 text-emerald-300"
              : "border-indigo-500/20 bg-indigo-500/8 text-indigo-200",
        )}
      >
        {failed ? (
          <CircleX className="h-3.5 w-3.5 shrink-0 text-rose-400" />
        ) : job.status === "completed" ? (
          <Check className="h-3.5 w-3.5 shrink-0 text-emerald-400" />
        ) : (
          <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin text-indigo-400" />
        )}
        <span>{activityLabel}</span>
      </div>

      {/* Failure reason */}
      {job.failure_reason && (
        <div className="mt-3">
          <Msg tone="error">
            <span>
              <span className="block text-[11px] font-semibold tracking-wider uppercase opacity-80">
                Why it failed
              </span>
              {friendlyNavError(job.failure_reason)}
            </span>
          </Msg>
        </div>
      )}
    </div>
  )
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

// Stage icon map for site-level events
function stageIcon(
  stage: string,
  state: StageEvent["state"],
  jobTerminal = false,
): React.ReactNode {
  if (state === "passed") return <Check className="h-3 w-3" />
  if (state === "failed") return <CircleX className="h-3 w-3" />
  if (state === "active") {
    // A stage can be left "active" when a worker dies or never publishes a
    // terminal update. Once the job itself has finished, that leftover spinner
    // must stop - otherwise the site timeline shows work "in progress" forever
    // on a job that already completed.
    if (jobTerminal) return <Check className="h-3 w-3 text-emerald-400" />
    return <Loader2 className="h-3 w-3 animate-spin" />
  }
  // Pending icons by stage type
  const icons: Record<string, React.ReactNode> = {
    queued: <Clock3 className="h-3 w-3" />,
    fetching: <Globe className="h-3 w-3" />,
    challenge_detected: <ShieldAlert className="h-3 w-3" />,
    challenge: <ShieldAlert className="h-3 w-3" />,
    camoufox_fallback: <RefreshCw className="h-3 w-3" />,
    login: <Zap className="h-3 w-3" />,
    signup: <Zap className="h-3 w-3" />,
    verification: <Check className="h-3 w-3" />,
    transcribing: <FileText className="h-3 w-3" />,
    parsing: <FileText className="h-3 w-3" />,
  }
  return icons[stage] ?? <Clock3 className="h-3 w-3" />
}

function SiteEventFeed({
  site,
  resolve,
  jobTerminal,
}: {
  site: SiteProgress
  resolve: (ev: StageEvent) => StageEvent
  jobTerminal: boolean
}) {
  const present = STAGE_ORDER.filter((s) => site.stages[s])
  if (present.length === 0) {
    return <p className="text-xs text-slate-600">No stage activity recorded yet.</p>
  }

  return (
    <div className="space-y-1.5">
      {present.map((s) => {
        const ev = resolve(site.stages[s])
        const dotStyle = cn(
          "flex h-5 w-5 shrink-0 items-center justify-center rounded-full border transition-all duration-300",
          ev.state === "passed"
            ? "border-emerald-500/40 bg-emerald-500/12 text-emerald-400"
            : ev.state === "failed"
              ? "border-rose-500/40 bg-rose-500/12 text-rose-400"
              : ev.state === "active"
                ? "border-indigo-500/50 bg-indigo-500/12 text-indigo-300"
                : "border-slate-800 bg-slate-900/50 text-slate-600",
        )
        const labelStyle = cn(
          "text-xs font-medium",
          ev.state === "passed"
            ? "text-emerald-300"
            : ev.state === "failed"
              ? "text-rose-300"
              : ev.state === "active"
                ? "text-indigo-200"
                : "text-slate-500",
        )
        const detailText = ev.state === "failed"
          ? friendlyNavError(ev.detail)
          : ev.detail

        return (
          <div key={s} className="flex items-start gap-2.5">
            <div className="mt-0.5 flex flex-col items-center">
              <div className={dotStyle}>
                {stageIcon(s, ev.state, jobTerminal)}
              </div>
            </div>
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <span className={labelStyle}>{stageLabel(s)}</span>
                {ev.state === "active" && (
                  <span className="rounded-full border border-indigo-500/30 bg-indigo-500/10 px-1.5 py-px text-[9px] font-semibold uppercase tracking-wider text-indigo-300">
                    live
                  </span>
                )}
                {ev.state === "passed" && s === "challenge" && (
                  <span className="flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-1.5 py-px text-[9px] font-semibold text-emerald-400">
                    <ShieldCheck className="h-2.5 w-2.5" /> bypassed
                  </span>
                )}
              </div>
              {detailText && (
                <p className="mt-0.5 break-words text-[11px] leading-snug text-slate-500">
                  {detailText}
                </p>
              )}
            </div>
            {ev.ts && (
              <span className="shrink-0 text-[10px] text-slate-700">
                {new Date(ev.ts).toLocaleTimeString()}
              </span>
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

  const resolve = (ev: StageEvent): StageEvent => {
    if (ev.state !== "active" || !jobTerminal) return ev
    return { ...ev, state: jobFailed ? "failed" : "passed" }
  }

  const [, setTick] = useState(0)
  const hasActive = list.some((s) => s.state === "active")
  useEffect(() => {
    if (!hasActive) return
    const t = window.setInterval(() => setTick((v) => v + 1), 1000)
    return () => window.clearInterval(t)
  }, [hasActive])

  const elapsedLabel = (site: SiteProgress): string => {
    const start = site.startedTs ? new Date(site.startedTs).getTime() : null
    if (start === null || Number.isNaN(start)) return "-"
    const stalledEnd = stalled(site) ? lastActivityTs(site) : null
    const end = site.finishedTs
      ? new Date(site.finishedTs).getTime()
      : stalledEnd ?? (site.state === "active" ? Date.now() : null)
    if (end === null) return "-"
    const sec = Math.max(0, (end - start) / 1000)
    if (sec < 60) return `${sec.toFixed(0)}s`
    return `${Math.floor(sec / 60)}m ${Math.round(sec % 60)}s`
  }

  const done = list.filter((s) => displayState(s) === "passed").length
  const failedCount = list.filter((s) => displayState(s) === "failed" && !stalled(s)).length
  const stalledCount = list.filter((s) => stalled(s)).length
  const activeCount = list.filter((s) => displayState(s) === "active").length

  // Latest stage label for a site
  const latestStageOf = (site: SiteProgress): string => {
    const present = STAGE_ORDER.filter((s) => site.stages[s])
    if (present.length === 0) return ""
    const last = present[present.length - 1]
    const ev = resolve(site.stages[last])
    return stageLabel(last) + (ev.detail ? ` — ${ev.detail}` : "")
  }

  return (
    <div className="card mb-6 overflow-hidden">
      {/* Header */}
      <div className="flex flex-col gap-2 px-5 py-3.5 sm:flex-row sm:items-center sm:justify-between">
        <h2 className="flex items-center gap-2 text-sm font-semibold text-slate-200">
          <Globe className="h-4 w-4 text-emerald-400" />
          Site progress
          <span className="font-normal text-slate-500">({list.length})</span>
        </h2>
        <div className="flex flex-wrap items-center gap-2 text-[11px]">
          {activeCount > 0 && (
            <span className="flex items-center gap-1 rounded-full border border-indigo-500/30 bg-indigo-500/10 px-2 py-0.5 text-indigo-300">
              <Loader2 className="h-2.5 w-2.5 animate-spin" />
              {activeCount} active
            </span>
          )}
          {done > 0 && (
            <span className="flex items-center gap-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-emerald-400">
              <Check className="h-2.5 w-2.5" />
              {done} done
            </span>
          )}
          {failedCount > 0 && (
            <span className="flex items-center gap-1 rounded-full border border-rose-500/30 bg-rose-500/10 px-2 py-0.5 text-rose-400">
              <CircleX className="h-2.5 w-2.5" />
              {failedCount} failed
            </span>
          )}
          {stalledCount > 0 && (
            <span className="flex items-center gap-1 rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-amber-400">
              <Clock3 className="h-2.5 w-2.5" />
              {stalledCount} stalled
            </span>
          )}
        </div>
      </div>

      {/* Site rows */}
      <div className="divide-y divide-slate-800/50 border-t border-slate-800/70">
        {list.map((site) => {
          const isOpen = expanded === site.url
          const visibleState = displayState(site)
          const isStalled = stalled(site)
          const present = STAGE_ORDER.filter((s) => site.stages[s])
          const latestStage = latestStageOf(site)

          // State-based row accent
          const rowAccent = isStalled
            ? "border-l-2 border-l-amber-500/50"
            : visibleState === "active"
              ? "border-l-2 border-l-indigo-500/60"
              : visibleState === "failed"
                ? "border-l-2 border-l-rose-500/50"
                : "border-l-2 border-l-emerald-500/40"

          return (
            <div key={site.url} className={rowAccent}>
              <button
                onClick={() => setExpanded(isOpen ? null : site.url)}
                className="flex w-full items-center gap-3 px-4 py-3 text-left transition hover:bg-slate-800/25"
              >
                {/* State icon */}
                <div className="shrink-0">
                  {isStalled ? (
                    <Clock3 className="h-4 w-4 text-amber-400" />
                  ) : visibleState === "active" ? (
                    <Loader2 className="h-4 w-4 animate-spin text-indigo-400" />
                  ) : visibleState === "failed" ? (
                    <CircleX className="h-4 w-4 text-rose-400" />
                  ) : (
                    <Check className="h-4 w-4 text-emerald-400" />
                  )}
                </div>

                {/* URL + latest stage */}
                <div className="min-w-0 flex-1">
                  <p className="truncate font-mono text-xs text-slate-200">{site.url}</p>
                  {latestStage && (
                    <p
                      className={cn(
                        "mt-0.5 truncate text-[11px]",
                        isStalled
                          ? "text-amber-400"
                          : visibleState === "active"
                            ? "text-indigo-300"
                            : visibleState === "failed"
                              ? "text-rose-400"
                              : "text-emerald-400",
                      )}
                    >
                      {isStalled
                        ? `No updates for ${stallMinutes(site)} min`
                        : latestStage}
                    </p>
                  )}
                </div>

                {/* Stage count pills */}
                {present.length > 0 && (
                  <span className="hidden shrink-0 text-[10px] text-slate-600 sm:block">
                    {present.length} stage{present.length !== 1 ? "s" : ""}
                  </span>
                )}

                {/* Elapsed */}
                <span className="shrink-0 text-[11px] text-slate-500">
                  {elapsedLabel(site)}
                </span>

                {/* Status badge */}
                <span
                  className={cn(
                    "shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-semibold capitalize",
                    isStalled
                      ? "border-amber-500/40 bg-amber-500/10 text-amber-300"
                      : visibleState === "active"
                        ? "border-indigo-500/40 bg-indigo-500/10 text-indigo-300"
                        : visibleState === "failed"
                          ? "border-rose-500/40 bg-rose-500/10 text-rose-300"
                          : "border-emerald-500/40 bg-emerald-500/10 text-emerald-400",
                  )}
                >
                  {isStalled ? "stalled" : visibleState === "passed" ? "done" : visibleState}
                </span>

                {/* Expand chevron */}
                <ChevronDown
                  className={cn(
                    "h-3.5 w-3.5 shrink-0 text-slate-600 transition-transform duration-200",
                    isOpen && "rotate-180",
                  )}
                />
              </button>

              {/* Expanded event feed */}
              {isOpen && (
                <div className="border-t border-slate-800/40 bg-slate-950/50 px-5 py-4">
                  <SiteEventFeed site={site} resolve={resolve} jobTerminal={jobTerminal} />
                  {visibleState === "failed" && (
                    <div className="mt-3">
                      <Msg tone="error">
                        {friendlyNavError(
                          site.stages["site_finished"]?.detail ||
                          site.stages["fetching"]?.detail,
                        ) || "This site could not be processed."}
                      </Msg>
                    </div>
                  )}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

/** Text preview for one expanded row, fetched at most once per item. */
interface ItemSample {
  loading: boolean
  text: string
  truncated: boolean
  error: string | null
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
  // Fetched lazily on first expand. Shipping a preview for every row would mean
  // a 500-row job sending 600 kB of text to render a list.
  const [sample, setSample] = useState<ItemSample>({
    loading: false,
    text: "",
    truncated: false,
    error: null,
  })

  useEffect(() => {
    if (!expanded || sample.text || sample.error || sample.loading) return
    let cancelled = false
    setSample((prev) => ({ ...prev, loading: true }))
    api
      .getItem(item.item_id)
      .then((detail) => {
        if (cancelled) return
        setSample({
          loading: false,
          text: detail.text_sample ?? "",
          truncated: detail.text_sample_truncated ?? false,
          error: null,
        })
      })
      .catch((e: unknown) => {
        if (cancelled) return
        setSample({
          loading: false,
          text: "",
          truncated: false,
          error: e instanceof Error ? e.message : "Could not load the text.",
        })
      })
    return () => {
      cancelled = true
    }
    // `sample` is intentionally not a dependency: it changes as a result of this
    // effect, and depending on it would re-run the fetch on every state change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expanded, item.item_id])
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
        <td data-label="Item ID" className="px-5 py-3 font-mono text-xs text-sky-600">{item.item_id}</td>
        <td data-label="Title / URL" className="max-w-xs px-5 py-3">
          <p className="truncate text-sm text-slate-200">{item.title ?? "(untitled)"}</p>
          <p className="truncate text-[11px] text-slate-500">{item.source_url}</p>
        </td>
        <td data-label="Lang" className="px-5 py-3 text-xs text-slate-400">{languageLabel(item.language)}</td>
        <td data-label="Words" className="px-5 py-3 text-xs text-slate-400">
          {formatNumber(item.word_count)}
        </td>
        <td data-label="Chars" className="px-5 py-3 text-xs text-slate-400">
          {formatNumber(item.character_count)}
        </td>
        <td data-label="Flags" className="px-5 py-3">
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
        <td data-label="Parsed" className="px-5 py-3 text-xs whitespace-nowrap text-slate-500">
          {relativeTime(item.parsed_at)}
        </td>
      </tr>
      {expanded && (
        <tr className="border-t border-slate-800/40 bg-slate-950/60">
          <td />
          <td colSpan={7} className="px-5 py-4">
            {/* Sample of what was actually extracted. The source URL, publish
                date and parsed-at are deliberately not repeated here: the URL is
                already the row's title link, and the dates describe the crawl
                rather than the content. */}
            <div>
              <p className="mb-1.5 text-[11px] tracking-wider text-slate-500 uppercase">
                Sample of extracted text
              </p>
              {sample.loading ? (
                <p className="text-xs text-slate-500">Loading text...</p>
              ) : sample.error ? (
                <p className="text-xs text-amber-400">{sample.error}</p>
              ) : sample.text ? (
                <>
                  {/* Pre-wrap rather than truncate: the point is to read what
                      was captured, and collapsing it would defeat that. */}
                  <pre className="max-h-72 overflow-y-auto rounded-lg border border-slate-800 bg-slate-950/60 p-3 text-xs leading-relaxed whitespace-pre-wrap text-slate-300">
                    {sample.text}
                    {sample.truncated && (
                      <span className="text-slate-500">{" ..."}</span>
                    )}
                  </pre>
                  <div className="mt-2 flex items-center gap-3">
                    <a
                      href={item.source_url}
                      target="_blank"
                      rel="noreferrer"
                      className="text-xs text-indigo-400 hover:text-indigo-300 hover:underline"
                    >
                      Open original page
                    </a>
                  </div>
                </>
              ) : (
                <p className="text-xs text-slate-500">
                  No text was extracted from this page.
                </p>
              )}
            </div>
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

  const discovery = parseDiscoveryUrl(job.url)

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
        <div className="lg:col-span-2">
          <OverallProgressCard job={job} stages={stages} />
        </div>
        <div className="card p-6">
          <dl className="space-y-4">
            <InfoRow label={discovery ? "Topic discovery" : "Target URL"}>
              {discovery ? (
                <span className="flex flex-wrap items-center gap-2 break-all text-slate-200">
                  <span className="rounded-full border border-sky-500/30 bg-sky-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-sky-300">
                    {discovery.network}
                  </span>
                  <span className="font-mono text-xs">{discovery.query || job.url}</span>
                </span>
              ) : (
                <a
                  href={job.url}
                  target="_blank"
                  rel="noreferrer"
                  className="break-all text-indigo-400 hover:text-indigo-300 hover:underline"
                >
                  {job.url}
                </a>
              )}
            </InfoRow>
            <div className="grid grid-cols-2 gap-4">
              <InfoRow label="Language">{languageLabel(job.language)}</InfoRow>
              <InfoRow label="Created">{formatDateTime(job.created_at)}</InfoRow>
              <InfoRow label="Completed">{formatDateTime(job.completed_at)}</InfoRow>
              {/* Duration sits next to Completed: the two answer "when did it
                  end, and how long did it take" as a pair. */}
              <InfoRow label="Duration">
                {formatDuration(job.created_at, job.completed_at)}
              </InfoRow>
            </div>
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
          <div className="table-scroll border-t border-slate-800/70">
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
