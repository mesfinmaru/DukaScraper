/**
 * Alerts — the high-severity (4/5) intelligence feed.
 *
 * Two behaviours here are the reason this is not just a filtered table:
 *
 *  1. The badge clears as you read. Scrolling to the end of the list marks
 *     everything above the fold read, because the operator has demonstrably
 *     seen it; leaving a badge that only clears via an explicit "mark all"
 *     button trains people to ignore it.
 *
 *  2. Unread rows stay visually loud until acted on, and read ones recede. An
 *     alert feed that looks identical whether or not you have triaged it gives
 *     no signal about what still needs attention.
 */
import { useCallback, useEffect, useRef, useState } from "react"
import { Link, useSearchParams } from "react-router-dom"
import { AlertTriangle, ExternalLink, ShieldAlert } from "lucide-react"
import { api } from "../api"
import { useAuth } from "../config"
import { DEFAULT_REFRESH_MS, useAutoRefresh } from "../useAutoRefresh"
import type { AlertRow } from "../types"
import { EmptyState, ErrorBanner, LoadingBlock, PageHeader } from "../components/ui"
import { cn } from "../utils"

const PAGE_SIZE = 25

/** Rows scrolled fully past are considered seen. */
const SEEN_MARGIN_PX = 120

const PRIORITY_STYLES: Record<string, string> = {
  critical: "border-rose-500/50 bg-rose-500/15 text-rose-300",
  high: "border-amber-500/50 bg-amber-500/15 text-amber-300",
  system: "border-cyan-500/50 bg-cyan-500/15 text-cyan-300",
  low: "border-slate-600 bg-slate-800/60 text-slate-400",
}

function SeverityPill({ severity, priority }: { severity: number; priority: string }) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wide",
        PRIORITY_STYLES[priority] ?? PRIORITY_STYLES.low,
      )}
    >
      <ShieldAlert className="h-3 w-3" />
      {severity}/5
    </span>
  )
}

function timeAgo(iso: string | null): string {
  if (!iso) return ""
  const then = new Date(iso.endsWith("Z") ? iso : `${iso}Z`).getTime()
  if (Number.isNaN(then)) return iso
  const seconds = Math.max(0, Math.floor((Date.now() - then) / 1000))
  if (seconds < 60) return "just now"
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return `${Math.floor(hours / 24)}d ago`
}

export default function Alerts() {
  const { session } = useAuth()
  const [params] = useSearchParams()
  const focusId = params.get("alert")

  const [rows, setRows] = useState<AlertRow[]>([])
  const [total, setTotal] = useState(0)
  const [offset, setOffset] = useState(0)
  const [alertType, setAlertType] = useState<"all" | "threat" | "system">("all")
  const [unreadOnly, setUnreadOnly] = useState(false)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [marking, setMarking] = useState(false)

  const listRef = useRef<HTMLDivElement>(null)
  const atBottom = useRef(false)

  const fetchPage = useCallback(
    async (nextOffset: number, replace: boolean) => {
      const data = await api.getAlerts({
        alert_type: alertType,
        limit: PAGE_SIZE,
        offset: nextOffset,
        unread_only: unreadOnly,
      })
      setTotal(data.total)
      setOffset(nextOffset)
      setRows((prev) => (replace ? data.alerts : [...prev, ...data.alerts]))
      return data.alerts
    },
    [alertType, unreadOnly],
  )

  const load = useCallback(async () => {
    try {
      await fetchPage(0, true)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load alerts")
    } finally {
      setLoading(false)
    }
  }, [fetchPage])

  useAutoRefresh({ load, intervalMs: DEFAULT_REFRESH_MS })

  useEffect(() => {
    setLoading(true)
    setOffset(0)
    void load()
  }, [load])

  const loadMore = async () => {
    if (loadingMore || offset + rows.length >= total) return
    setLoadingMore(true)
    try {
      await fetchPage(offset + rows.length, false)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load more alerts")
    } finally {
      setLoadingMore(false)
    }
  }

  /**
   * Clear the badge for what the operator has actually seen.
   *
   * Scrolling to within `SEEN_MARGIN_PX` of the bottom means every row rendered
   * above it has passed through the viewport. Marking exactly those (rather
   * than all unread) keeps the badge meaningful while paging: alerts on a later
   * page are still unread until reached.
   */
  const markVisibleRead = useCallback(async () => {
    if (atBottom.current) return
    atBottom.current = true
    const unseen = rows.filter((r) => !r.read)
    if (unseen.length === 0) return
    setRows((prev) => prev.map((r) => ({ ...r, read: true })))
    try {
      await Promise.all(unseen.map((r) => api.markAlertRead(r.alert_id)))
    } catch {
      // A failed read-mark is not worth an error banner over the feed itself;
      // the next poll restores the true unread state.
      void load()
    } finally {
      atBottom.current = false
    }
  }, [rows, load])

  /**
   * When the whole page fits without scrolling there is no scroll event to
   * hang read-marking on, so the badge would sit at "7 unread" on a feed the
   * operator is plainly looking at. Detect that case and settle it.
   */
  useEffect(() => {
    if (loading || rows.length === 0) return
    const el = listRef.current
    if (!el) return
    if (el.scrollHeight > el.clientHeight + SEEN_MARGIN_PX) return
    void markVisibleRead()
  }, [loading, rows, markVisibleRead])

  const onScroll = useCallback(() => {
    const el = listRef.current
    if (!el) return
    const remaining = el.scrollHeight - el.scrollTop - el.clientHeight
    if (remaining <= SEEN_MARGIN_PX) {
      if (offset + rows.length < total) void loadMore()
      void markVisibleRead()
    }
  }, [loadMore, markVisibleRead, offset, rows.length, total])

  const markAll = async () => {
    setMarking(true)
    try {
      await api.markAllAlertsRead()
      setRows((prev) => prev.map((r) => ({ ...r, read: true })))
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not clear alerts")
    } finally {
      setMarking(false)
    }
  }

  const unreadCount = rows.filter((r) => !r.read).length
  const alertTabs: Array<"all" | "threat" | "system"> =
    session?.user.role === "admin" ? ["all", "threat", "system"] : ["all", "threat"]

  return (
    <div>
      <PageHeader
        title="Alerts"
        description="A single operational inbox for threat findings and system incidents. Threats stay scoped to your jobs; system incidents are visible to administrators."
        actions={
          <>
            <button
              type="button"
              onClick={() => setUnreadOnly((v) => !v)}
              className={cn(
                "cursor-pointer rounded-md border px-3 py-1.5 text-xs font-medium transition",
                unreadOnly
                  ? "border-indigo-500/60 bg-indigo-500/15 text-indigo-300"
                  : "border-slate-700 bg-slate-900/60 text-slate-300 hover:border-slate-500",
              )}
            >
              {unreadOnly ? "Unread only" : "All alerts"}
            </button>
            <button
              type="button"
              onClick={() => void markAll()}
              disabled={marking || unreadCount === 0}
              title="Clear the unread badge"
              className="inline-flex cursor-pointer items-center gap-1.5 rounded-md border border-slate-700 bg-slate-900/60 px-3 py-1.5 text-xs font-medium text-slate-300 transition hover:border-slate-500 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Mark all read
            </button>
          </>
        }
      />

      <div className="mb-4 flex flex-wrap items-center gap-1 rounded-lg border border-slate-800 bg-slate-950/70 p-1">
        {alertTabs.map((type) => (
          <button
            key={type}
            type="button"
            onClick={() => setAlertType(type)}
            className={cn(
              "cursor-pointer rounded-md px-3 py-1.5 text-xs font-semibold transition",
              alertType === type
                ? type === "system"
                  ? "bg-cyan-500/15 text-cyan-300"
                  : "bg-indigo-500/15 text-indigo-300"
                : "text-slate-500 hover:bg-white/5 hover:text-slate-300",
            )}
          >
            {type === "all" ? "All alerts" : type === "threat" ? "Threat alerts" : "System alerts"}
          </button>
        ))}
      </div>

      {error && <ErrorBanner message={error} onRetry={() => void load()} />}

      <div className="mb-3 flex items-center justify-between text-xs text-slate-500">
        <span>
          {loading ? "Loading alerts…" : `${rows.length} of ${total} alert${total === 1 ? "" : "s"}`}
        </span>
        {unreadCount > 0 && (
          <span className="inline-flex items-center gap-1 text-amber-400">
            <AlertTriangle className="h-3.5 w-3.5" />
            {unreadCount} unread in view
          </span>
        )}
      </div>

      <div
        ref={listRef}
        onScroll={onScroll}
        className="max-h-[70vh] space-y-2 overflow-y-auto pr-1"
      >
        {loading ? (
          <LoadingBlock label="Loading alerts" />
        ) : rows.length === 0 ? (
          <EmptyState
            icon={<AlertTriangle className="h-8 w-8" />}
            title={unreadOnly ? "No unread alerts" : alertType === "system" ? "No system alerts" : alertType === "threat" ? "No threat alerts" : "No alerts"}
            hint={alertType === "system" ? "System incidents such as failed or crashed jobs appear here for administrators." : "Threat alerts appear when the analysis pipeline identifies a high-severity finding."}
          />
        ) : (
          rows.map((a) => (
            <article
              key={a.alert_id}
              className={cn(
                "rounded-lg border p-3 transition",
                a.read
                  ? "border-slate-800 bg-slate-900/30 opacity-70"
                  : a.alert_type === "system"
                    ? "border-cyan-500/40 bg-cyan-950/20 ring-1 ring-cyan-500/10"
                    : "border-indigo-500/30 bg-slate-900/70 ring-1 ring-indigo-500/10",
                a.alert_id === focusId && "ring-2 ring-amber-400/60",
              )}
              id={`alert-${a.alert_id}`}
            >
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <SeverityPill severity={a.severity} priority={a.priority} />
                    <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-400">
                      {a.alert_type === "system" ? "System alert" : "Threat alert"}
                    </span>
                    <span className="text-[11px] font-medium uppercase tracking-wide text-slate-500">
                      {a.category.replace(/_/g, " ") || "uncategorised"}
                    </span>
                    {a.language && (
                      <span className="text-[11px] uppercase text-slate-600">{a.language}</span>
                    )}
                    {!a.read && (
                      <span className="rounded-full bg-indigo-500/20 px-1.5 py-0.5 text-[10px] font-semibold uppercase text-indigo-300">
                        new
                      </span>
                    )}
                  </div>
                  <h2 className="mt-1.5 text-sm font-semibold text-slate-100">
                    {a.title || a.url}
                  </h2>
                  <div className="mt-2 rounded-md border border-slate-800/80 bg-black/20 px-3 py-2">
                    <div className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">Summary</div>
                    <p className="mt-1 text-sm leading-relaxed text-slate-200">{a.short_summary}</p>
                  </div>
                </div>
                <span className="shrink-0 text-[11px] text-slate-600">{timeAgo(a.created_at)}</span>
              </div>

              {a.entities.length > 0 && (
                <div className="mt-2 flex flex-wrap gap-1">
                  {a.entities.slice(0, 12).map((e) => (
                    <span
                      key={e}
                      className="rounded bg-slate-800/80 px-1.5 py-0.5 text-[11px] text-slate-400"
                    >
                      {e}
                    </span>
                  ))}
                  {a.entities.length > 12 && (
                    <span className="text-[11px] text-slate-600">+{a.entities.length - 12} more</span>
                  )}
                </div>
              )}

              <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-slate-500">
                <a
                  href={a.url}
                  target="_blank"
                  rel="noreferrer noopener"
                  className="inline-flex items-center gap-1 text-slate-400 hover:text-indigo-400"
                >
                  <ExternalLink className="h-3 w-3" />
                  Source
                </a>
                {a.job_id && (
                  <>
                    <Link
                      to={`/jobs/${a.job_id}/articles`}
                      className="text-slate-400 hover:text-indigo-400"
                    >
                      Job articles
                    </Link>
                    <span className="font-mono text-slate-600">{a.job_id}</span>
                  </>
                )}
                {a.analysis_source !== "llm" && (
                  <span
                    className="rounded border border-amber-500/40 bg-amber-500/10 px-1.5 py-0.5 text-amber-400"
                    title="This label came from the rule-based fallback, not the model"
                  >
                    heuristic ({a.analysis_source})
                  </span>
                )}
              </div>
            </article>
          ))
        )}

        {!loading && offset + rows.length < total && (
          <div className="flex justify-center py-3">
            <button
              type="button"
              onClick={() => void loadMore()}
              disabled={loadingMore}
              className="cursor-pointer rounded-md border border-slate-700 bg-slate-900/60 px-4 py-1.5 text-xs font-medium text-slate-300 transition hover:border-slate-500 disabled:opacity-50"
            >
              {loadingMore ? "Loading…" : `Load ${Math.min(PAGE_SIZE, total - rows.length)} more`}
            </button>
          </div>
        )}
      </div>
    </div>
  )
}