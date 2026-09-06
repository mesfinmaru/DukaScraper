import { useCallback, useEffect, useRef, useState } from "react"
import { Link, useParams } from "react-router-dom"
import {
  ArrowLeft,
  Check,
  ChevronRight,
  CircleSlash,
  CircleX,
  Clock3,
  Eye,
  FileText,
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
  CopyButton,
  EmptyState,
  ErrorBanner,
  InfoRow,
  LoadingBlock,
  StatusBadge,
} from "../components/ui"

const TERMINAL_STATUSES = new Set(["completed", "failed", "skipped"])

function ProgressStepper({ job }: { job: JobDetailType }) {
  const failed = job.status === "failed"
  const skipped = job.status === "skipped"
  const review = job.status === "needs_review"
  const done = job.status === "completed"
  const stage = TERMINAL_STATUSES.has(job.status) || review ? 2 : job.status === "running" ? 1 : 0

  const nodes = [
    { label: "Queued", icon: Clock3 },
    { label: "Crawling & parsing", icon: Loader2 },
    failed
      ? { label: "Failed", icon: CircleX }
      : skipped
        ? { label: "Skipped", icon: CircleSlash }
        : review
          ? { label: "Needs review", icon: Eye }
          : { label: "Completed", icon: Check },
  ]

  return (
    <div className="flex items-center">
      {nodes.map((node, i) => (
        <div key={node.label} className="flex items-center">
          <div className="flex flex-col items-center gap-1.5">
            <div
              className={cn(
                "flex h-8 w-8 items-center justify-center rounded-full border",
                i < stage || (i === 2 && (done || failed))
                  ? done
                    ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-600"
                    : failed
                      ? "border-rose-500/40 bg-rose-500/10 text-rose-500"
                      : "border-sky-500/40 bg-sky-500/10 text-sky-600"
                  : i === stage
                    ? "border-indigo-500/40 bg-indigo-500/10 text-indigo-300"
                    : "border-slate-800 bg-slate-900 text-slate-600",
              )}
            >
              <node.icon
                className={cn("h-4 w-4", i === 1 && i === stage && "animate-spin")}
              />
            </div>
            <span
              className={cn(
                "text-[11px] font-medium whitespace-nowrap",
                i <= stage && !(i === 2 && !done && !failed) ? "text-slate-300" : "text-slate-600",
              )}
            >
              {node.label}
            </span>
          </div>
          {i < nodes.length - 1 && (
            <div
              className={cn(
                "mx-2 mb-5 h-px w-16 sm:w-28",
                i < stage ? "bg-sky-500/50" : "bg-slate-800",
              )}
            />
          )}
        </div>
      ))}
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
              <InfoRow label="Raw HTML path">
                <div className="flex items-center gap-2">
                  <code className="block max-w-xs truncate rounded bg-slate-900 px-2 py-1 font-mono text-[11px] text-slate-400">
                    {item.raw_html_path}
                  </code>
                  <CopyButton value={item.raw_html_path} label="" />
                </div>
              </InfoRow>
              <InfoRow label="Parsed JSON path">
                <div className="flex items-center gap-2">
                  <code className="block max-w-xs truncate rounded bg-slate-900 px-2 py-1 font-mono text-[11px] text-slate-400">
                    {item.parsed_json_path}
                  </code>
                  <CopyButton value={item.parsed_json_path} label="" />
                </div>
              </InfoRow>
              <InfoRow label="Item ID">
                <div className="flex items-center gap-2">
                  <span className="font-mono text-xs text-slate-300">{item.item_id}</span>
                  <CopyButton value={item.item_id} label="" />
                </div>
              </InfoRow>
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
    if (!active) return
    const timer = window.setInterval(() => {
      if (document.visibilityState !== "visible") return
      void load()
    }, 5000)
    return () => window.clearInterval(timer)
  }, [job, load])

  const { connected } = useRealtimeSocket({
    url: jobId ? getJobStatusSocketUrl(jobId) : null,
    enabled: true,
    onEvent: (payload) => {
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
        {!TERMINAL_STATUSES.has(job.status) && !["needs_review"].includes(job.status) && (
          <span className="inline-flex items-center gap-2 text-xs text-slate-500">
            <Loader2 className="h-3.5 w-3.5 animate-spin text-sky-400" />
            {connected ? "Live - updates pushed over WebSocket" : "Live - reconnecting…"}
          </span>
        )}
      </div>

      <div className="mb-6 grid grid-cols-1 gap-6 lg:grid-cols-3">
        <div className="card p-6 lg:col-span-2">
          <ProgressStepper job={job} />
          {job.failure_reason && (
            <div className="mt-4 rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3">
              <p className="text-xs font-semibold text-rose-300">Failure reason</p>
              <p className="mt-1 break-words text-sm text-rose-200">{job.failure_reason}</p>
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
            <span className="text-xs text-slate-500">
              Click a row for storage paths & metadata
            </span>
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
