import { useCallback, useEffect, useMemo, useState } from "react"
import { Link } from "react-router-dom"
import { ChevronRight, Loader2, Pause, Play, RefreshCw, Waypoints } from "lucide-react"
import { api, getJobFeedSocketUrl } from "../api"
import { useAuth } from "../config"
import { useRealtimeSocket } from "../useRealtime"
import { JOB_STATUSES } from "../types"
import type { JobStatus, UserJobsResponse } from "../types"
import { cn, relativeTime } from "../utils"
import {
  EmptyState,
  ErrorBanner,
  LoadingBlock,
  PageHeader,
  StatusBadge,
} from "../components/ui"

type Filter = JobStatus | "all"

export default function Jobs() {
  const { session } = useAuth()
  const isAdmin = session?.user.role === "admin"
  const [data, setData] = useState<UserJobsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [statusFilter, setStatusFilter] = useState<Filter>("all")
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [refreshing, setRefreshing] = useState(false)

  const load = useCallback(
    async (silent = false) => {
      if (!silent) setRefreshing(true)
      try {
        if (!session) {
          setData(null)
          setError(null)
          return
        }
        const res = isAdmin ? await api.getAllJobs() : await api.getUserJobs(session.user.user_id)
        setData(res)
        setError(null)
      } catch (e) {
        setError(e instanceof Error ? e.message : "Failed to load jobs")
      } finally {
        setLoading(false)
        if (!silent) setRefreshing(false)
      }
    },
    [session, isAdmin],
  )

  // Initial snapshot is fetched via REST; afterwards the page subscribes to the
  // job feed WebSocket and refreshes on push events instead of polling.
  useEffect(() => {
    setLoading(true)
    void load()
  }, [load])

  const handleFeedEvent = useCallback(
    (payload: Record<string, unknown>) => {
      const event = payload.event
      if (event !== "job_status" && event !== "job_created") return
      const jobId = typeof payload.job_id === "string" ? payload.job_id : null
      if (!jobId) return

      // Newly created jobs are not in the snapshot yet — refetch the list.
      if (event === "job_created") {
        void load(true)
        return
      }

      // Status change: patch in place when the row is visible.
      const visible = (data?.jobs ?? []).some((j) => j.job_id === jobId)
      const status =
        typeof payload.status === "string" &&
        JOB_STATUSES.includes(payload.status as JobStatus)
          ? (payload.status as JobStatus)
          : null
      if (!visible) {
        void load(true)
        return
      }
      if (!status) return
      setData((prev) =>
        prev
          ? {
              ...prev,
              jobs: prev.jobs.map((j) =>
                j.job_id === jobId ? { ...j, status } : j,
              ),
            }
          : prev,
      )
    },
    [data, load],
  )

  const { connected } = useRealtimeSocket({
    url: autoRefresh ? getJobFeedSocketUrl() : null,
    enabled: autoRefresh,
    onEvent: handleFeedEvent,
    // Reconcile any changes missed while the socket was down.
    onOpen: () => void load(true),
    onFallback: () => void load(true),
    fallbackMs: 15000,
  })

  const jobs = useMemo(() => {
    const all = data?.jobs ?? []
    return statusFilter === "all" ? all : all.filter((j) => j.status === statusFilter)
  }, [data, statusFilter])

  const countFor = (f: Filter) =>
    f === "all" ? (data?.total ?? 0) : (data?.jobs ?? []).filter((j) => j.status === f).length

  return (
    <div>
      <PageHeader
        title="Jobs"
        actions={
          <>
            <button
              type="button"
              onClick={() => setAutoRefresh((v) => !v)}
              title={
                autoRefresh
                  ? connected
                    ? "Pause live updates"
                    : "Live updates enabled (reconnecting…)"
                  : "Resume live updates"
              }
              className="btn-secondary px-3 py-2"
            >
              {autoRefresh ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
              <span className="hidden sm:inline">{autoRefresh ? "Pause" : "Resume"}</span>
            </button>
            <button
              type="button"
              onClick={() => void load()}
              disabled={refreshing}
              title="Refresh now"
              className="btn-secondary px-3 py-2"
            >
              {refreshing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
              <span className="hidden sm:inline">{refreshing ? "Refreshing" : "Refresh"}</span>
            </button>
            <Link to="/new-crawl" className="btn-primary">
              <Waypoints className="h-4 w-4" />
              New crawl
            </Link>
          </>
        }
      />

      <div className="mb-5 flex flex-wrap gap-1.5">
        {(["all", ...JOB_STATUSES] as Filter[]).map((f) => (
          <button key={f} type="button" onClick={() => setStatusFilter(f)}>
            <span
              className={cn(
                "inline-flex cursor-pointer items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs font-medium capitalize transition",
                statusFilter === f
                  ? "border-indigo-600 bg-indigo-600 text-white"
                  : "border-slate-700 bg-black text-slate-300 hover:border-indigo-500 hover:text-indigo-300",
              )}
            >
              {f === "needs_review" ? "needs review" : f}
              <span
                className={cn(
                  "rounded-full px-1.5 py-px text-[10px] font-semibold",
                  statusFilter === f ? "bg-white/20 text-white" : "bg-slate-800 text-slate-400",
                )}
              >
                {countFor(f)}
              </span>
            </span>
          </button>
        ))}
      </div>

      {error && (
        <div className="mb-6">
          <ErrorBanner message={error} onRetry={() => void load()} />
        </div>
      )}

      {loading && !data ? (
        <LoadingBlock label="Loading your jobs..." />
      ) : (
        <div className="card overflow-hidden">
          {jobs.length === 0 ? (
            <EmptyState
              icon={<Waypoints className="h-8 w-8" />}
              title={statusFilter === "all" ? "No jobs yet" : `No ${statusFilter.replace("_", " ")} jobs`}
              hint={
                statusFilter === "all"
                  ? "Submit your first crawl from the New Crawl page and it will appear here instantly."
                  : "Try a different status filter above."
              }
            />
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[720px]">
                <thead className="border-b border-slate-800 bg-slate-950">
                  <tr>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Job
                    </th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      URL
                    </th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Status
                    </th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Submitted
                    </th>
                    <th className="w-12 px-5 py-3" />
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/70">
                  {jobs.map((job) => (
                    <tr key={job.job_id} className="transition hover:bg-slate-800/40">
                      <td className="px-5 py-3.5 whitespace-nowrap">
                        <Link
                          to={`/jobs/${job.job_id}`}
                          className="font-mono text-xs font-medium text-indigo-400 hover:text-indigo-300 hover:underline"
                        >
                          {job.job_id}
                        </Link>
                      </td>
                      <td className="max-w-[340px] px-5 py-3.5">
                        <p className="truncate font-mono text-xs text-slate-400" title={job.url}>
                          {job.url}
                        </p>
                      </td>
                      <td className="px-5 py-3.5">
                        <StatusBadge status={job.status} />
                      </td>
                      <td className="px-5 py-3.5 text-xs whitespace-nowrap text-slate-500">
                        {relativeTime(job.created_at)}
                      </td>
                      <td className="px-5 py-3.5 text-right">
                        <Link
                          to={`/jobs/${job.job_id}`}
                          aria-label={`Open ${job.job_id}`}
                          className="inline-flex rounded-md p-1 text-slate-500 transition hover:bg-slate-800 hover:text-indigo-400"
                        >
                          <ChevronRight className="h-4 w-4" />
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
