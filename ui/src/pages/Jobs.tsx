import { useCallback, useEffect, useMemo, useState } from "react"
import { Link } from "react-router-dom"
import { ChevronRight, Loader2, Pause, Play, Waypoints } from "lucide-react"
import { api, getJobFeedSocketUrl } from "../api"
import { useAuth } from "../config"
import { useRealtimeSocket } from "../useRealtime"
import { JOB_STATUSES } from "../types"
import type { JobStatus, JobSummary, UserJobsResponse } from "../types"
import { humanize, relativeTime } from "../utils"
import { SelectFilter } from "../components/SelectFilter"
import { useAutoRefresh } from "../useAutoRefresh"
import { useConfirm } from "../components/ui"
import {
  EmptyState,
  ErrorBanner,
  LoadingBlock,
  Msg,
  PageHeader,
  StatusBadge,
} from "../components/ui"

type Filter = JobStatus | "all"
type SiteFilter = string // "all" or a site_name

export default function Jobs() {
  const { session } = useAuth()
  const isAdmin = session?.user.role === "admin"
  const [data, setData] = useState<UserJobsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [statusFilter, setStatusFilter] = useState<Filter>("all")
  const [siteFilter, setSiteFilter] = useState<SiteFilter>("all")
  const [busyJob, setBusyJob] = useState<string | null>(null)
  const [notice, setNotice] = useState<{ tone: "success" | "error"; text: string } | null>(
    null,
  )
  const { confirm, confirmDialog } = useConfirm()

  const load = useCallback(
    async () => {
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
        setError(e instanceof Error ? e.message : "Could not load your jobs.")
      } finally {
        setLoading(false)
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
        void load()
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
        void load()
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
    url: getJobFeedSocketUrl(),
    enabled: true,
    onEvent: handleFeedEvent,
    // Reconcile any changes missed while the socket was down.
    onOpen: () => void load(),
    onFallback: () => void load(),
    fallbackMs: 15000,
  })

  // The push socket only fires on status changes; a slow job can sit on one
  // row for minutes. Poll as well so counts and rows stay current, and skip the
  // tick while a background tab is asleep.
  useAutoRefresh({ load })

  const jobs = useMemo(() => {
    const all = data?.jobs ?? []
    return all
      .filter((j) => statusFilter === "all" || j.status === statusFilter)
      .filter((j) => siteFilter === "all" || (j.site_name ?? "unknown") === siteFilter)
  }, [data, statusFilter, siteFilter])

  /**
   * Sites present in the loaded jobs, busiest first.
   *
   * `site_name` is derived from the URL, so a search:// discovery job has no
   * host and falls back to "unknown" rather than inventing a site — those jobs
   * stay reachable under that label instead of disappearing from the list.
   */
  const sites = useMemo(() => {
    const counts = new Map<string, number>()
    for (const j of data?.jobs ?? []) {
      const site = j.site_name ?? "unknown"
      counts.set(site, (counts.get(site) ?? 0) + 1)
    }
    return [...counts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
  }, [data])

  const countForSite = (site: SiteFilter) =>
    site === "all" ? (data?.jobs ?? []).length : sites.find(([s]) => s === site)?.[1] ?? 0

  /** Pause or resume one job, after confirming what pausing actually does. */
  const togglePause = async (job: JobSummary) => {
    const isPaused = job.status === "paused"
    if (!isPaused) {
      const ok = await confirm({
        title: "Pause this job?",
        message:
          "Pages already being fetched will finish and are kept. After that the job stops, and resuming continues from where it left off - nothing is re-downloaded.",
        confirmLabel: "Pause job",
      })
      if (!ok) return
    }

    setBusyJob(job.job_id)
    try {
      const res = isPaused
        ? await api.resumeJob(job.job_id)
        : await api.pauseJob(job.job_id)
      setNotice({ tone: "success", text: res.message })
      await load()
    } catch (e) {
      setNotice({
        tone: "error",
        text: e instanceof Error ? e.message : "Could not change the job.",
      })
    } finally {
      setBusyJob(null)
    }
  }

  const countFor = (f: Filter) =>
    f === "all" ? (data?.total ?? 0) : (data?.jobs ?? []).filter((j) => j.status === f).length

  return (
    <div>
      <PageHeader
        title="Jobs"
        actions={
          <Link to="/new-crawl" className="btn-primary">
            <Waypoints className="h-4 w-4" />
            New crawl
          </Link>
        }
      />

      {/* Mounted once; the dialog itself only appears when opened. */}
      {confirmDialog}

      <div className="mb-5 flex flex-wrap items-center gap-2">
        <SelectFilter
          label="Status"
          value={statusFilter}
          onChange={setStatusFilter}
          className="w-48"
          allLabel="All statuses"
          options={(["all", ...JOB_STATUSES] as Filter[]).map((f) => ({
            value: f,
            label: f === "all" ? "All statuses" : f === "needs_review" ? "Needs review" : humanize(f),
            count: countFor(f),
          }))}
        />
        <SelectFilter
          label="Site"
          value={siteFilter}
          onChange={setSiteFilter}
          className="w-56"
          allLabel="All sites"
          options={[
            { value: "all", label: "All sites", count: countForSite("all") },
            ...sites.map(([site, count]) => ({ value: site, label: site, count })),
          ]}
        />
        <span className="text-[11px] text-slate-500">
          {connected ? "Live" : "Reconnecting"} · updates every 5s
        </span>
      </div>

      {notice && (
        <div className="mb-4">
          <Msg tone={notice.tone}>{notice.text}</Msg>
        </div>
      )}

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
            <div className="table-scroll">
              <table className="w-full min-w-[720px]">
                <thead className="border-b border-slate-800 bg-slate-950">
                  <tr>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                      Site
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
                      <td data-label="Site" className="px-5 py-3.5 whitespace-nowrap">
                        <div className="flex items-center gap-2">
                          <button
                            type="button"
                            onClick={() => setSiteFilter(job.site_name ?? "unknown")}
                            title={`Show only ${job.site_name ?? "unknown"} jobs`}
                            className="max-w-[180px] truncate font-medium text-slate-200 hover:text-indigo-300 hover:underline"
                          >
                            {job.site_name ?? "unknown"}
                          </button>
                          <Link
                            to={`/jobs/${job.job_id}`}
                            className="font-mono text-[11px] text-slate-600 hover:text-indigo-400 hover:underline"
                            title={job.job_id}
                          >
                            {job.job_id}
                          </Link>
                        </div>
                      </td>
                      <td data-label="URL" className="max-w-[340px] px-5 py-3.5">
                        {job.url.startsWith("search://") ? (
                          <div className="flex items-center gap-2">
                            <span className="shrink-0 rounded-full border border-sky-500/30 bg-sky-500/10 px-2 py-0.5 text-[10px] font-semibold tracking-wider text-sky-300 uppercase">
                              discovery
                            </span>
                            <span className="truncate font-mono text-xs text-slate-400" title={job.url}>
                              {job.url.slice("search://".length)}
                            </span>
                          </div>
                        ) : (
                          <p
                            className="truncate font-mono text-xs text-slate-400"
                            title={
                              job.assignment_reason
                                ? `${job.url} — routed: ${job.assignment_reason.replace(/_/g, " ")}`
                                : job.url
                            }
                          >
                            {job.url}
                          </p>
                        )}
                      </td>
                      <td data-label="Status" className="px-5 py-3.5">
                        <StatusBadge status={job.status} />
                      </td>
                      <td data-label="Submitted" className="px-5 py-3.5 text-xs whitespace-nowrap text-slate-500">
                        {relativeTime(job.created_at)}
                      </td>
                      <td className="px-5 py-3.5">
                        <div className="flex items-center justify-end gap-1">
                          {/* Only offer the control while it can do something. */}
                          {(job.status === "paused" ||
                            job.status === "running" ||
                            job.status === "pending") && (
                            <button
                              type="button"
                              onClick={() => void togglePause(job)}
                              disabled={busyJob === job.job_id}
                              title={
                                job.status === "paused"
                                  ? "Resume this job where it stopped"
                                  : "Pause this job"
                              }
                              aria-label={`${job.status === "paused" ? "Resume" : "Pause"} ${job.job_id}`}
                              className="inline-flex cursor-pointer items-center gap-1 rounded-md px-2 py-1 text-[11px] text-slate-400 transition hover:bg-slate-800 hover:text-slate-100 disabled:cursor-not-allowed disabled:opacity-50"
                            >
                              {busyJob === job.job_id ? (
                                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                              ) : job.status === "paused" ? (
                                <Play className="h-3.5 w-3.5" />
                              ) : (
                                <Pause className="h-3.5 w-3.5" />
                              )}
                              <span className="hidden lg:inline">
                                {job.status === "paused" ? "Resume" : "Pause"}
                              </span>
                            </button>
                          )}
                          <Link
                            to={`/jobs/${job.job_id}`}
                            aria-label={`Open ${job.job_id}`}
                            className="inline-flex rounded-md p-1 text-slate-500 transition hover:bg-slate-800 hover:text-indigo-400"
                          >
                            <ChevronRight className="h-4 w-4" />
                          </Link>
                        </div>
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
