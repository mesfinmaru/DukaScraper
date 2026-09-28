import { useCallback, useEffect, useMemo, useState } from "react"
import type { ReactNode } from "react"
import { Link } from "react-router-dom"
import {
  CheckCircle2,
  ChevronRight,
  CircleX,
  Clock3,
  Loader2,
  RefreshCw,
  Rocket,
  Search,
  ShieldAlert,
  Waypoints,
} from "lucide-react"
import { PieChart, Pie, Cell, Tooltip } from "recharts"
import { api } from "../api"
import { useAuth } from "../config"
import { JOB_STATUSES } from "../types"
import type { ThreatAnalyticsResponse, UserJobsResponse } from "../types"
import { cn, formatNumber, relativeTime } from "../utils"
import { EmptyState, ErrorBanner, LoadingBlock, PageHeader, StatusBadge } from "../components/ui"

const STATUS_COLORS: Record<string, string> = {
  pending: "#f59e0b",
  running: "#38bdf8",
  completed: "#34d399",
  failed: "#fb7185",
  skipped: "#94a3b8",
  needs_review: "#a78bfa",
}

function StatCard({
  label,
  value,
  icon,
  tone,
}: {
  label: string
  value: number
  icon: ReactNode
  tone: string
}) {
  return (
    <div className="card flex items-center gap-4 p-5">
      <div className={cn("flex h-10 w-10 shrink-0 items-center justify-center rounded-lg", tone)}>
        {icon}
      </div>
      <div className="min-w-0">
        <p className="text-2xl font-bold tracking-tight text-slate-100">{formatNumber(value)}</p>
        <p className="truncate text-xs font-medium tracking-wider text-gray-400 uppercase">
          {label}
        </p>
      </div>
    </div>
  )
}

function ThreatSeverityCard({ threats }: { threats: ThreatAnalyticsResponse | null }) {
  const count = (level: number) =>
    threats?.by_severity.find((s) => s.severity === level)?.count ?? 0
  const flagged = (count(4) ?? 0) + (count(5) ?? 0)
  const total = threats?.total ?? 0
  const share = total > 0 ? ((flagged / total) * 100).toFixed(1) : "0.0"
  const critical = flagged > 0 && count(4) === 0

  return (
    <div className="card flex flex-col p-5">
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-amber-500/15">
            <ShieldAlert className="h-5 w-5 text-amber-500" />
          </div>
          <div>
            <p className="text-sm font-semibold text-slate-200">Threat severity</p>
            <p className="text-[11px] tracking-wider text-slate-500 uppercase">
              High &amp; critical threats
            </p>
          </div>
        </div>
        <span
          className={cn(
            "rounded-full border px-2.5 py-0.5 text-[11px] font-semibold tracking-wider",
            critical
              ? "border-rose-500/40 bg-rose-500/10 text-rose-500"
              : "border-amber-500/40 bg-amber-500/10 text-amber-500",
          )}
        >
          {critical ? "CRITICAL" : "HIGH"}
        </span>
      </div>

      <div className="mt-5 flex items-baseline gap-3">
        <span className="text-4xl font-bold tracking-tight text-amber-500">
          {formatNumber(flagged)}
        </span>
        <span className="text-xs tracking-wider text-slate-500 uppercase">threats flagged</span>
      </div>

      <div className="mt-4 h-1.5 w-full overflow-hidden rounded-full bg-slate-800">
        <div
          className="h-full rounded-full bg-gradient-to-r from-amber-500 to-rose-500 transition-all duration-500"
          style={{ width: `${Math.min(100, Math.max(0, Number(share)))}%` }}
        />
      </div>

      <div className="mt-3 flex items-center justify-between">
        <p className="text-[11px] text-slate-500">{share}% of flagged threats</p>
        <Link
          to="/analytics"
          className="inline-flex items-center gap-0.5 text-xs font-medium text-amber-500 transition hover:text-amber-400"
        >
          Details
          <ChevronRight className="h-3.5 w-3.5" />
        </Link>
      </div>
    </div>
  )
}

function StatusDonut({ data }: { data: { name: string; value: number }[] }) {
  const total = data.reduce((sum, d) => sum + d.value, 0)
  if (!total) {
    return (
      <div className="flex h-48 items-center justify-center text-sm text-slate-600">
        No jobs yet
      </div>
    )
  }
  return (
    <div className="relative h-48">
      <div className="absolute inset-y-0 left-0 right-44 sm:right-52 flex items-center justify-center">
        <PieChart width={192} height={192}>
          <Pie
            data={data}
            dataKey="value"
            nameKey="name"
            cx="50%"
            cy="50%"
            innerRadius={55}
            outerRadius={80}
            paddingAngle={3}
            strokeWidth={0}
          >
            {data.map((d) => (
              <Cell key={d.name} fill={STATUS_COLORS[d.name] ?? "#64748b"} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{
              background: "#0a120c",
              border: "1px solid #1d3926",
              borderRadius: 8,
              fontSize: 12,
              color: "#90c699",
            }}
            itemStyle={{ color: "#d6eeda" }}
          />
        </PieChart>
      </div>
      <div className="pointer-events-none absolute inset-y-0 left-0 right-44 sm:right-52 flex flex-col items-center justify-center">
        <span className="text-2xl font-bold text-slate-100">{formatNumber(total)}</span>
        <span className="text-[11px] tracking-wider text-slate-500 uppercase">jobs</span>
      </div>
      <div className="absolute inset-y-0 right-0 w-44 sm:w-52 flex flex-col justify-center gap-1.5">
        {data.map((d) => (
          <span key={d.name} className="inline-flex items-center gap-1.5 text-xs text-slate-400">
            <span
              className="h-2 w-2 shrink-0 rounded-full"
              style={{ background: STATUS_COLORS[d.name] ?? "#64748b" }}
            />
            {d.name.replace("_", " ")} ({d.value})
          </span>
        ))}
      </div>
    </div>
  )
}

export default function Dashboard() {
  const { session } = useAuth()
  const isAdmin = session?.user.role === "admin"
  const [data, setData] = useState<UserJobsResponse | null>(null)
  const [threats, setThreats] = useState<ThreatAnalyticsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [refreshTick, setRefreshTick] = useState(0)

  const load = useCallback(async () => {
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
    }
  }, [session, isAdmin])

  const loadThreats = useCallback(async () => {
    try {
      setThreats(await api.getThreatAnalytics())
    } catch {
      setThreats(null)
    }
  }, [])

  useEffect(() => {
    setLoading(true)
    void load()
    void loadThreats()
  }, [load, loadThreats, refreshTick])

  const jobs = useMemo(() => data?.jobs ?? [], [data])
  const sorted = useMemo(
    () =>
      [...jobs].sort((a, b) => (b.created_at ?? "").localeCompare(a.created_at ?? "")),
    [jobs],
  )
  const counts = useMemo(() => {
    const map: Record<string, number> = {}
    for (const j of jobs) map[j.status] = (map[j.status] ?? 0) + 1
    return map
  }, [jobs])
  const donutData = JOB_STATUSES.filter((s) => counts[s]).map((s) => ({
    name: s,
    value: counts[s],
  }))
  const activeCount = (counts.pending ?? 0) + (counts.running ?? 0)

  return (
    <div>
      <PageHeader
        title="Dashboard"
        actions={
          <>
            <button
              type="button"
              className="btn-secondary"
              onClick={() => setRefreshTick((t) => t + 1)}
              disabled={loading}
            >
              {loading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4" />
              )}
              Refresh
            </button>
            <Link to="/new-crawl" className="btn-primary">
              <Rocket className="h-4 w-4" />
              New crawl
            </Link>
          </>
        }
      />

      {error && (
        <div className="mb-6">
          <ErrorBanner message={error} onRetry={() => void load()} />
        </div>
      )}

      {loading && !data ? (
        <LoadingBlock label="Loading your jobs..." />
      ) : (
        <div className="space-y-6">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <StatCard
              label="Total jobs"
              value={jobs.length}
              icon={<Waypoints className="h-5 w-5 text-sky-600" />}
              tone="bg-sky-500/15"
            />
            <StatCard
              label="Active (pending + running)"
              value={activeCount}
              icon={<Clock3 className="h-5 w-5 text-amber-500" />}
              tone="bg-amber-500/15"
            />
            <StatCard
              label="Completed"
              value={counts.completed ?? 0}
              icon={<CheckCircle2 className="h-5 w-5 text-emerald-600" />}
              tone="bg-emerald-500/15"
            />
            <StatCard
              label="Failed"
              value={counts.failed ?? 0}
              icon={<CircleX className="h-5 w-5 text-rose-500" />}
              tone="bg-rose-500/15"
            />
          </div>

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <div className="card p-5">
              <div className="mb-2 flex items-center justify-between">
                <h2 className="text-sm font-semibold text-slate-200">Jobs by status</h2>
                <Link to="/jobs" className="text-xs text-indigo-400 hover:text-indigo-300">
                  View all
                </Link>
              </div>
              <StatusDonut data={donutData} />
            </div>

            <ThreatSeverityCard threats={threats} />
          </div>

          <div className="card overflow-hidden">
            <div className="flex items-center justify-between px-5 py-4">
              <h2 className="text-sm font-semibold text-slate-200">Recent jobs</h2>
            </div>
            {sorted.length === 0 ? (
              <EmptyState
                title="No jobs submitted yet"
                hint="Trigger your first crawl from the New Crawl page and it will appear here."
                icon={<Search className="h-8 w-8" />}
              />
            ) : (
              <ul className="divide-y divide-slate-800/70 border-t border-slate-800/70">
                {sorted.slice(0, 5).map((job) => (
                  <li key={job.job_id}>
                    <Link
                      to={`/jobs/${encodeURIComponent(job.job_id)}`}
                      className="flex items-center gap-4 px-5 py-3 transition hover:bg-slate-800/40"
                    >
                      <span className="font-mono text-xs text-sky-600">{job.job_id}</span>
                      <span className="min-w-0 flex-1 truncate text-sm text-slate-300">
                        {job.url}
                      </span>
                      <span className="hidden text-xs text-slate-500 sm:block">
                        {relativeTime(job.created_at)}
                      </span>
                      <StatusBadge status={job.status} />
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
