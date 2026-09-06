import { useCallback, useEffect, useState } from "react"
import type { FormEvent } from "react"
import { useNavigate } from "react-router-dom"
import {
  Activity,
  BarChart3,
  CheckCircle2,
  ClipboardCheck,
  Loader2,
  RefreshCw,
  ShieldAlert,
} from "lucide-react"
import { api, ApiError } from "../api"
import { useAuth } from "../config"
import {
  CONTENT_TOPICS,
  INTELLIGENCE_CATEGORIES,
  SOURCE_TYPES,
} from "../types"
import type {
  EvaluationResponse,
  MetricsResponse,
  PerformanceResponse,
  ThreatAnalyticsResponse,
  ThreatRecentRow,
} from "../types"
import { cn, formatNumber } from "../utils"
import { CopyButton, ErrorBanner, LoadingBlock, PageHeader, WorkerBadge } from "../components/ui"

const DIMENSIONS = [
  { key: "source_type", title: "Source type", tone: "bg-sky-500" },
  { key: "topic", title: "Topic", tone: "bg-emerald-500" },
  { key: "category", title: "Category", tone: "bg-violet-500" },
] as const

const SEVERITY_STYLES: Record<number, string> = {
  1: "border border-emerald-500/40 bg-emerald-500/10 text-emerald-600",
  2: "border border-lime-500/40 bg-lime-500/10 text-lime-500",
  3: "border border-amber-500/40 bg-amber-500/10 text-amber-500",
  4: "border border-orange-500/40 bg-orange-500/10 text-orange-500",
  5: "border border-rose-500/40 bg-rose-500/10 text-rose-500",
}

function pct(v: number | null | undefined): string {
  return v === null || v === undefined ? "--" : `${(v * 100).toFixed(1)}%`
}

function formatBytes(bytes: number | null): string {
  if (bytes === null || Number.isNaN(bytes)) return "--"
  if (bytes < 1024) return `${Math.round(bytes)} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`
}

function MetricBar({ value, tone }: { value: number | null; tone: string }) {
  const width = value === null ? 0 : Math.min(100, Math.max(0, value * 100))
  return (
    <div className="h-1.5 w-full overflow-hidden rounded-full bg-slate-800">
      <div
        className={cn("h-full rounded-full transition-all duration-500", tone)}
        style={{ width: `${width}%` }}
      />
    </div>
  )
}

function SeverityBadge({ severity }: { severity: number }) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium whitespace-nowrap",
        SEVERITY_STYLES[severity] ?? "border border-slate-600 bg-slate-800/60 text-slate-300",
      )}
    >
      S{severity}
    </span>
  )
}

export default function Analytics() {
  const { session } = useAuth()
  const navigate = useNavigate()

  const [metrics, setMetrics] = useState<MetricsResponse | null>(null)
  const [metricsLoading, setMetricsLoading] = useState(true)
  const [metricsError, setMetricsError] = useState<string | null>(null)

  const [threats, setThreats] = useState<ThreatAnalyticsResponse | null>(null)
  const [threatsLoading, setThreatsLoading] = useState(true)
  const [threatsError, setThreatsError] = useState<string | null>(null)

  const [performance, setPerformance] = useState<PerformanceResponse | null>(null)
  const [perfLoading, setPerfLoading] = useState(true)
  const [perfError, setPerfError] = useState<string | null>(null)

  const [itemId, setItemId] = useState("")
  const [jobId, setJobId] = useState("")
  const [evaluatedBy, setEvaluatedBy] = useState(session?.user.username ?? "")
  const [sourceType, setSourceType] = useState("")
  const [topic, setTopic] = useState("")
  const [category, setCategory] = useState("")
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const [success, setSuccess] = useState<EvaluationResponse | null>(null)

  const loadMetrics = useCallback(async () => {
    setMetricsLoading(true)
    try {
      setMetrics(await api.getMetrics())
      setMetricsError(null)
    } catch (e) {
      if (e instanceof ApiError && e.status === 503) {
        setMetricsError("Analytics database unavailable - is ClickHouse running?")
      } else {
        setMetricsError(e instanceof Error ? e.message : "Failed to load metrics")
      }
      setMetrics(null)
    } finally {
      setMetricsLoading(false)
    }
  }, [])

  const loadThreats = useCallback(async () => {
    setThreatsLoading(true)
    try {
      setThreats(await api.getThreatAnalytics())
      setThreatsError(null)
    } catch (e) {
      setThreatsError(
        e instanceof ApiError && e.status === 503
          ? "Analytics database unavailable - is ClickHouse running?"
          : e instanceof Error
            ? e.message
            : "Failed to load threat analytics",
      )
      setThreats(null)
    } finally {
      setThreatsLoading(false)
    }
  }, [])

  const loadPerformance = useCallback(async () => {
    setPerfLoading(true)
    try {
      setPerformance(await api.getPerformance())
      setPerfError(null)
    } catch (e) {
      setPerfError(
        e instanceof ApiError && e.status === 503
          ? "Analytics database unavailable - is ClickHouse running?"
          : e instanceof Error
            ? e.message
            : "Failed to load system performance",
      )
      setPerformance(null)
    } finally {
      setPerfLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadMetrics()
    void loadThreats()
    void loadPerformance()
  }, [loadMetrics, loadThreats, loadPerformance])

  useEffect(() => {
    setEvaluatedBy(session?.user.username ?? "")
  }, [session?.user.username])

  const submitEvaluation = async (e: FormEvent) => {
    e.preventDefault()
    setFormError(null)
    setSuccess(null)

    if (!itemId.trim() || !jobId.trim() || !evaluatedBy.trim()) {
      setFormError("Item ID, Job ID and evaluator name are required.")
      return
    }
    if (!sourceType || !topic || !category) {
      setFormError("Please select expected source type, topic and category.")
      return
    }

    setSubmitting(true)
    try {
      const res = await api.submitEvaluation({
        item_id: itemId.trim(),
        job_id: jobId.trim(),
        evaluated_by: evaluatedBy.trim(),
        expected_source_type: sourceType,
        expected_topic: topic,
        expected_category: category,
      })
      setSuccess(res)
      void loadMetrics()
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) {
        setFormError(
          "No model analysis found for this item/job pair. The llm-worker must analyze it first.",
        )
      } else if (err instanceof ApiError && err.status === 503) {
        setFormError("Analytics database unavailable - is ClickHouse running?")
      } else {
        setFormError(err instanceof Error ? err.message : "Failed to record evaluation")
      }
    } finally {
      setSubmitting(false)
    }
  }

  const refreshAll = () => {
    void loadMetrics()
    void loadThreats()
    void loadPerformance()
  }

  const fillEvaluation = (row: ThreatRecentRow) => {
    setItemId(row.item_id)
    setJobId(row.job_id)
    setSourceType(row.source_type)
    setTopic("other")
    setCategory(row.category)
    setFormError(null)
    setSuccess(null)
    document
      .getElementById("human-evaluation")
      ?.scrollIntoView({ behavior: "smooth", block: "start" })
  }

  const maxSeverityCount = threats
    ? Math.max(1, ...threats.by_severity.map((s) => s.count))
    : 1

  return (
    <div>
      <PageHeader
        title="Analytics & evaluation"
        actions={
          <button type="button" className="btn-secondary" onClick={refreshAll}>
            <RefreshCw className="h-4 w-4" />
            Refresh all
          </button>
        }
      />

      {metricsError && (
        <div className="mb-6">
          <ErrorBanner message={metricsError} onRetry={() => void loadMetrics()} />
        </div>
      )}

      {metricsLoading && !metrics ? (
        <LoadingBlock label="Loading evaluation metrics..." />
      ) : metrics ? (
        <>
          <div className="mb-6 grid grid-cols-1 gap-4 md:grid-cols-3">
            {DIMENSIONS.map((dim) => {
              const m = metrics[dim.key]
              return (
                <div key={dim.key} className="card p-5">
                  <div className="mb-3 flex items-center justify-between">
                    <h3 className="text-sm font-semibold text-slate-200">{dim.title}</h3>
                    <span className="rounded-full border border-slate-700 bg-slate-900 px-2 py-0.5 text-[11px] text-slate-400">
                      {formatNumber(m.samples)} sample{m.samples === 1 ? "" : "s"}
                    </span>
                  </div>
                  <div className="mb-1 flex items-baseline gap-3">
                    <span className="text-3xl font-bold tracking-tight text-slate-50">
                      {pct(m.accuracy)}
                    </span>
                    <span className="text-[11px] tracking-wider text-slate-500 uppercase">
                      accuracy
                    </span>
                  </div>
                  <MetricBar value={m.accuracy} tone={dim.tone} />
                  <div className="mt-4 mb-1 flex items-baseline gap-3">
                    <span className="text-lg font-semibold text-slate-200">
                      {m.macro_f1 === null ? "--" : m.macro_f1.toFixed(4)}
                    </span>
                    <span className="text-[11px] tracking-wider text-slate-500 uppercase">
                      macro F1
                    </span>
                  </div>
                  <MetricBar value={m.macro_f1} tone={dim.tone} />
                </div>
              );
            })}
          </div>
          <p className="mb-8 text-xs text-slate-600">{metrics.note}</p>
        </>
      ) : null}

      {/* ---------------- Threat intelligence ---------------- */}
      <section className="mb-10">
        <h2 className="mb-1 flex items-center gap-2 text-base font-semibold text-slate-100">
          <ShieldAlert className="h-5 w-5 text-rose-400" />
          Threat analytics
        </h2>
        <p className="mb-4 text-xs leading-relaxed text-slate-500">
          Aggregated threat classifications produced by the LLM worker into ClickHouse{" "}
          <code className="font-mono">intelligence_analytics</code> - severity is rated 1 (low) to 5
          (critical).
        </p>

        {threatsError && (
          <div className="mb-4">
            <ErrorBanner message={threatsError} onRetry={() => void loadThreats()} />
          </div>
        )}

        {threatsLoading && !threats ? (
          <LoadingBlock label="Loading threat analytics..." />
        ) : threats ? (
          threats.total === 0 ? (
            <div className="card p-8 text-center text-sm text-slate-500">
              No classified items yet - run crawls and let the llm-worker analyze them.
            </div>
          ) : (
            <>
              <div className="mb-6 grid grid-cols-1 gap-4 lg:grid-cols-3">
                <div className="card p-5">
                  <p className="text-[11px] tracking-wider text-slate-500 uppercase">
                    Total classified items
                  </p>
                  <p className="mt-2 text-3xl font-bold tracking-tight text-slate-50">
                    {formatNumber(threats.total)}
                  </p>
                </div>
                <div className="card p-5 lg:col-span-2">
                  <p className="mb-3 text-[11px] tracking-wider text-slate-500 uppercase">
                    By severity
                  </p>
                  <div className="space-y-2">
                    {[1, 2, 3, 4, 5].map((level) => {
                      const count =
                        threats.by_severity.find((s) => s.severity === level)?.count ?? 0
                      return (
                        <div key={level} className="flex items-center gap-3">
                          <SeverityBadge severity={level} />
                          <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-slate-800">
                            <div
                              className={cn(
                                "h-full rounded-full bg-gradient-to-r",
                                level <= 2
                                  ? "from-emerald-500 to-emerald-400"
                                  : level === 3
                                    ? "from-amber-500 to-amber-400"
                                    : "from-rose-500 to-rose-400",
                              )}
                              style={{ width: `${(count / maxSeverityCount) * 100}%` }}
                            />
                          </div>
                          <span className="w-12 text-right text-xs tabular-nums text-slate-300">
                            {formatNumber(count)}
                          </span>
                        </div>
                      )
                    })}
                  </div>
                </div>
              </div>

              <div className="card overflow-x-auto p-0">
                <table className="w-full min-w-[720px] text-left text-sm">
                  <thead>
                    <tr className="border-b border-slate-800 text-[11px] tracking-wider text-slate-500 uppercase">
                      <th className="px-4 py-3 font-medium">Category</th>
                      <th className="px-4 py-3 font-medium">Items</th>
                    </tr>
                  </thead>
                  <tbody>
                    {threats.by_category.map((c) => (
                      <tr key={c.category} className="border-b border-slate-800/60 last:border-0">
                        <td className="px-4 py-2.5 capitalize text-slate-200">
                          {c.category.replace(/_/g, " ")}
                        </td>
                        <td className="px-4 py-2.5 tabular-nums text-slate-300">
                          {formatNumber(c.count)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="card mt-6 overflow-x-auto p-0">
                <div className="flex items-center justify-between gap-2 border-b border-slate-800 px-4 py-3">
                  <span className="text-sm font-semibold text-slate-200">Recent flagged content</span>
                  <span className="text-[11px] text-slate-500">
                    Click a row to view parsed content, or Evaluate to fill the form below
                  </span>
                </div>
                <table className="w-full min-w-[900px] text-left text-sm">
                  <thead>
                    <tr className="border-b border-slate-800 text-[11px] tracking-wider text-slate-500 uppercase">
                      <th className="px-4 py-3 font-medium">Item</th>
                      <th className="px-4 py-3 font-medium">Category</th>
                      <th className="px-4 py-3 font-medium">Severity</th>
                      <th className="px-4 py-3 font-medium">Source</th>
                      <th className="px-4 py-3 font-medium">URL</th>
                      <th className="px-4 py-3 font-medium">Summary</th>
                      <th className="px-4 py-3 font-medium" />
                    </tr>
                  </thead>
                  <tbody>
                    {threats.recent.map((row) => (
                      <tr
                        key={`${row.job_id}-${row.item_id}`}
                        onClick={() => navigate(`/jobs/${row.job_id}/articles?itemId=${encodeURIComponent(row.item_id)}`)}
                        title="View parsed content in storage"
                        className="cursor-pointer border-b border-slate-800/60 last:border-0 hover:bg-slate-900/40"
                      >
                        <td className="px-4 py-2.5">
                          <span className="font-mono text-xs text-sky-600">{row.item_id}</span>
                        </td>
                        <td className="px-4 py-2.5 capitalize text-slate-200">
                          {row.category.replace(/_/g, " ")}
                        </td>
                        <td className="px-4 py-2.5">
                          <SeverityBadge severity={row.severity} />
                        </td>
                        <td className="px-4 py-2.5 capitalize text-slate-300">{row.source_type}</td>
                        <td className="max-w-56 truncate px-4 py-2.5 font-mono text-xs text-slate-400" title={row.url}>
                          {row.url}
                        </td>
                        <td className="max-w-72 px-4 py-2.5 text-xs text-slate-300" title={row.summary}>
                          {row.summary.length > 120 ? `${row.summary.slice(0, 120)}...` : row.summary}
                        </td>
                        <td className="px-4 py-2.5 text-right">
                          <button
                            type="button"
                            onClick={(e) => {
                              e.stopPropagation()
                              fillEvaluation(row)
                            }}
                            className="inline-flex cursor-pointer items-center gap-1 rounded-md border border-sky-500/40 bg-sky-500/10 px-2 py-1 text-[11px] font-medium text-sky-400 transition hover:bg-sky-500/20"
                          >
                            <ClipboardCheck className="h-3 w-3" />
                            Evaluate
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )
        ) : null}
      </section>

      {/* ---------------- System performance / latency ---------------- */}
      <section className="mb-10">
        <h2 className="mb-1 flex items-center gap-2 text-base font-semibold text-slate-100">
          <Activity className="h-5 w-5 text-emerald-400" />
          System performance
        </h2>
        <p className="mb-4 text-xs leading-relaxed text-slate-500">
          Crawler fetch latency and reliability per worker from ClickHouse{" "}
          <code className="font-mono">crawler_performance</code>.
        </p>

        {perfError && (
          <div className="mb-4">
            <ErrorBanner message={perfError} onRetry={() => void loadPerformance()} />
          </div>
        )}

        {perfLoading && !performance ? (
          <LoadingBlock label="Loading system performance..." />
        ) : performance ? (
          performance.overall.requests === 0 ? (
            <div className="card p-8 text-center text-sm text-slate-500">
              No crawl attempts recorded yet - latency metrics appear after workers start fetching.
            </div>
          ) : (
            <>
              <div className="mb-6 grid grid-cols-2 gap-4 lg:grid-cols-4">
                <div className="card p-5">
                  <p className="text-[11px] tracking-wider text-slate-500 uppercase">Requests</p>
                  <p className="mt-2 text-3xl font-bold tracking-tight text-slate-50">
                    {formatNumber(performance.overall.requests)}
                  </p>
                </div>
                <div className="card p-5">
                  <p className="text-[11px] tracking-wider text-slate-500 uppercase">Avg latency</p>
                  <p className="mt-2 text-3xl font-bold tracking-tight text-slate-50">
                    {performance.overall.avg_latency_ms === null
                      ? "--"
                      : `${formatNumber(performance.overall.avg_latency_ms)} ms`}
                  </p>
                </div>
                <div className="card p-5">
                  <p className="text-[11px] tracking-wider text-slate-500 uppercase">P95 latency</p>
                  <p className="mt-2 text-3xl font-bold tracking-tight text-slate-50">
                    {performance.overall.p95_latency_ms === null
                      ? "--"
                      : `${formatNumber(performance.overall.p95_latency_ms)} ms`}
                  </p>
                </div>
                <div className="card p-5">
                  <p className="text-[11px] tracking-wider text-slate-500 uppercase">Error rate</p>
                  <p className="mt-2 text-3xl font-bold tracking-tight text-slate-50">
                    {pct(performance.overall.error_rate)}
                  </p>
                </div>
              </div>

              <div className="rounded-xl border border-slate-800 bg-[#050b08] overflow-x-auto p-0">
                <div className="border-b border-slate-800 px-4 py-3 text-sm font-semibold text-slate-200">
                  Latency per worker
                </div>
                <table className="w-full min-w-[760px] text-left text-sm">
                  <thead>
                    <tr className="border-b border-slate-800 text-[11px] tracking-wider text-slate-500 uppercase">
                      <th className="px-4 py-3 font-medium">Worker</th>
                      <th className="px-4 py-3 font-medium">Requests</th>
                      <th className="px-4 py-3 font-medium">Avg latency</th>
                      <th className="px-4 py-3 font-medium">P95</th>
                      <th className="px-4 py-3 font-medium">Max</th>
                      <th className="px-4 py-3 font-medium">Retries</th>
                      <th className="px-4 py-3 font-medium">Error rate</th>
                      <th className="px-4 py-3 font-medium">Avg payload</th>
                    </tr>
                  </thead>
                  <tbody>
                    {performance.workers.map((w) => {
                      const isDark = w.worker.toLowerCase() === "dark";
                      return (
                        <tr
                          key={w.worker}
                          className={cn(
                            "border-b border-slate-800/60 last:border-0 hover:bg-slate-800/40",
                            isDark && "ring-2 ring-zinc-500/30 bg-zinc-900/40",
                          )}
                        >
                          <td className="px-4 py-2.5">
                            <WorkerBadge worker={w.worker} />
                          </td>
                          <td className="px-4 py-2.5 tabular-nums text-slate-400">{formatNumber(w.requests)}</td>
                          <td className="px-4 py-2.5 tabular-nums text-slate-100">
                            {w.avg_latency_ms === null ? "--" : `${formatNumber(w.avg_latency_ms)} ms`}
                          </td>
                          <td className="px-4 py-2.5 tabular-nums text-slate-400">
                            {w.p95_latency_ms === null ? "--" : `${formatNumber(w.p95_latency_ms)} ms`}
                          </td>
                          <td className="px-4 py-2.5 tabular-nums text-slate-400">
                            {w.max_latency_ms === null ? "--" : `${formatNumber(w.max_latency_ms)} ms`}
                          </td>
                          <td className="px-4 py-2.5 tabular-nums text-slate-400">{formatNumber(w.retries)}</td>
                          <td className="px-4 py-2.5 tabular-nums text-slate-400">{pct(w.error_rate)}</td>
                          <td className="px-4 py-2.5 text-slate-400">{formatBytes(w.avg_payload_bytes)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              <div className="card mt-6 overflow-x-auto p-0">
                <div className="border-b border-slate-800 px-4 py-3 text-sm font-semibold text-slate-200">
                  Recent attempts
                </div>
                <table className="w-full min-w-[720px] text-left text-sm">
                  <thead>
                    <tr className="border-b border-slate-800 text-[11px] tracking-wider text-slate-500 uppercase">
                      <th className="px-4 py-3 font-medium">Job</th>
                      <th className="px-4 py-3 font-medium">Worker</th>
                      <th className="px-4 py-3 font-medium">Status code</th>
                      <th className="px-4 py-3 font-medium">Latency</th>
                      <th className="px-4 py-3 font-medium">Retries</th>
                      <th className="px-4 py-3 font-medium">Payload</th>
                      <th className="px-4 py-3 font-medium">When</th>
                    </tr>
                  </thead>
                  <tbody>
                    {performance.recent_attempts.map((a, i) => (
                      <tr key={`${a.job_id}-${i}`} className="border-b border-slate-800/60 last:border-0">
                        <td className="px-4 py-2.5 font-mono text-xs text-sky-600">{a.job_id}</td>
                        <td className="px-4 py-2.5">
                          <WorkerBadge worker={a.worker} />
                        </td>
                        <td className="px-4 py-2.5">
                          <span
                            className={cn(
                              "font-mono text-xs",
                              a.status_code >= 400 ? "text-rose-400" : "text-emerald-400",
                            )}
                          >
                            {a.status_code || "-"}
                          </span>
                        </td>
                        <td className="px-4 py-2.5 tabular-nums text-slate-300">{formatNumber(a.latency_ms)} ms</td>
                        <td className="px-4 py-2.5 tabular-nums text-slate-300">{a.retry_count}</td>
                        <td className="px-4 py-2.5 text-slate-300">{formatBytes(a.payload_size_bytes)}</td>
                        <td className="px-4 py-2.5 text-xs whitespace-nowrap text-slate-400">{a.created_at}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )
        ) : null}
      </section>

      {/* ---------------- Human evaluation form ---------------- */}
      <div id="human-evaluation" className="card max-w-3xl p-6">
        <h2 className="mb-1 flex items-center gap-2 text-base font-semibold text-slate-100">
          <ClipboardCheck className="h-5 w-5 text-sky-400" />
          Submit a human evaluation
        </h2>
        <p className="mb-6 text-xs leading-relaxed text-slate-500">
          Record what you believe the correct label should have been. It is compared against the
          LLM's stored prediction in ClickHouse (<code className="font-mono">model_evaluations</code>)
          and feeds the accuracy / macro-F1 metrics above.
        </p>

        <form onSubmit={(e) => void submitEvaluation(e)} className="space-y-5">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <label className="label" htmlFor="ev-item">
                Item ID *
              </label>
              <input
                id="ev-item"
                className="input font-mono"
                value={itemId}
                onChange={(e) => setItemId(e.target.value)}
                placeholder="ITEM00000001"
                spellCheck={false}
              />
            </div>
            <div>
              <label className="label" htmlFor="ev-job">
                Job ID *
              </label>
              <input
                id="ev-job"
                className="input font-mono"
                value={jobId}
                onChange={(e) => setJobId(e.target.value)}
                placeholder="JOB00000001"
                spellCheck={false}
              />
            </div>
          </div>

          <div>
            <label className="label" htmlFor="ev-by">
              Evaluated by *
            </label>
            <input
              id="ev-by"
              className="input"
              value={evaluatedBy}
              onChange={(e) => setEvaluatedBy(e.target.value)}
              placeholder={session?.user.username ?? "your name"}
            />
          </div>

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            <div>
              <label className="label" htmlFor="ev-source">
                Expected source type *
              </label>
              <select
                id="ev-source"
                className="input capitalize"
                value={sourceType}
                onChange={(e) => setSourceType(e.target.value)}
              >
                <option value="">Select...</option>
                {SOURCE_TYPES.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="label" htmlFor="ev-topic">
                Expected topic *
              </label>
              <select
                id="ev-topic"
                className="input"
                value={topic}
                onChange={(e) => setTopic(e.target.value)}
              >
                <option value="">Select...</option>
                {CONTENT_TOPICS.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="label" htmlFor="ev-cat">
                Expected category *
              </label>
              <select
                id="ev-cat"
                className="input"
                value={category}
                onChange={(e) => setCategory(e.target.value)}
              >
                <option value="">Select...</option>
                {INTELLIGENCE_CATEGORIES.map((c) => (
                  <option key={c} value={c}>
                    {c.replace("_", " ")}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {formError && (
            <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-4 py-3 text-sm break-words text-rose-200">
              {formError}
            </div>
          )}

          {success && (
            <div className="flex flex-col gap-2 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
              <p className="flex items-center gap-2 text-sm text-emerald-200">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-400" />
                Evaluation recorded:
                <span className="font-mono">{success.evaluation_id.slice(0, 8)}</span>
              </p>
              <CopyButton value={success.evaluation_id} label="Copy ID" />
            </div>
          )}

          <button type="submit" className="btn-primary min-w-48" disabled={submitting}>
            {submitting ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <BarChart3 className="h-4 w-4" />
            )}
            {submitting ? "Recording..." : "Record evaluation"}
          </button>
        </form>
      </div>
    </div>
  )
}
