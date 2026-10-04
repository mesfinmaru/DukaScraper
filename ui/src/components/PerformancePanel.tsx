import { useCallback, useEffect, useState } from "react"
import { api, ApiError } from "../api"
import type { PerformanceResponse } from "../types"
import { cn, formatBytes, formatNumber } from "../utils"
import { ErrorBanner, LoadingBlock, WorkerBadge } from "./ui"
import { useAutoRefresh } from "../useAutoRefresh"

/**
 * Crawler latency and reliability, per worker.
 *
 * Lives here rather than inline in a page because it moved from Analytics to
 * Monitoring: one implementation, rendered wherever it is needed.
 */
export default function PerformancePanel({ className }: { className?: string }) {
  const [performance, setPerformance] = useState<PerformanceResponse | null>(null)
  const [perfLoading, setPerfLoading] = useState(true)
  const [perfError, setPerfError] = useState<string | null>(null)

  const loadPerformance = useCallback(async () => {
    try {
      setPerformance(await api.getPerformance())
      setPerfError(null)
    } catch (e) {
      if (e instanceof ApiError && e.status === 503) {
        setPerfError("Performance data is unavailable right now.")
      } else {
        setPerfError(e instanceof Error ? e.message : "Could not load performance data.")
      }
      setPerformance(null)
    } finally {
      setPerfLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadPerformance()
  }, [loadPerformance])

  // Keeps the latency figures current without a manual refresh button.
  useAutoRefresh({ load: loadPerformance })

  return (
    <section className={className}>
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

                <div className="rounded-xl border border-slate-800 bg-[#050b08] table-scroll p-0">
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

                <div className="card mt-6 table-scroll p-0">
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
  )
}

function pct(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "--"
  return `${(value * 100).toFixed(1)}%`
}
