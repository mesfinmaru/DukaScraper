import { useCallback, useEffect, useState } from "react"
import type { ReactNode } from "react"
import {
  Activity,
  Database,
  ExternalLink,
  Gauge,
  Loader2,
  Radio,
  RefreshCw,
} from "lucide-react"
import { api, ApiError } from "../api"
import {
  ErrorBanner,
  LoadingBlock,
  PageHeader,
} from "../components/ui"
import type { MonitoringHealthResponse, MonitoringUrlsResponse, PrometheusMetricsResponse } from "../types"
import { cn } from "../utils"

const SERVICE_ICONS: Record<string, typeof Database> = {
  postgres: Database,
  clickhouse: Database,
  elasticsearch: Database,
  kafka: Radio,
  redis: Database,
  minio: Database,
  qdrant: Database,
  ollama: Gauge,
}

const DEFAULT_URLS: MonitoringUrlsResponse = {
  grafana: "http://localhost:3000",
  prometheus: "http://localhost:9090",
  kafka_ui: "http://localhost:8088",
  kibana: "http://localhost:5601",
  pgadmin: "http://localhost:5050",
  minio_console: "http://localhost:9001",
  api: "http://localhost:8000",
}

type EmbedTab = { key: string; label: string; embedUrl: string; openUrl: string }

const EMBED_TABS = (base: string): EmbedTab[] => [
  {
    key: "overview",
    label: "Overview",
    embedUrl: `${base}/d/duka-overview?orgId=1&refresh=30s&kiosk`,
    openUrl: `${base}/d/duka-overview?orgId=1`,
  },
]

const promGraphUrl = (base: string) => {
  const expr = encodeURIComponent(
    'sum(rate(duka_http_requests_total{job="duka-api"}[5m]))',
  )
  return `${base}/graph?g0.expr=${expr}&g0.tab=0&g0.stacked=1&g0.range_input=2h`
}

const PROMETHEUS_TABS = (base: string): EmbedTab[] => [
  { key: "traffic", label: "Traffic", embedUrl: promGraphUrl(base), openUrl: promGraphUrl(base) },
  { key: "targets", label: "Targets", embedUrl: `${base}/targets`, openUrl: `${base}/targets` },
  { key: "alerts", label: "Alerts", embedUrl: `${base}/alerts`, openUrl: `${base}/alerts` },
]

const ACCENTS = {
  grafana: {
    text: "text-orange-400",
    tabActive: "bg-orange-500/20 text-orange-300",
    linkHover: "hover:border-orange-500 hover:text-orange-300",
  },
  prometheus: {
    text: "text-rose-400",
    tabActive: "bg-rose-500/20 text-rose-300",
    linkHover: "hover:border-rose-500 hover:text-rose-300",
  },
} as const

function EmbedSection({
  icon,
  title,
  description,
  tabs,
  activeTab,
  onSelect,
  openLabel,
  provider,
}: {
  icon: ReactNode
  title: string
  description: string
  tabs: EmbedTab[]
  activeTab: string | null
  onSelect: (key: string) => void
  openLabel: string
  provider: keyof typeof ACCENTS
}) {
  const accent = ACCENTS[provider]
  const active = tabs.find((t) => t.key === activeTab)

  return (
    <section>
      <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="flex items-center gap-2 text-base font-semibold text-slate-100">
            {icon}
            {title}
          </h2>
          <p className="mt-1 text-xs text-slate-500">{description}</p>
        </div>
        {tabs.length > 1 && (
          <div className="inline-flex rounded-lg border border-slate-800 bg-slate-900 p-0.5">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                type="button"
                onClick={() => onSelect(tab.key)}
                className={cn(
                  "cursor-pointer rounded-md px-3 py-1.5 text-xs font-medium transition",
                  activeTab === tab.key
                    ? accent.tabActive
                    : "text-slate-400 hover:text-slate-200",
                )}
              >
                {tab.label}
              </button>
            ))}
          </div>
        )}
      </div>

      {active ? (
        <div className="overflow-hidden rounded-xl border border-slate-800 bg-[#050b08]">
          <div className="flex items-center justify-between gap-2 border-b border-slate-800 px-4 py-2">
            <div className="flex items-center gap-2">
              {icon}
              <span className="text-sm font-medium text-slate-200">{active.label}</span>
              <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium text-emerald-400">
                as of {new Date().toLocaleTimeString()}
              </span>
            </div>
            <a
              href={active.openUrl}
              target="_blank"
              rel="noreferrer"
              className={cn(
                "inline-flex shrink-0 items-center gap-1.5 rounded-md border border-slate-700 bg-black px-2 py-1 text-[11px] font-medium text-slate-300 transition",
                accent.linkHover,
              )}
            >
              Open in {openLabel}
              <ExternalLink className="h-3 w-3" />
            </a>
          </div>
          <iframe
            key={active.key}
            src={active.embedUrl}
            title={active.label}
            className="h-[600px] w-full border-0"
            loading="lazy"
            referrerPolicy="no-referrer"
          />
        </div>
      ) : (
        <div className="card p-10 text-center text-sm text-slate-500">
          Start {openLabel} to view {title.toLowerCase()}.
        </div>
      )}

      <p className="mt-3 flex items-center gap-2 text-xs text-slate-600">
        <Loader2 className="h-3 w-3" />
        If the iframe stays blank, run `docker compose up -d grafana` and make sure
        GF_ALLOW_EMBEDDING=true is set (already configured in docker-compose.yml).
      </p>
    </section>
  )
}

function StatusDot({ status }: { status: string }) {
  const dot =
    status === "healthy"
      ? "bg-emerald-500"
      : status === "disconnected"
        ? "bg-amber-500"
        : "bg-rose-500"
  const ping = status === "healthy"
  return (
    <span className="relative flex h-2 w-2">
      {ping && (
        <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-60" />
      )}
      <span className={cn("relative inline-flex h-2 w-2 rounded-full", dot)} />
    </span>
  )
}

export default function Monitoring() {
  const [health, setHealth] = useState<MonitoringHealthResponse | null>(null)
  const [urls, setUrls] = useState<MonitoringUrlsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [grafanaTabs, setGrafanaTabs] = useState<EmbedTab[]>([])
  const [promTabs, setPromTabs] = useState<EmbedTab[]>([])
  const [grafanaTab, setGrafanaTab] = useState<string | null>(null)
  const [promTab, setPromTab] = useState<string | null>(null)
  const [promMetrics, setPromMetrics] = useState<PrometheusMetricsResponse | null>(null)
  const [promLoading, setPromLoading] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)

    let healthData: MonitoringHealthResponse | null = null
    let urlsData: MonitoringUrlsResponse = DEFAULT_URLS

    try {
      healthData = await api.getMonitoringHealth()
      setError(null)
    } catch (e) {
      setError(
        e instanceof ApiError && e.status === 403
          ? "Admin access required to view monitoring."
          : e instanceof Error
            ? e.message
            : "Failed to load monitoring data",
      )
    }

    try {
      urlsData = (await api.getMonitoringUrls()) ?? DEFAULT_URLS
    } catch {
      urlsData = DEFAULT_URLS
    }

    setHealth(healthData)
    setUrls(urlsData)
    setGrafanaTabs(EMBED_TABS(urlsData.grafana))
    setPromTabs(PROMETHEUS_TABS(urlsData.prometheus))
    setGrafanaTab((prev) => prev ?? EMBED_TABS(urlsData.grafana)[0]?.key ?? null)
    setPromTab((prev) => prev ?? PROMETHEUS_TABS(urlsData.prometheus)[0]?.key ?? null)
    setLoading(false)

    // Load Prometheus metrics
    setPromLoading(true)
    try {
      const metrics = await api.getPrometheusMetrics()
      setPromMetrics(metrics)
    } catch {
      setPromMetrics(null)
    } finally {
      setPromLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div>
      <PageHeader
        title="Monitoring"
        description="Live health of the DukaScraper infrastructure and embedded Grafana / Prometheus views."
        actions={
          <button type="button" className="btn-secondary" onClick={() => void load()}>
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        }
      />

      {error && (
        <div className="mb-6">
          <ErrorBanner message={error} onRetry={() => void load()} />
        </div>
      )}

      {loading && !health ? (
        <LoadingBlock label="Checking infrastructure..." />
      ) : (
        <>
          {/* Service health grid */}
          <section className="mb-8">
            <h2 className="mb-1 flex items-center gap-2 text-base font-semibold text-slate-100">
              <Activity className="h-5 w-5 text-emerald-400" />
              Infrastructure health
            </h2>
            <p className="mb-4 text-xs leading-relaxed text-slate-500">
              Live connectivity check against each backend service. Grafana and Prometheus are the
              primary monitoring tools (below).
            </p>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
              {health?.services.map((svc) => {
                const Icon = SERVICE_ICONS[svc.name.toLowerCase()] ?? Database
                return (
                  <div
                    key={svc.name}
                    className={cn(
                      "card flex items-start gap-3 p-4",
                      svc.status === "healthy" && "ring-1 ring-emerald-500/20",
                      svc.status === "unhealthy" && "ring-1 ring-rose-500/30",
                    )}
                  >
                    <div
                      className={cn(
                        "flex h-9 w-9 shrink-0 items-center justify-center rounded-lg",
                        svc.status === "healthy"
                          ? "bg-emerald-500/10 text-emerald-400"
                          : svc.status === "disconnected"
                            ? "bg-amber-500/10 text-amber-500"
                            : "bg-rose-500/10 text-rose-400",
                      )}
                    >
                      <Icon className="h-4 w-4" />
                    </div>
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center justify-between gap-2">
                        <p className="truncate text-sm font-semibold text-slate-100">{svc.name}</p>
                        <StatusDot status={svc.status} />
                      </div>
                      <p
                        className={cn(
                          "mt-0.5 text-[11px] font-medium capitalize",
                          svc.status === "healthy"
                            ? "text-emerald-500"
                            : svc.status === "disconnected"
                              ? "text-amber-500"
                              : "text-rose-400",
                        )}
                      >
                        {svc.status}
                      </p>
                      <p className="mt-1 truncate font-mono text-[10px] text-slate-500" title={svc.url}>
                        {svc.url || "n/a"}
                      </p>
                    </div>
                  </div>
                )
              })}
            </div>

            {health && (
              <p className="mt-3 text-xs text-slate-600">
                {health.healthy}/{health.total} services healthy
                {health.unhealthy > 0 && (
                  <span className="text-rose-500"> · {health.unhealthy} unhealthy</span>
                )}
              </p>
            )}
          </section>

          {/* Monitoring tool quick links */}
          <section className="mb-8">
            <h2 className="mb-4 flex items-center gap-2 text-base font-semibold text-slate-100">
              <ExternalLink className="h-5 w-5 text-sky-400" />
              Monitoring tools
            </h2>
            {urls && (
              <div className="flex flex-wrap gap-3">
                {[
                  { label: "Grafana", url: urls.grafana },
                  { label: "Prometheus", url: urls.prometheus },
                  { label: "Kafka UI", url: urls.kafka_ui },
                  { label: "Kibana", url: urls.kibana },
                  { label: "pgAdmin", url: urls.pgadmin },
                  { label: "MinIO Console", url: urls.minio_console },
                ].map((tool) => (
                  <a
                    key={tool.label}
                    href={tool.url}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-sm font-medium text-slate-200 transition hover:border-sky-500 hover:text-sky-300"
                  >
                    {tool.url.includes("3000") && <Gauge className="h-4 w-4 text-orange-400" />}
                    {tool.url.includes("9090") && <Activity className="h-4 w-4 text-rose-400" />}
                    {tool.label}
                    <ExternalLink className="h-3.5 w-3.5 text-slate-500" />
                  </a>
                ))}
              </div>
            )}
          </section>

          {/* Grafana embedded dashboard */}
          <div className="mb-10">
            <EmbedSection
              icon={<Gauge className="h-5 w-5 text-orange-400" />}
              title="Grafana dashboards"
              description="Embedded read-only views. Open Grafana for the full editor experience."
              tabs={grafanaTabs}
              activeTab={grafanaTab}
              onSelect={setGrafanaTab}
              openLabel="Grafana"
              provider="grafana"
            />
          </div>

          {/* Prometheus embedded views */}
          <EmbedSection
            icon={<Activity className="h-5 w-5 text-rose-400" />}
            title="Prometheus views"
            description="Live request traffic plus scrape targets and alert rules from Prometheus."
            tabs={promTabs}
            activeTab={promTab}
            onSelect={setPromTab}
            openLabel="Prometheus"
            provider="prometheus"
          />

          {/* Prometheus key metrics */}
          <section className="mb-8">
            <h2 className="mb-4 flex items-center gap-2 text-base font-semibold text-slate-100">
              <Activity className="h-5 w-5 text-rose-400" />
              Prometheus metrics (API)
            </h2>
            <p className="mb-4 text-xs leading-relaxed text-slate-500">
              Key metrics scraped from the API's /metrics endpoint. Refresh to update.
            </p>

            {promLoading && <LoadingBlock label="Loading Prometheus metrics..." />}

            {!promLoading && promMetrics ? (
              <div className="space-y-6">
                {/* In-progress requests */}
                <div>
                  <h3 className="mb-2 text-sm font-medium text-slate-300">In-progress requests</h3>
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                    {Object.entries(promMetrics.http_requests_in_progress).map(([method, value]) => (
                      <div key={method} className="card p-4">
                        <p className="font-mono text-lg font-semibold text-rose-400">{method.toUpperCase()}</p>
                        <p className="text-xs text-slate-500">{value} active</p>
                      </div>
                    ))}
                    {Object.keys(promMetrics.http_requests_in_progress).length === 0 && (
                      <p className="text-sm text-slate-500">No in-progress requests</p>
                    )}
                  </div>
                </div>

                {/* Request totals by path/status */}
                <div>
                  <h3 className="mb-2 text-sm font-medium text-slate-300">Request totals (since startup)</h3>
                  <div className="overflow-x-auto">
                    <table className="w-full min-w-[700px]">
                      <thead className="border-b border-slate-800 bg-slate-950">
                        <tr>
                          <th className="px-4 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Method</th>
                          <th className="px-4 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Path</th>
                          <th className="px-4 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Status</th>
                          <th className="px-4 py-2 text-right text-xs font-semibold tracking-wider text-slate-500 uppercase">Count</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-800/70">
                        {Object.entries(promMetrics.http_requests_total)
                          .sort(([, a], [, b]) => b - a)
                          .slice(0, 30)
                          .map(([key, value]) => {
                            const match = key.match(/^(\w+)\s+(.+)\s+\[(\d+)\]$/)
                            if (!match) return null
                            const [, method, path, status] = match
                            return (
                              <tr key={key} className="hover:bg-slate-800/40">
                                <td className="px-4 py-2 font-mono text-xs text-slate-300">{method}</td>
                                <td className="px-4 py-2 font-mono text-xs text-slate-400 truncate max-w-[300px]">{path}</td>
                                <td className="px-4 py-2">
                                  <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${
                                    status.startsWith("2") ? "bg-emerald-500/10 text-emerald-500" :
                                    status.startsWith("4") ? "bg-amber-500/10 text-amber-500" :
                                    "bg-rose-500/10 text-rose-500"
                                  }`}>
                                    {status}
                                  </span>
                                </td>
                                <td className="px-4 py-2 text-right font-mono text-sm text-slate-200">{value.toLocaleString()}</td>
                              </tr>
                            )
                          })}
                      </tbody>
                    </table>
                  </div>
                  {Object.keys(promMetrics.http_requests_total).length === 0 && (
                    <p className="text-sm text-slate-500">No request data yet</p>
                  )}
                </div>

                {/* Request latency percentiles */}
                <div>
                  <h3 className="mb-2 text-sm font-medium text-slate-300">Request latency (seconds)</h3>
                  <div className="overflow-x-auto">
                    <table className="w-full min-w-[600px]">
                      <thead className="border-b border-slate-800 bg-slate-950">
                        <tr>
                          <th className="px-4 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Method</th>
                          <th className="px-4 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Path</th>
                          <th className="px-4 py-2 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Percentile</th>
                          <th className="px-4 py-2 text-right text-xs font-semibold tracking-wider text-slate-500 uppercase">Value (s)</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-800/70">
                        {Object.entries(promMetrics.http_request_duration_seconds)
                          .sort(([, a], [, b]) => b - a)
                          .slice(0, 30)
                          .map(([key, value]) => {
                            const match = key.match(/^(\w+)\s+(.+)\s+\(p(.+)\)$/)
                            if (!match) return null
                            const [, method, path, percentile] = match
                            return (
                              <tr key={key} className="hover:bg-slate-800/40">
                                <td className="px-4 py-2 font-mono text-xs text-slate-300">{method}</td>
                                <td className="px-4 py-2 font-mono text-xs text-slate-400 truncate max-w-[300px]">{path}</td>
                                <td className="px-4 py-2 text-slate-300">p{percentile}</td>
                                <td className="px-4 py-2 text-right font-mono text-sm text-slate-200">{value.toFixed(4)}</td>
                              </tr>
                            )
                          })}
                      </tbody>
                    </table>
                  </div>
                  {Object.keys(promMetrics.http_request_duration_seconds).length === 0 && (
                    <p className="text-sm text-slate-500">No latency data yet</p>
                  )}
                </div>
              </div>
            ) : !promLoading && !promMetrics ? (
              <div className="card p-6 text-center text-slate-500">
                <p>Failed to load Prometheus metrics. Check admin access.</p>
              </div>
            ) : null}
          </section>
        </>
      )}
    </div>
  )
}