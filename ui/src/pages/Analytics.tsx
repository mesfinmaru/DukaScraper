import { useCallback, useEffect, useMemo, useState } from "react"
import type { FormEvent } from "react"
import { useNavigate } from "react-router-dom"
import { SelectFilter } from "../components/SelectFilter"
import { useAutoRefresh } from "../useAutoRefresh"
import {
  BarChart3,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  ClipboardCheck,
  Loader2,
  Network,
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
  EntityMentionsResponse,
  EntitySummaryResponse,
  EvaluationResponse,
  MetricsResponse,
  ThreatAnalyticsResponse,
  ThreatRecentRow,
} from "../types"
import { cn, formatNumber, humanize } from "../utils"
import {
  CopyButton,
  ErrorBanner,
  LoadingBlock,
  Msg,
  PageHeader,
} from "../components/ui"

/** Percentage for a 0..1 ratio, or "--" when the backend has no value. */
function pct(v: number | null | undefined): string {
  return v === null || v === undefined ? "--" : `${(v * 100).toFixed(1)}%`
}

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

type AnalyticsSection = "threats" | "entities"

/** Section switcher, top right of the page header. */
const ANALYTICS_SECTIONS: { key: AnalyticsSection; label: string }[] = [
  { key: "threats", label: "Threat analytics" },
  { key: "entities", label: "Entity intelligence" },
]

/** Rows per page in the flagged-content list. */
const THREAT_PAGE_SIZE = 50

export default function Analytics() {
  const { session } = useAuth()
  const navigate = useNavigate()

  const [metrics, setMetrics] = useState<MetricsResponse | null>(null)
  const [metricsLoading, setMetricsLoading] = useState(true)
  const [metricsError, setMetricsError] = useState<string | null>(null)

  const [threats, setThreats] = useState<ThreatAnalyticsResponse | null>(null)
  const [threatsLoading, setThreatsLoading] = useState(true)
  const [threatsError, setThreatsError] = useState<string | null>(null)

  // Which single section to render. The page used to stack all of them, which
  // meant a lot of scrolling before reaching any one.
  const [section, setSection] = useState<AnalyticsSection>("threats")
  // Server-side paging for the flagged-content list.
  const [threatPage, setThreatPage] = useState(0)
  const [filterCategory, setFilterCategory] = useState("")
  const [filterSeverity, setFilterSeverity] = useState("")
  const [filterSource, setFilterSource] = useState("")

  const [entities, setEntities] = useState<EntitySummaryResponse | null>(null)
  const [entitiesLoading, setEntitiesLoading] = useState(true)
  const [entitiesError, setEntitiesError] = useState<string | null>(null)
  const [mentions, setMentions] = useState<EntityMentionsResponse | null>(null)
  const [mentionsLoading, setMentionsLoading] = useState(false)


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
      setThreats(
        await api.getThreatAnalytics({
          offset: threatPage * THREAT_PAGE_SIZE,
          limit: THREAT_PAGE_SIZE,
          category: filterCategory || undefined,
          severity: filterSeverity ? Number(filterSeverity) : undefined,
          source_type: filterSource || undefined,
        }),
      )
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
  }, [threatPage, filterCategory, filterSeverity, filterSource])

  const loadEntities = useCallback(async () => {
    setEntitiesLoading(true)
    try {
      setEntities(await api.getIntelligenceEntities(100))
      setEntitiesError(null)
    } catch (e) {
      setEntitiesError(
        e instanceof ApiError && e.status === 503
          ? "Analytics database unavailable - is ClickHouse running?"
          : e instanceof Error
            ? e.message
            : "Failed to load entity intelligence",
      )
      setEntities(null)
    } finally {
      setEntitiesLoading(false)
    }
  }, [])

  const loadMentions = useCallback(async (entity: string) => {
    setMentionsLoading(true)
    try {
      setMentions(await api.searchEntityMentions(entity, 50))
    } catch (e) {
      setMentions({ entity, total: 0, mentions: [] })
    } finally {
      setMentionsLoading(false)
    }
  }, [])


  useEffect(() => {
    void loadMetrics()
    // Only fetch the section on screen. The old page loaded everything on
    // mount, which is most of the reason it felt slow.
    if (section === "threats") void loadThreats()
    if (section === "entities") void loadEntities()
  }, [loadMetrics, loadThreats, loadEntities, section])

  // Refresh the visible section every 5s; no manual refresh button anywhere.
  useAutoRefresh({ load: loadThreats, enabled: section === "threats" })
  useAutoRefresh({ load: loadEntities, enabled: section === "entities" })
  useAutoRefresh({ load: loadMetrics })

  useEffect(() => {
    setEvaluatedBy(session?.user.username ?? "")
  }, [session?.user.username])

  const submitEvaluation = async (e: FormEvent) => {
    e.preventDefault()
    setFormError(null)
    setSuccess(null)

    if (!itemId.trim() || !jobId.trim() || !evaluatedBy.trim()) {
      setFormError("Fill in Item ID, Job ID and evaluator name.")
      return
    }
    if (!sourceType || !topic || !category) {
      setFormError("Choose the source type, topic and category.")
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
        setFormError("Reports are unavailable right now. Try again soon.")
      } else {
        setFormError(err instanceof Error ? err.message : "Could not save this.")
      }
    } finally {
      setSubmitting(false)
    }
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

  // Filter options come from the facets the endpoint returns, so the dropdowns
  // only ever offer values that are actually present.
  const categoryOptions = useMemo(
    () => [
      { value: "", label: "All" },
      ...(threats?.categories ?? []).map((c) => ({ value: c, label: humanize(c) })),
    ],
    [threats],
  )
  const sourceOptions = useMemo(
    () => [
      { value: "", label: "All" },
      ...(threats?.sources ?? []).map((s) => ({ value: s, label: humanize(s) })),
    ],
    [threats],
  )
  const severityOptions = useMemo(() => {
    const counts = new Map(
      (threats?.by_severity ?? []).map((s) => [String(s.severity), s.count]),
    )
    return [
      { value: "", label: "All" },
      ...[1, 2, 3, 4, 5].map((level) => ({
        value: String(level),
        label: `Level ${level}`,
        count: counts.get(String(level)) ?? 0,
      })),
    ]
  }, [threats])

  // Any filter change restarts at page 1: a page-5 offset applied to a newly
  // narrowed result set would show the middle of the filtered rows and read as
  // "the filter deleted my data".
  const applyFilter = (
    setter: (v: string) => void,
  ) => (value: string) => {
    setter(value)
    setThreatPage(0)
  }

  const threatTotal = threats?.total ?? 0
  const threatFrom = threatTotal === 0 ? 0 : threatPage * THREAT_PAGE_SIZE + 1
  const threatTo = threatPage * THREAT_PAGE_SIZE + (threats?.recent.length ?? 0)

  return (
    <div>
      <PageHeader
        title="Analytics & evaluation"
        actions={
          <div
            className="flex rounded-lg border border-slate-800 bg-slate-900/60 p-1"
            role="tablist"
            aria-label="Analytics section"
          >
            {ANALYTICS_SECTIONS.map((s) => (
              <button
                key={s.key}
                type="button"
                role="tab"
                aria-selected={section === s.key}
                onClick={() => setSection(s.key)}
                className={cn(
                  "cursor-pointer rounded-md px-3 py-1.5 text-xs font-medium transition",
                  section === s.key
                    ? "bg-sky-500/20 text-sky-400"
                    : "text-slate-400 hover:text-slate-200",
                )}
              >
                {s.label}
              </button>
            ))}
          </div>
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
          <p className="mb-2 text-xs text-slate-600">
            Based on{" "}
            <span className="font-medium text-slate-400">
              {metrics.coverage?.labeled_items ?? 0}
            </span>{" "}
            human-labelled item
            {(metrics.coverage?.labeled_items ?? 0) === 1 ? "" : "s"} out of{" "}
            {(metrics.coverage?.total_analyzed_items ?? 0).toLocaleString()} analysed
            {metrics.coverage?.labeled_items ? (
              <>
                {" "}
                ({" "}
                {(
                  ((metrics.coverage.labeled_items /
                    Math.max(metrics.coverage.total_analyzed_items, 1)) *
                  100
                ).toFixed(1)
                )}
                % sampled )
              </>
            ) : (
              " — label some items above to get a first reading."
            )}
          </p>
          <p className="mb-8 text-xs text-slate-600">{metrics.note}</p>
        </>
      ) : null}

      {/* ---------------- Threat intelligence ---------------- */}
      {section === "threats" && (
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
                {/* justify-center: this card is one short number next to a much
                    taller severity chart, so it keeps the shared height without
                    leaving its value stranded at the top of an empty box. */}
                <div className="card flex flex-col justify-center p-5">
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

              <div className="card table-scroll p-0">
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
                        <td data-label="Category" className="px-4 py-2.5 capitalize text-slate-200">
                          {c.category.replace(/_/g, " ")}
                        </td>
                        <td data-label="Items" className="px-4 py-2.5 tabular-nums text-slate-300">
                          {formatNumber(c.count)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="card mt-6 table-scroll p-0">
                <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 px-4 py-3">
                  <span className="text-sm font-semibold text-slate-200">Flagged content</span>
                  <span className="text-[11px] text-slate-500">
                    Click a row to view parsed content, or Evaluate to fill the form below
                  </span>
                </div>

                {/* Filters + paging. One dropdown each, no apply button. */}
                <div className="flex flex-wrap items-center gap-2 border-b border-slate-800/60 px-4 py-2.5">
                  <SelectFilter
                    label="Category"
                    value={filterCategory}
                    options={categoryOptions}
                    onChange={applyFilter(setFilterCategory)}
                    className="w-44"
                    allLabel="All"
                  />
                  <SelectFilter
                    label="Severity"
                    value={filterSeverity}
                    options={severityOptions}
                    onChange={applyFilter(setFilterSeverity)}
                    className="w-36"
                    allLabel="All"
                  />
                  <SelectFilter
                    label="Source"
                    value={filterSource}
                    options={sourceOptions}
                    onChange={applyFilter(setFilterSource)}
                    className="w-36"
                    allLabel="All"
                  />

                  <div className="ml-auto flex items-center gap-2">
                    <span className="text-[11px] tabular-nums text-slate-500">
                      {threatTotal === 0
                        ? "No matching items"
                        : `Showing ${formatNumber(threatFrom)}-${formatNumber(threatTo)} of ${formatNumber(threatTotal)}`}
                    </span>
                    <button
                      type="button"
                      onClick={() => setThreatPage((p) => Math.max(0, p - 1))}
                      disabled={threatPage === 0}
                      className="btn-secondary px-2 py-1 text-xs disabled:cursor-not-allowed disabled:opacity-40"
                      title="Previous page"
                    >
                      <ChevronLeft className="h-3.5 w-3.5" />
                      Prev
                    </button>
                    <button
                      type="button"
                      onClick={() => setThreatPage((p) => p + 1)}
                      disabled={!threats?.has_more}
                      className="btn-secondary px-2 py-1 text-xs disabled:cursor-not-allowed disabled:opacity-40"
                      title="Next page"
                    >
                      Next
                      <ChevronRight className="h-3.5 w-3.5" />
                    </button>
                  </div>
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
                        <td data-label="Item" className="px-4 py-2.5">
                          <span className="font-mono text-xs text-sky-600">{row.item_id}</span>
                        </td>
                        <td data-label="Category" className="px-4 py-2.5 capitalize text-slate-200">
                          {row.category.replace(/_/g, " ")}
                        </td>
                        <td data-label="Severity" className="px-4 py-2.5">
                          <SeverityBadge severity={row.severity} />
                        </td>
                        <td data-label="Source" className="px-4 py-2.5 capitalize text-slate-300">{row.source_type}</td>
                        <td data-label="URL" className="max-w-56 truncate px-4 py-2.5 font-mono text-xs text-slate-400" title={row.url}>
                          {row.url}
                        </td>
                        <td data-label="Summary" className="max-w-72 px-4 py-2.5 text-xs text-slate-300" title={row.summary}>
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
      )}

      {/* ---------------- Entity intelligence ---------------- */}
      {section === "entities" && (
      <section className="mb-10">
        <h2 className="mb-1 flex items-center gap-2 text-base font-semibold text-slate-100">
          <Network className="h-5 w-5 text-sky-400" />
          Entity intelligence
        </h2>
        <p className="mb-4 text-xs leading-relaxed text-slate-500">
          People, hosts, IPs and emails found in the analyzed pages. Click one to see every
          page it appears on.
        </p>

        {entitiesError && (
          <div className="mb-4">
            <ErrorBanner message={entitiesError} onRetry={() => void loadEntities()} />
          </div>
        )}

        {entitiesLoading && !entities ? (
          <LoadingBlock label="Loading entity intelligence..." />
        ) : entities && entities.total === 0 ? (
          <div className="card p-8 text-center text-sm text-slate-500">
            No entities extracted yet - they appear after the llm-worker analyzes content.
          </div>
        ) : entities ? (
          <div className="card table-scroll p-0">
            <table className="w-full min-w-[720px] text-left text-sm">
              <thead>
                <tr className="border-b border-slate-800 text-[11px] tracking-wider text-slate-500 uppercase">
                  <th className="px-4 py-3 font-medium">Entity</th>
                  <th className="px-4 py-3 font-medium">Type</th>
                  <th className="px-4 py-3 font-medium">Pages</th>
                  <th className="px-4 py-3 font-medium">Sample sources</th>
                </tr>
              </thead>
              <tbody>
                {entities.entities.map((e) => (
                  <tr
                    key={`${e.entity_type}:${e.entity}`}
                    onClick={() => void loadMentions(e.entity)}
                    title={`Find every page mentioning "${e.entity}"`}
                    className="cursor-pointer border-b border-slate-800/60 last:border-0 hover:bg-slate-900/40"
                  >
                    <td data-label="Entity" className="px-4 py-2.5 font-mono text-xs break-all text-sky-300">{e.entity}</td>
                    <td data-label="Type" className="px-4 py-2.5">
                      <span className="rounded-full border border-slate-700 px-2 py-0.5 text-[11px] text-slate-400 capitalize">
                        {e.entity_type}
                      </span>
                    </td>
                    <td data-label="Pages" className="px-4 py-2.5 tabular-nums text-slate-300">{formatNumber(e.occurrences)}</td>
                    <td data-label="Sample sources" className="max-w-72 px-4 py-2.5 font-mono text-[11px] text-slate-500">
                      {e.sample_urls.length ? e.sample_urls[0] : "--"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}

        {mentionsLoading && <LoadingBlock label={`Searching mentions of "${mentions?.entity ?? ""}"...`} />}

        {mentions && !mentionsLoading && (
          <div className="card mt-4 table-scroll p-0">
            <div className="flex items-center justify-between gap-2 border-b border-slate-800 px-4 py-3">
              <span className="text-sm font-semibold text-slate-200">
                Pages mentioning <span className="font-mono text-sky-300">{mentions.entity}</span>
              </span>
              <span className="text-[11px] text-slate-500">{formatNumber(mentions.total)} found</span>
            </div>
            {mentions.total === 0 ? (
              <p className="px-4 py-6 text-center text-sm text-slate-500">No mentions found.</p>
            ) : (
              <table className="w-full min-w-[720px] text-left text-sm">
                <thead>
                  <tr className="border-b border-slate-800 text-[11px] tracking-wider text-slate-500 uppercase">
                    <th className="px-4 py-3 font-medium">Item</th>
                    <th className="px-4 py-3 font-medium">Category</th>
                    <th className="px-4 py-3 font-medium">Severity</th>
                    <th className="px-4 py-3 font-medium">URL</th>
                  </tr>
                </thead>
                <tbody>
                  {mentions.mentions.map((m, i) => (
                    <tr
                      key={`${m.item_id}-${i}`}
                      onClick={() => navigate(`/jobs/${m.job_id}/articles?itemId=${encodeURIComponent(m.item_id)}`)}
                      className="cursor-pointer border-b border-slate-800/60 last:border-0 hover:bg-slate-900/40"
                    >
                      <td data-label="Item" className="px-4 py-2.5 font-mono text-xs text-sky-600">{m.item_id}</td>
                      <td data-label="Category" className="px-4 py-2.5 capitalize text-slate-200">{m.category.replace(/_/g, " ")}</td>
                      <td data-label="Severity" className="px-4 py-2.5">
                        <SeverityBadge severity={m.severity} />
                      </td>
                      <td data-label="URL" className="max-w-72 truncate px-4 py-2.5 font-mono text-xs text-slate-400" title={m.url}>
                        {m.url}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}
      </section>
      )}

      {/* ---------------- Human evaluation form ---------------- */}
      {section === "threats" && (
      <div id="human-evaluation" className="card max-w-3xl p-6">
        <h2 className="mb-1 flex items-center gap-2 text-base font-semibold text-slate-100">
          <ClipboardCheck className="h-5 w-5 text-sky-400" />
          Submit a human evaluation
        </h2>
        <p className="mb-6 text-xs leading-relaxed text-slate-500">
          Enter the label you believe is correct. We compare it with the model's answer to
          measure accuracy.
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

          {formError && <Msg tone="error" className="text-sm">{formError}</Msg>}

          {success && (
            <Msg tone="success" className="items-center text-sm sm:justify-between">
              <CheckCircle2 className="h-4 w-4 shrink-0" aria-hidden />
              <span className="min-w-0 flex-1">Saved.</span>
              <CopyButton value={success.evaluation_id} label="Copy ID" />
            </Msg>
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
      )}
    </div>
  )
}
