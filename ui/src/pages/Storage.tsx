import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useLocation } from "react-router-dom"
import {
  Archive,
  ChevronLeft,
  ChevronRight,
  Download,
  Eye,
  Funnel,
  FolderSearch,
  HardDrive,
  Loader2,
  Square,
} from "lucide-react"
import { api, ApiError } from "../api"
import { SelectFilter } from "../components/SelectFilter"
import { useAutoRefresh } from "../useAutoRefresh"
import { useAuth } from "../config"
import type { BucketOverviewResponse, BucketItemsResponse, JobSummary } from "../types"
import { cn, formatBytes, formatDateTime, formatNumber } from "../utils"
import {
  CopyButton,
  EmptyState,
  ErrorBanner,
  LoadingBlock,
  Modal,
  Msg,
  PageHeader,
} from "../components/ui"

const TABS = [
  { key: "raw", label: "Raw HTML" },
  { key: "parsed", label: "Parsed JSON" },
  { key: "exports", label: "Exports" },
] as const

type TabKey = (typeof TABS)[number]["key"]

/** Formats the backend can convert on the fly (pdf/docx need extra deps). */
const EXPORT_FORMATS = ["json", "txt", "csv", "html"] as const
type ExportFormat = (typeof EXPORT_FORMATS)[number]

/** Which formats make sense per bucket: raw pages convert to text/html,
 *  parsed JSON converts to anything. Export files are already final. */
const FORMATS_BY_TAB: Record<TabKey, readonly ExportFormat[]> = {
  raw: ["html", "txt"],
  parsed: ["json", "txt", "csv", "html"],
  exports: [],
}

const DEFAULT_FORMAT: Record<TabKey, ExportFormat> = {
  raw: "html",
  parsed: "json",
  exports: "json", // unused - exports tab downloads raw files
}

function saveBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}

/**
 * Rows per page. Larger pages than the old 25/50/100/200 because the request is
 * now paged server-side, so a bigger page costs one round trip rather than the
 * user clicking through more pages.
 */
const PAGE_SIZES = [200, 500, 1000]

/** Truncate a site label for the dropdown without hiding which one it is. */
function siteLabel(site: string): string {
  return site.length > 28 ? `${site.slice(0, 27)}…` : site
}

export default function Storage() {
  const location = useLocation()
  const { session } = useAuth()
  const isAdmin = session?.user.role === "admin"
  const state = (location.state as { tab?: TabKey; prefix?: string } | null) ?? {}
  const stateTab = state.tab
  const statePrefix = state.prefix
  const [overview, setOverview] = useState<BucketOverviewResponse | null>(null)
  const [tab, setTab] = useState<TabKey>(() =>
    stateTab === "raw" || stateTab === "parsed" || stateTab === "exports" ? stateTab : "raw",
  )
  const [prefixInput, setPrefixInput] = useState(() => statePrefix ?? "")
  const [prefix, setPrefix] = useState(() => statePrefix ?? "")
  const [data, setData] = useState<BucketItemsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [viewer, setViewer] = useState<{ name: string; content: string } | null>(null)
  const [viewLoading, setViewLoading] = useState(false)
  const [viewError, setViewError] = useState<string | null>(null)
  const [batchFormat, setBatchFormat] = useState<ExportFormat>(DEFAULT_FORMAT.raw)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [batchBusy, setBatchBusy] = useState(false)
  const [batchError, setBatchError] = useState<string | null>(null)
  const [jobs, setJobs] = useState<JobSummary[]>([])
  const [jobFilter, setJobFilter] = useState<string>("all")
  const [siteFilter, setSiteFilter] = useState<string>("")
  /**
   * Cursors for the pages already visited. `cursorStack[i]` is the `after` value
   * for page i, so Previous is a pop rather than a backwards offset - and an
   * offset request would make the server walk and discard every skipped key.
   */
  const [cursorStack, setCursorStack] = useState<string[]>([])
  const [pageSize, setPageSize] = useState(200)
  const selectAllRef = useRef<HTMLInputElement>(null)

  const canConvert = tab !== "exports"

  // Keep the batch format in sync when switching tabs.
  useEffect(() => {
    setBatchFormat(DEFAULT_FORMAT[tab])
    setBatchError(null)
  }, [tab])

  // The page *is* the result set now - filtering and slicing happen on the
  // server, so there is nothing to slice here.
  const items = data?.items ?? []
  const pagedItems = items

  /** Site labels present on the current page, for the filter dropdown. */
  const sites = useMemo(() => {
    const seen = new Set<string>()
    for (const item of items) if (item.site) seen.add(item.site)
    return [...seen].sort()
  }, [items])

  const totalBytes = useMemo(
    () => items.reduce((sum, item) => sum + (item.size_bytes ?? 0), 0),
    [items],
  )

  // Any change to what is being listed invalidates the cursor history: the
  // cursors are keys within the *previous* result set.
  useEffect(() => {
    setCursorStack([])
  }, [tab, prefix, jobFilter, siteFilter, pageSize])

  // Jobs for the filter dropdown (same scoping as the Jobs page).
  useEffect(() => {
    if (!session) {
      setJobs([])
      return
    }
    let cancelled = false
    const loadJobs = async () => {
      try {
        const res = isAdmin
          ? await api.getAllJobs()
          : await api.getUserJobs(session.user.user_id)
        if (!cancelled) {
          setJobs(
            [...res.jobs].sort((a, b) =>
              (b.created_at ?? "").localeCompare(a.created_at ?? ""),
            ),
          )
        }
      } catch {
        if (!cancelled) setJobs([]) // dropdown just stays empty
      }
    }
    void loadJobs()
    return () => {
      cancelled = true
    }
  }, [session, isAdmin])

  const allSelected = pagedItems.length > 0 && selected.size === pagedItems.length
  const someSelected = selected.size > 0 && !allSelected

  useEffect(() => {
    if (selectAllRef.current) {
      selectAllRef.current.indeterminate = someSelected
    }
  }, [someSelected])

  const toggleSelectAll = () => {
    setSelected(allSelected ? new Set() : new Set(pagedItems.map((i) => i.object_name)))
  }

  const toggleOne = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  const downloadSelected = async () => {
    if (selected.size === 0) return
    setBatchBusy(true)
    setBatchError(null)
    try {
      const blob = await api.downloadMany(tab, Array.from(selected), batchFormat)
      saveBlob(blob, `duka-${tab}-${selected.size}-items.zip`)
    } catch (e) {
      setBatchError(e instanceof Error ? e.message : "Failed to download selection")
    } finally {
      setBatchBusy(false)
    }
  }

  const openObject = useCallback(
    async (name: string) => {
      setViewer({ name, content: "" })
      setViewError(null)
      setViewLoading(true)
      try {
        const text = await api.getStorageObjectText(tab, name)
        let pretty = text
        try {
          pretty = JSON.stringify(JSON.parse(text), null, 2)
        } catch {
          /* not JSON - show raw text */
        }
        setViewer({ name, content: pretty })
      } catch (e) {
        setViewError(e instanceof Error ? e.message : "Failed to open object")
      } finally {
        setViewLoading(false)
      }
    },
    [tab],
  )

  // Debounce prefix so typing doesn't hammer the API.
  useEffect(() => {
    const t = setTimeout(() => setPrefix(prefixInput.trim()), 350)
    return () => clearTimeout(t)
  }, [prefixInput])

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const res = await api.listBucketItems(tab, prefix, {
        limit: pageSize,
        after: cursorStack[cursorStack.length - 1] ?? "",
        job_id: jobFilter === "all" ? "" : jobFilter,
        site: siteFilter,
      })
      setData(res)
      setError(null)
    } catch (e) {
      if (e instanceof ApiError && e.status === 404) {
        setError("That list is not available.")
      } else {
        setError(e instanceof Error ? e.message : "Could not load this list.")
      }
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [tab, prefix, pageSize, cursorStack, jobFilter, siteFilter])

  useEffect(() => {
    void load()
  }, [load])

  // Keep the listing current without a manual refresh button. Storage was the
  // worst page for this: every tick used to re-download the whole bucket.
  useAutoRefresh({ load })

  /** Advance one page, remembering the cursor we came from. */
  const nextPage = () => {
    const cursor = data?.next_cursor
    if (!cursor) return
    setCursorStack((stack) => [...stack, cursor])
  }

  /** Go back one page by dropping the cursor that produced the current one. */
  const prevPage = () => setCursorStack((stack) => stack.slice(0, -1))

  // Drop selections that are no longer visible (filter changed / page reloaded).
  useEffect(() => {
    setSelected((prev) => {
      if (prev.size === 0) return prev
      const visible = new Set(pagedItems.map((i) => i.object_name))
      let changed = false
      const next = new Set<string>()
      for (const name of prev) {
        if (visible.has(name)) next.add(name)
        else changed = true
      }
      return changed ? next : prev
    })
  }, [pagedItems])

  useEffect(() => {
    api
      .listBuckets()
      .then(setOverview)
      .catch(() => setOverview(null))
  }, [])


  return (
    <div>
      <PageHeader title="Storage" />

      <div className="mb-5 flex flex-wrap items-center gap-3">
        <div className="flex rounded-lg border border-slate-800 bg-slate-900/60 p-1">
          {TABS.map((t) => (
            <button
              key={t.key}
              type="button"
              onClick={() => setTab(t.key)}
              className={cn(
                "cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition",
                tab === t.key
                  ? "bg-sky-500/20 text-sky-500"
                  : "text-slate-400 hover:text-slate-200",
              )}
            >
              {t.label}
              {overview?.buckets[t.key] && (
                <span className="ml-2 hidden font-mono text-[11px] text-slate-600 lg:inline">
                  {overview.buckets[t.key]}
                </span>
              )}
            </button>
          ))}
        </div>

        <div className="relative min-w-56 flex-1 sm:max-w-xs">
          <FolderSearch className="pointer-events-none absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-slate-500" />
          <input
            className="input py-2 pl-9 font-mono text-xs"
            value={prefixInput}
            onChange={(e) => setPrefixInput(e.target.value)}
            placeholder={`Filter by prefix e.g. ${tab === "exports" ? "JOB00000001" : "JOB00000001/"}`}
            spellCheck={false}
          />
        </div>

        <div className="relative">
          <Funnel className="pointer-events-none absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-slate-500" />
          <select
            value={jobFilter}
            onChange={(e) => setJobFilter(e.target.value)}
            className="input cursor-pointer py-2 pr-8 pl-9 font-mono text-xs"
            aria-label="Filter by job"
            title="Show only objects produced by this job"
          >
            <option value="all">All jobs</option>
            {jobs.map((j) => (
              <option key={j.job_id} value={j.job_id}>
                {j.job_id} · {j.status}
              </option>
            ))}
          </select>
        </div>

        {/* Site filter. Object names are laid out as SiteName/JobID/ItemID, so
            the site is the first path segment and the server can list just that
            prefix instead of us filtering an already-downloaded page. */}
        <SelectFilter
          label="Site"
          value={siteFilter}
          onChange={setSiteFilter}
          className="w-52"
          allLabel="All sites"
          options={[
            { value: "", label: "All sites" },
            ...sites.map((site) => ({ value: site, label: siteLabel(site) })),
          ]}
        />
      </div>

      {error && (
        <div className="mb-6">
          <ErrorBanner message={error} onRetry={() => void load()} />
        </div>
      )}

      {loading && !data ? (
        <LoadingBlock label={`Listing bucket "${tab}"...`} />
      ) : data ? (
        <div className="card overflow-hidden">
          <div className="flex flex-wrap items-center justify-between gap-2 px-5 py-4">
            <h2 className="flex items-center gap-2 text-sm font-semibold text-slate-200">
              <HardDrive className="h-4 w-4 text-sky-400" />
              <span className="font-mono">{data.bucket}</span>
            </h2>
            <p className="text-xs text-slate-500">
              {/* `total` is null while more pages remain, so this reports the
                  window on screen rather than claiming an exact count. */}
              {data.total === null
                ? `Showing ${formatNumber(items.length)}+ objects`
                : `${formatNumber(data.total)} object${data.total === 1 ? "" : "s"}`}
              {totalBytes > 0 && <> · {formatBytes(totalBytes)}</>}
            </p>
          </div>

          {items.length === 0 ? (
            <EmptyState
              title={
                jobFilter !== "all"
                  ? `No objects from ${jobFilter}`
                  : prefix
                    ? `No objects under "${prefix}"`
                    : "Bucket is empty"
              }
              hint={
                tab === "exports"
                  ? "Export files appear here after the exporter consumer processes parsed items."
                  : "Objects will appear here as the pipeline processes crawl jobs."
              }
              icon={<Archive className="h-8 w-8" />}
            />
          ) : (
            <>
              <div className="flex flex-wrap items-center gap-3 border-y border-slate-800/70 bg-slate-950/40 px-5 py-3">
                <label className="flex cursor-pointer items-center gap-2 text-xs font-medium text-slate-300">
                  <input
                    ref={selectAllRef}
                    type="checkbox"
                    checked={allSelected}
                    onChange={toggleSelectAll}
                    className="h-4 w-4 accent-sky-500"
                  />
                  <span className="hidden sm:inline">Select all</span>
                </label>
                <span className="text-[11px] text-slate-500">
                  {selected.size > 0
                    ? `${selected.size} of ${pagedItems.length} selected`
                    : `${pagedItems.length} visible`}
                </span>
                <div className="ml-auto flex flex-wrap items-center gap-2">
                  {canConvert && (
                    <select
                      value={batchFormat}
                      onChange={(e) => setBatchFormat(e.target.value as ExportFormat)}
                      className="cursor-pointer rounded-md border border-slate-800 bg-slate-900/80 px-2 py-1.5 text-xs font-medium text-slate-300 focus:border-sky-500/50 focus:outline-none"
                      aria-label="Download format"
                      title="Format for downloaded files"
                    >
                      {FORMATS_BY_TAB[tab].map((f) => (
                        <option key={f} value={f}>
                          as {f.toUpperCase()}
                        </option>
                      ))}
                    </select>
                  )}
                  <button
                    type="button"
                    onClick={() => void downloadSelected()}
                    disabled={selected.size === 0 || batchBusy}
                    title={
                      selected.size === 0
                        ? "Select items to download"
                        : `Download ${selected.size} item${selected.size === 1 ? "" : "s"}${canConvert ? ` as ${batchFormat.toUpperCase()}` : ""}`
                    }
                    className="inline-flex cursor-pointer items-center gap-1.5 rounded-md border border-emerald-500/40 bg-emerald-500/10 px-3 py-1.5 text-xs font-medium text-emerald-500 transition hover:bg-emerald-500/20 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {batchBusy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Download className="h-3.5 w-3.5" />}
                    Download{selected.size > 0 ? ` (${selected.size})` : ""}
                  </button>
                </div>
              </div>
              {batchError && (
                <div className="border-b border-rose-500/20 px-5 py-2">
                  <Msg tone="error">{batchError}</Msg>
                </div>
              )}
              <div className="table-scroll">
                <table className="w-full min-w-[720px]">
                  <thead className="bg-slate-900/40">
                    <tr>
                      <th className="w-10 px-4 py-3">
                        <Square className="h-3.5 w-3.5 text-slate-600" />
                      </th>
                      <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                        Object
                      </th>
                      <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                        Size
                      </th>
                      <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">
                        Last modified
                      </th>
                      <th className="w-28 px-5 py-3" />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/60">
                    {pagedItems.map((item) => (
                      <tr
                        key={item.object_name}
                        className={cn(
                          "transition hover:bg-slate-800/30",
                          selected.has(item.object_name) && "bg-sky-500/5",
                        )}
                      >
                        <td className="px-4 py-3">
                          <input
                            type="checkbox"
                            checked={selected.has(item.object_name)}
                            onChange={() => toggleOne(item.object_name)}
                            aria-label={`Select ${item.object_name}`}
                            className="h-4 w-4 accent-sky-500"
                          />
                        </td>
                        <td data-label="Object" className="max-w-md px-5 py-3">
                          <p
                            className="truncate font-mono text-xs text-slate-300"
                            title={item.object_name}
                          >
                            {item.object_name}
                          </p>
                        </td>
                        <td data-label="Size" className="px-5 py-3 text-xs whitespace-nowrap text-slate-400">
                          {formatBytes(item.size_bytes)}
                        </td>
                        <td data-label="Last modified" className="px-5 py-3 text-xs whitespace-nowrap text-slate-500">
                          {formatDateTime(item.last_modified)}
                        </td>
                        <td className="px-5 py-3">
                          <div className="flex items-center justify-end gap-1.5">
                            {canConvert && (
                              <button
                                type="button"
                                onClick={() => void openObject(item.object_name)}
                                title="Open object"
                                aria-label={`Open ${item.object_name}`}
                                className="inline-flex cursor-pointer items-center gap-1.5 rounded-md border border-sky-500/40 bg-sky-500/10 px-2 py-1 text-[11px] font-medium text-sky-500 transition hover:bg-sky-500/20"
                              >
                                <Eye className="h-3 w-3" />
                                Open
                              </button>
                            )}
                            <CopyButton value={item.object_name} label="" />
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-800/70 bg-slate-950/40 px-5 py-3">
                <span className="text-xs text-slate-500">
                  {items.length === 0
                    ? "No objects"
                    : `Page ${cursorStack.length + 1} · ${formatNumber(items.length)} rows`}
                  {data.has_more && " · more available"}
                </span>
                <div className="flex items-center gap-2">
                  <select
                    value={pageSize}
                    onChange={(e) => setPageSize(Number(e.target.value))}
                    className="cursor-pointer rounded-md border border-slate-800 bg-slate-900/80 px-2 py-1.5 text-xs font-medium text-slate-300 focus:border-sky-500/50 focus:outline-none"
                    aria-label="Rows per page"
                  >
                    {PAGE_SIZES.map((s) => (
                      <option key={s} value={s}>
                        {s} / page
                      </option>
                    ))}
                  </select>
                  <button
                    type="button"
                    onClick={prevPage}
                    disabled={cursorStack.length === 0}
                    aria-label="Previous page"
                    title="Previous page"
                    className="inline-flex cursor-pointer items-center rounded-md border border-slate-800 bg-slate-900/80 px-2 py-1.5 text-xs text-slate-300 transition hover:border-sky-500/40 hover:text-sky-400 disabled:cursor-not-allowed disabled:opacity-40"
                  >
                    <ChevronLeft className="h-3.5 w-3.5" />
                  </button>
                  <span className="font-mono text-xs whitespace-nowrap text-slate-400">
                    {cursorStack.length + 1}
                  </span>
                  <button
                    type="button"
                    onClick={nextPage}
                    disabled={!data.next_cursor}
                    aria-label="Next page"
                    title="Next page"
                    className="inline-flex cursor-pointer items-center rounded-md border border-slate-800 bg-slate-900/80 px-2 py-1.5 text-xs text-slate-300 transition hover:border-sky-500/40 hover:text-sky-400 disabled:cursor-not-allowed disabled:opacity-40"
                  >
                    <ChevronRight className="h-3.5 w-3.5" />
                  </button>
                </div>
              </div>
            </>
          )}
        </div>
      ) : null}

      <Modal
        open={viewer !== null}
        onClose={() => {
          setViewer(null)
          setViewError(null)
        }}
        title={viewer?.name ?? ""}
      >
        {viewError ? (
          <ErrorBanner message={viewError} />
        ) : viewLoading || !viewer ? (
          <div className="flex items-center justify-center gap-3 py-10 text-slate-400">
            <Loader2 className="h-5 w-5 animate-spin" />
            <span className="text-sm">Loading object...</span>
          </div>
        ) : (
          <>
            <div className="mb-3 flex items-center justify-between gap-2">
              <span className="text-xs text-slate-500">
                {formatBytes(new Blob([viewer.content]).size)} · JSON/text preview
              </span>
              <a
                href={api.getStorageObjectUrl(tab, viewer.name)}
                download={viewer.name}
                className="btn-primary px-3 py-1.5 text-xs"
              >
                <Download className="h-3.5 w-3.5" />
                Download
              </a>
            </div>
            <pre className="max-h-[60vh] overflow-auto rounded-lg border border-slate-800 bg-slate-950/80 p-4 font-mono text-[11px] leading-relaxed break-all whitespace-pre-wrap text-slate-300">
              {viewer.content}
            </pre>
          </>
        )}
      </Modal>
    </div>
  )
}