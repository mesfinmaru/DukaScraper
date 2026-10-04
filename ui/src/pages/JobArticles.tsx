import { useCallback, useEffect, useState } from "react"
import { useParams, useNavigate, useSearchParams } from "react-router-dom"
import { ArrowLeft, Download, Eye, Search, ShieldCheck } from "lucide-react"
import { api, ApiError } from "../api"
import { cn, formatDateTime } from "../utils"
import {
  EmptyState,
  ErrorBanner,
  LoadingBlock,
  Modal,
  PageHeader,
} from "../components/ui"
import { useAutoRefresh } from "../useAutoRefresh"

type TabKey = "metadata" | "content"

/** Object name for an s3://bucket/prefix/key path (used as a download fallback). */
function storageObjectName(path: string | null | undefined): string {
  if (!path) return ""
  if (path.startsWith("s3://")) return path.replace(/^s3:\/\/[^/]+\//, "")
  return path.split("/").pop() ?? ""
}

type JobSummary = {
  job_id: string
  total: number
  limit?: number
  offset?: number
  has_more?: boolean
  items: Array<{
    item_id: string
    source_url: string
    language: string
    title: string | null
    publish_date: string | null
    character_count: number | null
    word_count: number | null
    parsed_at: string | null
    is_exported: boolean
    intelligence_processed: boolean
    parsed_json_path: string | null
    raw_html_path: string | null
    parsed_object: string | null
    raw_object: string | null
    parsed_content: {
      extracted_text: string
      character_count: number | null
      title: string | null
      publish_date: string | null
      detected_language: string | null
      sections: Array<{ heading: string; level: number; text: string }>
      content_quality_score: number | null
      structure_valid: boolean | null
    } | null
    raw_html: string | null
  }>
}

export default function JobArticles() {
  const { jobId } = useParams<{ jobId: string }>()
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const [summary, setSummary] = useState<JobSummary | null>(null)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [pageCount, setPageCount] = useState(1)
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<TabKey>("content")
  const [viewer, setViewer] = useState<{ item: JobSummary["items"][0]; type: "parsed" | "raw" } | null>(null)

  const filterItemId = searchParams.get("itemId")

  const PAGE_SIZE = 10

  const load = useCallback(async () => {
    if (!jobId) return
    setLoading(true)
    try {
      // Re-request everything the user has already asked to see, rather than
      // just the first page: this list is polled for live updates, and
      // fetching offset 0 only would throw away the pages they loaded.
      const res = await api.getJobSummary(jobId, {
        limit: PAGE_SIZE * pageCount,
        offset: 0,
      })
      setSummary(res)
      setError(null)
    } catch (e) {
      setError(e instanceof ApiError ? e.message : "Could not load the results.")
      setSummary(null)
    } finally {
      setLoading(false)
    }
  }, [jobId, pageCount])

  // The summary endpoint is paged server-side: it embeds parsed text per item,
  // so an unpaged call returned megabytes and the list appeared truncated.
  const loadMore = useCallback(() => {
    if (loadingMore) return
    setLoadingMore(true)
    setPageCount((p) => p + 1)
  }, [loadingMore])

  useEffect(() => {
    if (!loadingMore) setLoadingMore(false)
  }, [summary, loadingMore])

  useEffect(() => {
    void load()
  }, [load])

  // New items appear while a job runs; poll so the list stays current.
  useAutoRefresh({ load })

  const openViewer = (item: JobSummary["items"][0], type: "parsed" | "raw") => {
    setViewer({ item, type })
  }

  const filteredItems = filterItemId
    ? summary?.items.filter((item) => item.item_id === filterItemId) ?? []
    : summary?.items ?? []

  if (!jobId) return null

  return (
    <div>
      <PageHeader
        title="Job Articles"
        description={summary ? (filterItemId ? `Viewing ${filterItemId} from ${summary.total} item(s)` : `Parsed content for ${summary.total} item(s)`) : "Loading..."}
        actions={
          <>
            {filterItemId && (
              <button
                type="button"
                className="btn-secondary"
                onClick={() => {
                  setSearchParams({})
                }}
              >
                Show all items
              </button>
            )}
            <button type="button" className="btn-secondary" onClick={() => navigate("/jobs")}>
              <ArrowLeft className="h-4 w-4" />
              Back to jobs
            </button>
          </>
        }
      />

      {error && (
        <div className="mb-6">
          <ErrorBanner message={error} onRetry={() => void load()} />
        </div>
      )}

      {loading && !summary ? (
        <LoadingBlock label="Fetching parsed articles from MinIO..." />
      ) : summary ? (
        <>
          <div className="mb-4 flex gap-2 border-b border-slate-800">
            <button
              type="button"
              onClick={() => setActiveTab("content")}
              className={cn(
                "px-4 py-2 text-sm font-medium border-b-2 transition",
                activeTab === "content"
                  ? "border-sky-500 text-sky-400"
                  : "border-transparent text-slate-500 hover:text-slate-300",
              )}
            >
              Parsed Content ({filteredItems.length})
            </button>
            <button
              type="button"
              onClick={() => setActiveTab("metadata")}
              className={cn(
                "px-4 py-2 text-sm font-medium border-b-2 transition",
                activeTab === "metadata"
                  ? "border-sky-500 text-sky-400"
                  : "border-transparent text-slate-500 hover:text-slate-300",
              )}
            >
              Metadata
            </button>
          </div>

          {activeTab === "content" && filteredItems.length > 0 && (
            <div className="space-y-4">
              {filteredItems.map((item) => (
                <div key={item.item_id} className="card overflow-hidden">
                  <div className="border-b border-slate-800 px-5 py-4 bg-slate-900/40">
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div>
                        <div className="flex items-center gap-2">
                          <span className="font-mono text-sm text-sky-600">{item.item_id}</span>
                          {item.intelligence_processed && (
                            <ShieldCheck className="h-3.5 w-3.5 text-violet-500" aria-label="LLM analyzed" />
                          )}
                          {item.is_exported && (
                            <span className="rounded-full border border-emerald-500/40 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium text-emerald-500">
                              Exported
                            </span>
                          )}
                        </div>
                        <p className="mt-1 truncate max-w-2xl font-mono text-xs text-slate-500">{item.source_url}</p>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-slate-500 whitespace-nowrap">{item.character_count?.toLocaleString()} chars</span>
                        <button
                          type="button"
                          onClick={() => openViewer(item, "parsed")}
                          className="btn-secondary px-2 py-1 text-xs"
                          aria-label="View parsed content"
                        >
                          <Eye className="h-3 w-3" />
                          View
                        </button>
                        {(item.raw_object || item.raw_html) && (
                          <button
                            type="button"
                            onClick={() => openViewer(item, "raw")}
                            className="btn-secondary px-2 py-1 text-xs"
                            aria-label="View raw HTML"
                          >
                            <Eye className="h-3 w-3" />
                            Raw HTML
                          </button>
                        )}
                      </div>
                    </div>
                  </div>

                  <div className="p-5">
                    <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                      <div className="md:col-span-2">
                        <h4 className="mb-2 font-medium text-slate-200">{item.title || "Untitled"}</h4>
                        <div className="flex flex-wrap items-center gap-3 text-xs text-slate-500">
                          <span>{item.language.toUpperCase()}</span>
                          <span>{item.publish_date || "Unknown date"}</span>
                          <span>{item.word_count?.toLocaleString()} words</span>
                          <span>{formatDateTime(item.parsed_at)}</span>
                        </div>
                      </div>
                      <div className="flex flex-col items-end gap-1">
                        {Number.isFinite(item.parsed_content?.content_quality_score) && (
                          <div className="w-40">
                            <div className="mb-1 flex justify-between text-[10px] text-slate-500">
                              <span>Quality</span>
                              <span className="font-mono text-slate-300">
                                {((item.parsed_content!.content_quality_score as number) * 100).toFixed(1)}%
                              </span>
                            </div>
                            <div className="h-1.5 overflow-hidden rounded-full bg-slate-800">
                              <div
                                className={cn(
                                  "h-full rounded-full transition-all duration-500",
                                  (item.parsed_content!.content_quality_score as number) > 0.7 ? "bg-emerald-500" :
                                  (item.parsed_content!.content_quality_score as number) > 0.3 ? "bg-amber-500" : "bg-rose-500"
                                )}
                                style={{ width: `${Math.min(100, Math.max(0, (item.parsed_content!.content_quality_score as number) * 100))}%` }}
                              />
                            </div>
                          </div>
                        )}
                      </div>
                    </div>

                    {item.parsed_content?.extracted_text && (
                      <div className="mt-4">
                        <h5 className="mb-2 text-xs font-semibold tracking-wider text-slate-500 uppercase">Extracted Text</h5>
                        <div className="max-h-96 overflow-auto rounded-lg border border-slate-800 bg-slate-950/80 p-4 font-mono text-[11px] leading-relaxed whitespace-pre-wrap text-slate-300">
                          {item.parsed_content.extracted_text}
                        </div>
                      </div>
                    )}

                    {item.parsed_content?.sections && item.parsed_content.sections.length > 0 && (
                      <div className="mt-4">
                        <h5 className="mb-2 text-xs font-semibold tracking-wider text-slate-500 uppercase">Sections ({item.parsed_content.sections.length})</h5>
                        <div className="space-y-2">
                          {item.parsed_content.sections.map((section, si) => (
                            <div key={si} className="rounded-lg border border-slate-800 bg-slate-950/60 p-3">
                              <h6 className="mb-1 font-semibold text-sm text-slate-200">
                                {section.heading}
                                <span className="ml-2 text-[10px] font-normal text-slate-500">(H{section.level})</span>
                              </h6>
                              <p className="text-xs text-slate-300 line-clamp-3">{section.text}</p>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}

          {activeTab === "metadata" && (
            <div className="card table-scroll">
              <table className="w-full min-w-[800px]">
                <thead className="bg-slate-900/40">
                  <tr>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Item ID</th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">URL</th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Lang</th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Title</th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Published</th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Chars / Words</th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Parsed At</th>
                    <th className="px-5 py-3 text-left text-xs font-semibold tracking-wider text-slate-500 uppercase">Status</th>
                    <th className="w-24 px-5 py-3" />
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60">
                  {filteredItems.map((item) => (
                    <tr key={item.item_id} className="hover:bg-slate-800/30">
                      <td data-label="Item ID" className="px-5 py-3 font-mono text-xs text-sky-600">{item.item_id}</td>
                      <td data-label="URL" className="max-w-48 truncate px-5 py-3 font-mono text-xs text-slate-400" title={item.source_url}>
                        {item.source_url}
                      </td>
                      <td data-label="Lang" className="px-5 py-3 text-xs text-slate-400">{item.language.toUpperCase()}</td>
                      <td data-label="Title" className="max-w-40 truncate px-5 py-3 text-sm text-slate-300" title={item.title || ""}>
                        {item.title || "—"}
                      </td>
                      <td data-label="Published" className="px-5 py-3 text-xs whitespace-nowrap text-slate-500">{item.publish_date || "—"}</td>
                      <td data-label="Chars / Words" className="px-5 py-3 text-xs text-slate-400">
                        {item.character_count?.toLocaleString()} / {item.word_count?.toLocaleString()}
                      </td>
                      <td data-label="Parsed At" className="px-5 py-3 text-xs whitespace-nowrap text-slate-500">{formatDateTime(item.parsed_at)}</td>
                      <td data-label="Status" className="px-5 py-3">
                        <div className="flex flex-wrap items-center gap-1.5">
                          {item.intelligence_processed && (
                            <span className="inline-flex items-center gap-1 rounded-full border border-violet-500/40 bg-violet-500/10 px-2 py-0.5 text-[10px] font-medium text-violet-500">
                              <ShieldCheck className="h-2.5 w-2.5" />
                              LLM
                            </span>
                          )}
                          {item.is_exported && (
                            <span className="inline-flex items-center gap-1 rounded-full border border-emerald-500/40 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium text-emerald-500">
                              Exported
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-5 py-3">
                        <button
                          type="button"
                          onClick={() => openViewer(item, "parsed")}
                          className="inline-flex cursor-pointer items-center gap-1.5 rounded-md border border-sky-500/40 bg-sky-500/10 px-2 py-1 text-[11px] font-medium text-sky-500 transition hover:bg-sky-500/20"
                        >
                          <Eye className="h-3 w-3" />
                          View
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {filteredItems.length === 0 && (
            <EmptyState
              title={filterItemId ? `No item ${filterItemId} found` : "No parsed items for this job"}
              hint={filterItemId ? "The requested item may not have parsed content yet." : "The parser worker may not have processed this job yet."}
              icon={<Search className="h-8 w-8" />}
            />
          )}

          {summary?.has_more && (
            <div className="mt-4 flex justify-center">
              <button
                type="button"
                onClick={() => void loadMore()}
                disabled={loadingMore}
                className="btn-secondary px-3 py-1.5 text-xs disabled:opacity-60"
              >
                {loadingMore
                  ? "Loading..."
                  : `Load more (showing ${summary.items.length} of ${summary.total})`}
              </button>
            </div>
          )}
        </>
      ) : null}

      <Modal
        open={viewer !== null}
        onClose={() => setViewer(null)}
        title={viewer?.item.item_id ?? ""}
      >
        {viewer ? (
          <div className="max-h-[70vh] overflow-auto">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <span className="font-mono text-xs text-sky-600">{viewer.item.item_id}</span>
                <span className="rounded-full border border-slate-700 bg-slate-900 px-1.5 py-0.5 text-[10px] font-medium text-slate-400 uppercase">
                  {viewer.type}
                </span>
              </div>
              <a
                href={
                  viewer.type === "parsed"
                    ? api.getStorageObjectUrl(
                        "parsed",
                        viewer.item.parsed_object ?? storageObjectName(viewer.item.parsed_json_path),
                      )
                    : api.getStorageObjectUrl(
                        "raw",
                        viewer.item.raw_object ?? storageObjectName(viewer.item.raw_html_path),
                      )
                }
                download
                className="btn-primary px-3 py-1.5 text-xs"
              >
                <Download className="h-3.5 w-3.5" />
                Download
              </a>
            </div>
            <pre className="max-h-[60vh] overflow-auto rounded-lg border border-slate-800 bg-slate-950/80 p-4 font-mono text-[11px] leading-relaxed break-all whitespace-pre-wrap text-slate-300">
              {viewer.type === "parsed" && viewer.item.parsed_content
                ? JSON.stringify(viewer.item.parsed_content, null, 2)
                : viewer.item.raw_html ||
                  "Raw HTML is not loaded into the page to keep it fast — use Download above to stream the original file."}
            </pre>
          </div>
        ) : null}
      </Modal>
    </div>
  )
}