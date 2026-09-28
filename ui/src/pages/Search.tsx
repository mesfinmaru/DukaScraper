import { useState } from "react"
import type { FormEvent } from "react"
import { Link } from "react-router-dom"
import { Braces, ExternalLink, FileSearch, Loader2, Search as SearchIcon, Sparkles } from "lucide-react"
import { api, ApiError } from "../api"
import type { SearchApiResponse, SearchDoc, SemanticSearchResponse } from "../types"
import { cn, formatDateTime, formatNumber, highlightText, languageLabel } from "../utils"
import { EmptyState, ErrorBanner, LoadingBlock, PageHeader } from "../components/ui"

function str(doc: SearchDoc, ...keys: string[]): string | null {
  for (const key of keys) {
    const v = doc[key]
    if (typeof v === "string" && v.trim()) return v
  }
  return null
}

function num(doc: SearchDoc, key: string): number | null {
  const v = doc[key]
  return typeof v === "number" ? v : null
}

/** Picks a short excerpt of long text around the first token match. */
function excerpt(text: string, tokens: string[]): { text: string; truncated: boolean } {
  const lower = text.toLowerCase()
  let idx = -1
  for (const t of tokens) {
    const found = lower.indexOf(t.toLowerCase())
    if (found !== -1 && (idx === -1 || found < idx)) idx = found
  }
  if (idx === -1) {
    return {
      text: text.length > 400 ? `${text.slice(0, 400)}` : text,
      truncated: text.length > 400,
    }
  }
  const start = Math.max(0, idx - 140)
  const end = Math.min(text.length, idx + 360)
  return { text: text.slice(start, end), truncated: start > 0 || end < text.length }
}

function ResultCard({ doc, tokens }: { doc: SearchDoc; tokens: string[] }) {
  const url = str(doc, "url", "source_url")
  const title = str(doc, "title") ?? url ?? "(untitled)"
  const body = str(doc, "extracted_text", "clean_text", "text") ?? ""
  const language = str(doc, "language")
  const domain = str(doc, "source_domain", "domain")
  const worker = str(doc, "worker")
  const itemId = str(doc, "item_id")
  const jobId = str(doc, "job_id")
  const chars = num(doc, "character_count")
  const createdAt = str(doc, "created_at")

  const snippet = excerpt(body, tokens)

  return (
    <article className="card p-5 transition hover:border-slate-700">
      <div className="mb-1.5 flex items-start justify-between gap-4">
        <h3 className="text-base leading-snug font-semibold text-sky-200">
          {highlightText(title, tokens)}
        </h3>
        {url && (
          <a
            href={url}
            target="_blank"
            rel="noreferrer"
            title="Open source page"
            className="shrink-0 rounded-md border border-slate-700 bg-black p-1.5 text-slate-400 shadow-sm transition hover:border-indigo-500 hover:text-sky-500"
          >
            <ExternalLink className="h-3.5 w-3.5" />
          </a>
        )}
      </div>

      {domain && (
        <p className="mb-3 font-mono text-[11px] text-slate-500">{domain}</p>
      )}

      {snippet.text && (
        <p className="text-sm leading-relaxed break-words whitespace-pre-line text-slate-400">
          {snippet.truncated ? "... " : ""}
          {highlightText(snippet.text, tokens)}
          {snippet.truncated ? " ..." : ""}
        </p>
      )}

      <div className="mt-4 flex flex-wrap items-center gap-x-3 gap-y-1.5 border-t border-slate-800/70 pt-3 text-[11px] text-slate-500">
        {itemId && <span className="font-mono">{itemId}</span>}
        {jobId && (
          <Link
            to={`/jobs/${encodeURIComponent(jobId)}`}
            className="font-mono text-indigo-400 hover:text-indigo-300 hover:underline"
          >
            {jobId}
          </Link>
        )}
        {language && (
          <span className="rounded-full border border-slate-700 px-2 py-0.5 text-slate-400">
            {languageLabel(language)}
          </span>
        )}
        {worker && (
          <span className="rounded-full border border-slate-700 px-2 py-0.5 capitalize text-slate-400">
            {worker}
          </span>
        )}
        {chars !== null && <span>{formatNumber(chars)} chars</span>}
        {createdAt && <span>{formatDateTime(createdAt)}</span>}
      </div>
    </article>
  )
}

function SemanticResultCard({ result }: { result: SemanticSearchResponse["results"][number] }) {
  return (
    <article className="card p-5 transition hover:border-slate-700">
      <div className="mb-1.5 flex items-start justify-between gap-4">
        <h3 className="text-base leading-snug font-semibold text-sky-200">
          {result.summary || "(no summary)"}
        </h3>
        {result.url && (
          <a
            href={result.url}
            target="_blank"
            rel="noreferrer"
            title="Open source page"
            className="shrink-0 rounded-md border border-slate-700 bg-black p-1.5 text-slate-400 shadow-sm transition hover:border-indigo-500 hover:text-sky-500"
          >
            <ExternalLink className="h-3.5 w-3.5" />
          </a>
        )}
      </div>
      <div className="mt-4 flex flex-wrap items-center gap-x-3 gap-y-1.5 border-t border-slate-800/70 pt-3 text-[11px] text-slate-500">
        {result.item_id && <span className="font-mono">{result.item_id}</span>}
        {result.language && (
          <span className="rounded-full border border-slate-700 px-2 py-0.5 text-slate-400">
            {languageLabel(result.language)}
          </span>
        )}
        {result.category && result.category !== "other" && (
          <span className="rounded-full border border-violet-500/40 bg-violet-500/10 px-2 py-0.5 text-violet-300">
            {result.category.replace(/_/g, " ")}
          </span>
        )}
        <span className="rounded-full border border-emerald-500/40 bg-emerald-500/10 px-2 py-0.5 text-emerald-300">
          {(result.score * 100).toFixed(1)}% match
        </span>
      </div>
    </article>
  )
}

export default function Search() {
  const [input, setInput] = useState("")
  const [size, setSize] = useState(20)
  const [mode, setMode] = useState<"keyword" | "semantic">("keyword")
  const [query, setQuery] = useState<string | null>(null)
  const [data, setData] = useState<SearchApiResponse | null>(null)
  const [semantic, setSemantic] = useState<SemanticSearchResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const runSearch = async (e?: FormEvent) => {
    e?.preventDefault()
    const q = input.trim()
    if (!q) return
    setQuery(q)
    setLoading(true)
    setError(null)
    setData(null)
    setSemantic(null)
    try {
      if (mode === "semantic") {
        setSemantic(await api.semanticSearch(q, size))
      } else {
        setData(await api.searchArticles(q, size))
      }
    } catch (err) {
      if (err instanceof ApiError && err.status === 503 && mode === "semantic") {
        setError("Semantic search is unavailable - the embedding service or vector DB is down. Try keyword search.")
      } else {
        setError(err instanceof Error ? err.message : "Search failed")
      }
    } finally {
      setLoading(false)
    }
  }

  const tokens = (query ?? "").split(/\s+/).filter((t) => t.length >= 2)

  return (
    <div>
      <PageHeader title="Search" />

      <div className="mb-4 flex gap-2">
        <button
          type="button"
          onClick={() => setMode("keyword")}
          className={cn(
            "inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs font-medium transition",
            mode === "keyword"
              ? "border-indigo-500/50 bg-indigo-500/15 text-indigo-300"
              : "border-slate-800 bg-slate-950 text-slate-400 hover:text-slate-200",
          )}
        >
          <Braces className="h-3.5 w-3.5" />
          Keyword
        </button>
        <button
          type="button"
          onClick={() => setMode("semantic")}
          className={cn(
            "inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs font-medium transition",
            mode === "semantic"
              ? "border-violet-500/50 bg-violet-500/15 text-violet-300"
              : "border-slate-800 bg-slate-950 text-slate-400 hover:text-slate-200",
          )}
          title="Meaning-based matching over article embeddings (multilingual: Amharic + English)"
        >
          <Sparkles className="h-3.5 w-3.5" />
          Semantic
        </button>
      </div>

      <form onSubmit={(e) => void runSearch(e)} className="card mb-6 flex flex-col gap-3 p-4 sm:flex-row sm:items-center">
        <div className="relative flex-1">
          <SearchIcon className="pointer-events-none absolute top-1/2 left-3.5 h-4 w-4 -translate-y-1/2 text-slate-500" />
          <input
            className="input py-2.5 pl-10"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={
              mode === "semantic"
                ? "Describe what you are looking for, e.g. rising cost of living"
                : "Search articles, e.g. inflation"
            }
            spellCheck={false}
          />
        </div>
        <select
          aria-label="Result size"
          className="input sm:w-32"
          value={size}
          onChange={(e) => setSize(Number(e.target.value))}
        >
          {[10, 20, 50, 100].map((n) => (
            <option key={n} value={n}>
              {n} results
            </option>
          ))}
        </select>
        <button type="submit" className="btn-primary min-w-36" disabled={loading}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <SearchIcon className="h-4 w-4" />}
          Search
        </button>
      </form>

      {error && (
        <div className="mb-6">
          <ErrorBanner message={error} onRetry={() => void runSearch()} />
        </div>
      )}

      {!query && !error && (
        <div className="card">
          <EmptyState
            title="Start typing to search"
            hint={
              mode === "semantic"
                ? "Queries are embedded and matched against stored article vectors - finds content by meaning, not exact words."
                : "Queries run a multi-match against indexed URLs and extracted article text."
            }
            icon={<FileSearch className="h-8 w-8" />}
          />
        </div>
      )}

      {loading && query && <LoadingBlock label={`Searching for "${query}"...`} />}

      {data && !loading && (
        <>
          <p className={cn("mb-4 text-sm text-slate-400")}>
            <span className="font-semibold text-slate-200">{formatNumber(data.total)}</span>{" "}
            result{data.total === 1 ? "" : "s"} for{" "}
            <span className="font-mono text-indigo-600">&ldquo;{data.query}&rdquo;</span>
            {data.results.length < data.total &&
              ` (showing ${data.results.length})`}
          </p>
          {data.results.length === 0 ? (
            <div className="card">
              <EmptyState
                title="No matches found"
                hint="Try broader keywords - matches are looked up in URLs and extracted text."
                icon={<FileSearch className="h-8 w-8" />}
              />
            </div>
          ) : (
            <div className="space-y-4">
              {data.results.map((doc, i) => (
                <ResultCard key={i} doc={doc} tokens={tokens} />
              ))}
            </div>
          )}
        </>
      )}

      {semantic && !loading && (
        <>
          <p className={cn("mb-4 text-sm text-slate-400")}>
            <span className="font-semibold text-slate-200">{formatNumber(semantic.total)}</span>{" "}
            semantic match{semantic.total === 1 ? "" : "es"} for{" "}
            <span className="font-mono text-violet-400">&ldquo;{semantic.query}&rdquo;</span>
          </p>
          {semantic.results.length === 0 ? (
            <div className="card">
              <EmptyState
                title="No semantic matches"
                hint="No stored articles are close enough in meaning - content may not be analyzed yet."
                icon={<Sparkles className="h-8 w-8" />}
              />
            </div>
          ) : (
            <div className="space-y-4">
              {semantic.results.map((result, i) => (
                <SemanticResultCard key={`${result.item_id}-${i}`} result={result} />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  )
}
