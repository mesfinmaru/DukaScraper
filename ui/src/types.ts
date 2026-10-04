export type JobStatus =
  | "pending"
  | "running"
  /** Paused by the user. Resumable; in-flight pages were kept. */
  | "paused"
  | "completed"
  | "failed"
  | "skipped"
  | "needs_review"

export type WorkerType = "surface" | "deep" | "dark"

export interface RecursiveConfig {
  enable_extraction?: boolean
  link_filter_patterns?: string[]
  skip_domains?: string[]
  [key: string]: unknown
}

export interface ScrapeRequest {
  url: string
  user_id: string
  language: string
  worker_override?: WorkerType | null
  max_depth?: number
  recursive_config?: RecursiveConfig
  job_params?: Record<string, unknown>
  /** Advisory hint for the kind of content sought. Defaults to "all". */
  datatype?: CrawlDatatype
}

export interface BatchScrapeRequest {
  urls: string[]
  user_id: string
  language: string
  worker_override?: WorkerType | null
  max_depth?: number
  recursive_config?: RecursiveConfig
  allow_login: boolean
  allow_signup: boolean
  allow_email_verification: boolean
  /** Advisory hint for the kind of content sought. Defaults to "all". */
  datatype?: CrawlDatatype
  credential_email?: string | null
  job_params?: Record<string, unknown>
}

export interface BatchScrapeResponse {
  total: number
  jobs: TriggerJobResponse[]
}

export interface TriggerJobResponse {
  message: string
  job_id: string
  assigned_worker: string
  assignment_reason: string
  max_depth: number
  kafka_topic: string
}

/**
 * Search networks for topic discovery.
 *
 * "all" searches surface, deep and dark together and merges the seeds. It is
 * not a crawler choice - discovered seeds are routed to workers by the normal
 * assignment engine, exactly like hand-entered URLs.
 */
export type DiscoveryNetwork = "surface" | "deep" | "dark" | "all"

/**
 * Advisory hint for the kind of content a crawl is after. Advisory because the
 * worker still fetches whatever a URL actually returns; a page that redirects
 * to a PDF is captured either way.
 */
export type CrawlDatatype = "all" | "html" | "pdf" | "document" | "audio"

/**
 * Topic-based discovery: start a crawl from a query instead of a URL.
 * The discovery-worker resolves it into seed URLs and emits ordinary
 * CrawlRequests, so the result is indistinguishable from a URL crawl.
 */
export interface DiscoverRequest {
  query: string
  network: DiscoveryNetwork
  user_id?: string | null
  language?: string
  max_results?: number
  engines?: string[] | null
  max_depth?: number
  recursive_config?: RecursiveConfig
  job_params?: Record<string, unknown>
  /** Advisory hint for the kind of content sought. Defaults to "all". */
  datatype?: CrawlDatatype
}

export interface DiscoverResponse {
  message: string
  job_id: string
  network: string
  query: string
  max_results: number
  kafka_topic: string
}

export interface CredentialSummary {
  email: string
  display_name?: string | null
  provider: string
  status: string
}

export interface JobSummary {
  job_id: string
  url: string
  /** Host the job crawls (e.g. "bbc.com"). Derived server-side from `url`. */
  site_name?: string
  status: JobStatus
  created_at: string | null
  user_id?: string
  assignment_reason?: string | null
}

export interface UserInfo {
  user_id: string
  username: string
  full_name: string
  role: "admin" | "user"
  must_change_password: boolean
}

export interface CreateUserResponse {
  message: string
  user: UserInfo
}

export interface VerifyEmailResponse {
  message: string
  email: string
  expires_in_seconds: number
  expires_at: string
}

export interface ConfirmEmailCodeResponse {
  message: string
  email: string
  token: string
  expires_in_seconds: number
  expires_at: string
}

export interface LoginResponse {
  access_token: string
  refresh_token: string
  token_type: string
  user: UserInfo
}

/** Login of an unverified (admin-provisioned) account — no tokens yet. */
export interface RequiresVerificationResponse {
  status: "REQUIRES_VERIFICATION"
  user_id: string
  redirect_to: string
  masked_email: string
  expires_in_seconds: number
}

export type LoginResult = LoginResponse | RequiresVerificationResponse

export function isRequiresVerification(
  res: LoginResult,
): res is RequiresVerificationResponse {
  return (res as RequiresVerificationResponse).status === "REQUIRES_VERIFICATION"
}

export interface VerifyOtpResponse {
  access_token: string
  refresh_token: string
  token_type: string
  user: UserInfo
}

export interface SiteCredentialStatus {
  exists: boolean
  email?: string
  username?: string
  source?: string
}

export interface ChangePasswordResponse {
  message: string
  must_change_password: boolean
}

export interface ForgotPasswordResponse {
  message: string
}

export interface ResetPasswordResponse {
  message: string
}

export interface AdminUserRow extends UserInfo {
  email: string
  is_email_verified: boolean
  is_active: boolean
  created_at: string | null
}

export interface UsersListResponse {
  total: number
  users: AdminUserRow[]
}

export interface UserJobsResponse {
  user_id: string
  total: number
  jobs: JobSummary[]
}

export interface JobDetail {
  job_id: string
  user_id: string
  site_name?: string
  url: string
  language: string
  status: JobStatus
  failure_reason: string | null
  created_at: string | null
  completed_at: string | null
}

export interface ArticleItem {
  item_id: string
  source_url: string
  language: string
  title: string | null
  publish_date: string | null
  character_count: number | null
  word_count: number | null
  raw_html_path: string
  parsed_json_path: string
  parsed_at: string | null
  is_exported: boolean
  intelligence_processed: boolean
}

export interface ArticleItemDetail extends ArticleItem {
  job_id: string
  /**
   * First slice of the extracted text, so expanding a row shows what was
   * actually captured. Empty when the parsed object is missing or unreadable.
   */
  text_sample: string
  /** True when the real text is longer than the sample. */
  text_sample_truncated: boolean
}

export interface JobArticlesResponse {
  job_id: string
  total: number
  items: ArticleItem[]
}

export interface SearchDoc {
  [key: string]: unknown
}

export interface SearchApiResponse {
  query: string
  total: number
  results: SearchDoc[]
}

export interface BucketOverviewResponse {
  buckets: Record<string, string>
}

export interface StorageObject {
  object_name: string
  size_bytes: number | null
  last_modified: string | null
  /** Owning job, parsed out of the object name. Null when it cannot be resolved. */
  job_id?: string | null
  /**
   * Site label: the first path segment of folder-style names
   * (`example.com/JOB.../ITEM...`). Empty for legacy flat names, which have no
   * folder - those fall back to showing the job instead.
   */
  site?: string
}

export interface BucketItemsResponse {
  bucket: string
  /** Exact count only when this page reached the end; null while more remain. */
  total: number | null
  limit: number
  offset: number
  has_more: boolean
  /** Key to pass as `after` for the next page; null at the end of the bucket. */
  next_cursor: string | null
  items: StorageObject[]
}

export interface MetricGroup {
  samples: number
  accuracy: number | null
  macro_f1: number | null
}

export interface MetricsResponse {
  source_type: MetricGroup
  topic: MetricGroup
  category: MetricGroup
  /**
   * How much of the corpus was actually reviewed. Accuracy from a handful of
   * labels is not comparable to accuracy from a thousand, so the UI shows this
   * next to the score instead of letting the bare percentage imply certainty.
   */
  coverage: {
    labeled_items: number
    total_analyzed_items: number
  }
  note: string
}

export interface EvaluationRequest {
  item_id: string
  job_id: string
  evaluated_by: string
  expected_source_type: string
  expected_topic: string
  expected_category: string
}

export interface EvaluationResponse {
  evaluation_id: string
  item_id: string
  job_id: string
}

/**
 * A high-severity (4 or 5) intelligence finding surfaced to operators.
 *
 * `analysis_source` is carried all the way to this screen on purpose: an alert
 * labelled by the heuristic fallback looks identical to a model verdict unless
 * the UI says otherwise, and a threat feed that cannot be trusted is worse than
 * no feed.
 */
export interface AlertRow {
  alert_id: string
  job_id: string
  item_id: string
  url: string
  title: string
  category: string
  severity: number
  language: string
  summary: string
  entities: string[]
  analysis_source: string
  llm_model: string
  created_at: string | null
  read: boolean
  priority: "critical" | "high" | "low"
  /** Whitespace-collapsed, length-capped summary for dense list rows. */
  short_summary: string
}

export interface AlertsResponse {
  alerts: AlertRow[]
  total: number
  limit: number
  offset: number
  has_more: boolean
  unread_only: boolean
}

export interface UnreadAlertsResponse {
  unread: number
}

/**
 * Short-lived token that authorises the monitoring embed proxy.
 *
 * An <iframe> cannot send an Authorization header, so the embed token rides in
 * the query string instead. It grants read access to Grafana/Prometheus only and
 * expires on its own.
 */
export interface EmbedTokenResponse {
  token: string
  expires_in: number
}

export interface ThreatRecentRow {
  item_id: string
  job_id: string
  url: string
  source_type: string
  category: string
  severity: number
  summary: string
  created_at: string
}

export interface ThreatAnalyticsResponse {
  total: number
  by_severity: { severity: number; count: number }[]
  by_category: { category: string; count: number }[]
  /** Distinct values present, for the filter dropdowns. */
  categories: string[]
  sources: string[]
  limit: number
  offset: number
  /** True when rows remain after this page. */
  has_more: boolean
  recent: ThreatRecentRow[]
}

/** Query options for the flagged-content list. */
export interface ThreatQuery {
  offset?: number
  limit?: number
  category?: string
  severity?: number
  source_type?: string
}

export interface EntitySummaryRow {
  entity: string
  entity_type: string
  occurrences: number
  sample_urls: string[]
}

export interface EntitySummaryResponse {
  total: number
  entities: EntitySummaryRow[]
}

export interface EntityMentionRow {
  item_id: string
  job_id: string
  url: string
  entity: string
  category: string
  language: string
  severity: number
  created_at: string
}

export interface EntityMentionsResponse {
  entity: string
  total: number
  mentions: EntityMentionRow[]
}

export interface SemanticSearchResult {
  item_id: string
  url: string
  language: string
  category: string
  summary: string
  score: number
}

export interface SemanticSearchResponse {
  query: string
  total: number
  results: SemanticSearchResult[]
}

export interface WorkerPerformanceRow {
  worker: string
  requests: number
  avg_latency_ms: number | null
  p95_latency_ms: number | null
  max_latency_ms: number | null
  retries: number
  error_rate: number | null
  avg_payload_bytes: number | null
}

export interface PerformanceResponse {
  workers: WorkerPerformanceRow[]
  overall: {
    requests: number
    avg_latency_ms: number | null
    p95_latency_ms: number | null
    error_rate: number | null
  }
  recent_attempts: {
    job_id: string
    worker: string
    status_code: number
    latency_ms: number
    retry_count: number
    payload_size_bytes: number
    created_at: string
  }[]
}

export interface ServiceHealth {
  name: string
  status: "healthy" | "unhealthy" | "disconnected"
  url: string
}

export interface MonitoringHealthResponse {
  total: number
  healthy: number
  unhealthy: number
  services: ServiceHealth[]
  checked_at: number
}

export interface MonitoringUrlsResponse {
  grafana: string
  prometheus: string
  kafka_ui: string
  kibana: string
  pgadmin: string
  minio_console: string
  api: string
}

export interface PrometheusMetricsResponse {
  http_requests_total: Record<string, number>
  http_request_duration_seconds: Record<string, number>
  http_requests_in_progress: Record<string, number>
}

export const SOURCE_TYPES = [
  "news",
  "forum",
  "blog",
  "social",
  "government",
  "academic",
  "encyclopedia",
  "ecommerce",
  "other",
] as const

export const CONTENT_TOPICS = [
  "economics",
  "politics",
  "health",
  "technology",
  "security",
  "environment",
  "society",
  "other",
] as const

export const INTELLIGENCE_CATEGORIES = [
  "data_leak",
  "gov_issue",
  "cyber_threat",
  "physical_threat",
  "misinformation",
  "other",
] as const

export const JOB_STATUSES: JobStatus[] = [
  "pending",
  "running",
  "paused",
  "completed",
  "failed",
  "skipped",
  "needs_review",
]
