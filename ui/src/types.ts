export type JobStatus =
  | "pending"
  | "running"
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

export interface CredentialSummary {
  email: string
  display_name?: string | null
  provider: string
  status: string
}

export interface JobSummary {
  job_id: string
  url: string
  status: JobStatus
  created_at: string | null
  user_id?: string
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
}

export interface BucketItemsResponse {
  bucket: string
  total: number
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
  recent: ThreatRecentRow[]
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
  "completed",
  "failed",
  "skipped",
  "needs_review",
]
