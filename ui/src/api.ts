import { getConfig } from "./config"
import type {
  ArticleItemDetail,
  SiteCredentialStatus,
  BucketItemsResponse,
  BucketOverviewResponse,
  ChangePasswordResponse,
  ConfirmEmailCodeResponse,
  CreateUserResponse,
  EvaluationRequest,
  EvaluationResponse,
  ForgotPasswordResponse,
  JobArticlesResponse,
  JobDetail,
  LoginResponse,
  MetricsResponse,
  MonitoringHealthResponse,
  MonitoringUrlsResponse,
  PerformanceResponse,
  PrometheusMetricsResponse,
  ResetPasswordResponse,
  ScrapeRequest,
  BatchScrapeRequest,
  BatchScrapeResponse,
  CredentialSummary,
  SearchApiResponse,
  ThreatAnalyticsResponse,
  TriggerJobResponse,
  UserJobsResponse,
  UserInfo,
  UsersListResponse,
  VerifyEmailResponse,
} from "./types"

export class ApiError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = "ApiError"
    this.status = status
  }
}

function extractDetail(body: unknown, fallback: string): string {
  if (body && typeof body === "object") {
    const detail = (body as Record<string, unknown>).detail
    if (typeof detail === "string") return detail
    if (Array.isArray(detail)) {
      // FastAPI validation errors
      return detail
        .map((item) => {
          if (item && typeof item === "object") {
            const rec = item as Record<string, unknown>
            const loc = Array.isArray(rec.loc) ? rec.loc.join(".") : ""
            const msg = typeof rec.msg === "string" ? rec.msg : ""
            return [loc, msg].filter(Boolean).join(": ")
          }
          return String(item)
        })
        .join("; ")
    }
  }
  return fallback
}

// ---------- session (token) persistence ----------
// Lives here so the API client can attach the bearer token without a circular
// import through React context.

const SESSION_KEY = "dukascraper.session.v1"

export interface StoredSession {
  token: string
  refreshToken?: string
  user: UserInfo
}

type SessionListener = (session: StoredSession | null) => void
const sessionListeners = new Set<SessionListener>()

export function getSession(): StoredSession | null {
  try {
    const raw = localStorage.getItem(SESSION_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as StoredSession
    return parsed.token && parsed.user ? parsed : null
  } catch {
    return null
  }
}

export function setSession(session: StoredSession | null): void {
  if (session) localStorage.setItem(SESSION_KEY, JSON.stringify(session))
  else localStorage.removeItem(SESSION_KEY)
  for (const listener of sessionListeners) listener(session)
}

export function onSessionChange(listener: SessionListener): () => void {
  sessionListeners.add(listener)
  return () => sessionListeners.delete(listener)
}

// ---------- token refresh ----------

/**
 * Single-flight token refresh. When the access token expires (default 30 min),
 * the backend rejects requests with 401; we rotate the refresh token and retry
 * transparently instead of kicking the user back to the login page.
 */
let refreshInFlight: Promise<string | null> | null = null

async function refreshAccessToken(): Promise<string | null> {
  const session = getSession()
  if (!session?.refreshToken) return null
  if (!refreshInFlight) {
    refreshInFlight = (async () => {
      try {
        const base = getConfig().apiBaseUrl.replace(/\/+$/, "")
        const res = await fetch(`${base}/api/v1/auth/refresh`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ refresh_token: session.refreshToken }),
        })
        if (!res.ok) return null
        const body = (await res.json()) as { access_token?: string; refresh_token?: string }
        if (!body.access_token) return null
        setSession({
          ...session,
          token: body.access_token,
          refreshToken: body.refresh_token ?? session.refreshToken,
        })
        return body.access_token
      } catch {
        return null
      } finally {
        refreshInFlight = null
      }
    })()
  }
  return refreshInFlight
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const base = getConfig().apiBaseUrl.replace(/\/+$/, "")
  const token = getSession()?.token
  let res: Response
  try {
    res = await fetch(base + path, {
      ...init,
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(init?.headers ?? {}),
      },
    })
  } catch {
    const target = base || "http://localhost:8000 (via dev proxy)"
    throw new ApiError(
      0,
      `Cannot reach the API at ${target}. Check that the backend is running, then open Settings (top-right) and update the API base URL if needed.`,
    )
  }
  if (res.status === 401 && !path.startsWith("/health") && !path.endsWith("/auth/login")) {
    // Expired access token: rotate it via the refresh token and retry once
    // before giving up (which clears the session and returns to login).
    const newToken = await refreshAccessToken()
    if (newToken) {
      const retryHeaders: Record<string, string> = {
        "Content-Type": "application/json",
        Authorization: `Bearer ${newToken}`,
        ...((init?.headers as Record<string, string> | undefined) ?? {}),
      }
      res = await fetch(base + path, { ...init, headers: retryHeaders })
    }
    if (res.status === 401) {
      setSession(null) // refresh failed too -> back to the login screen
    }
  }
  const text = await res.text()
  let body: unknown = null
  if (text) {
    try {
      body = JSON.parse(text)
    } catch {
      body = null
    }
  }
  if (!res.ok) {
    throw new ApiError(res.status, extractDetail(body, `${res.status} ${res.statusText}`))
  }
  return body as T
}

function storageObjectUrl(bucketKey: string, name: string): string {
  const base = getConfig().apiBaseUrl.replace(/\/+$/, "")
  return `${base}/api/v1/storage/${encodeURIComponent(bucketKey)}/download?name=${encodeURIComponent(name)}`
}

function webSocketBase(): string {
  const base = getConfig().apiBaseUrl.replace(/\/+$/, "")
  if (!base) {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:"
    return `${proto}//${window.location.host}`
  }
  return base.replace(/^http/, "ws")
}

/** Live status stream for a single job (WebSocket URL). */
export function getJobStatusSocketUrl(jobId: string): string {
  const token = getSession()?.token
  const base = `${webSocketBase()}/api/v1/ws/jobs/${encodeURIComponent(jobId)}`
  return token ? `${base}?token=${encodeURIComponent(token)}` : base
}

/** Live feed of job events across jobs (WebSocket URL). */
export function getJobFeedSocketUrl(): string {
  const session = getSession()
  const base = `${webSocketBase()}/api/v1/ws/jobs`
  const params = new URLSearchParams()
  if (session?.token) params.set("token", session.token)
  // Non-admin listeners scope the feed to their own jobs; admins get all.
  if (session && session.user.role !== "admin") {
    params.set("user_id", session.user.user_id)
  }
  const qs = params.toString()
  return qs ? `${base}?${qs}` : base
}

function storageExportUrl(bucketKey: string, name: string, format: string): string {
  const base = getConfig().apiBaseUrl.replace(/\/+$/, "")
  return `${base}/api/v1/storage/${encodeURIComponent(bucketKey)}/export?name=${encodeURIComponent(name)}&format=${encodeURIComponent(format)}`
}

export const api = {
  health: () => request<{ status: string }>("/health"),

  // ---------- auth ----------
  login: (username: string, password: string) =>
    request<LoginResponse>("/api/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),

  me: () => request<{ user: UserInfo } | UserInfo>("/api/v1/auth/me"),

  listUsers: () => request<UsersListResponse>("/api/v1/auth/users"),

  /** Step 1 - send a 6-digit verification code to the email address. */
  sendVerificationCode: (email: string, password: string) =>
    request<VerifyEmailResponse>("/api/v1/auth/users/verify", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    }),

  /** Step 2 - confirm the emailed code and get a registration token. */
  confirmVerificationCode: (email: string, code: string) =>
    request<ConfirmEmailCodeResponse>("/api/v1/auth/users/confirm-code", {
      method: "POST",
      body: JSON.stringify({ email, code }),
    }),

  /** Step 3 - create the (email-verified) account with username + profile. */
  completeUserCreation: (payload: {
    email: string
    username: string
    full_name?: string
    role: string
    token: string
    must_change_password?: boolean
  }) => request<CreateUserResponse>("/api/v1/auth/users/complete", {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  deleteUser: (username: string) =>
    request<{ message: string }>(`/api/v1/auth/users/${encodeURIComponent(username)}`, {
      method: "DELETE",
    }),

  setUserActive: (username: string, active: boolean) =>
    request<{ message: string }>(`/api/v1/auth/users/${encodeURIComponent(username)}/status`, {
      method: "PATCH",
      body: JSON.stringify({ active }),
    }),

  /** Admin: force an account to change its password on next login. */
  forceResetPassword: (username: string) =>
    request<{ message: string }>(`/api/v1/auth/users/${encodeURIComponent(username)}/force-reset`, {
      method: "POST",
    }),

  /** Change the current user's password (clears the force-reset flag). */
  changePassword: (currentPassword: string, newPassword: string) =>
    request<ChangePasswordResponse>("/api/v1/auth/change-password", {
      method: "POST",
      body: JSON.stringify({ current_password: currentPassword, new_password: newPassword }),
    }),

  /** Step 1 - send a password-reset code to the account's email. */
  forgotPassword: (email: string) =>
    request<ForgotPasswordResponse>("/api/v1/auth/forgot-password", {
      method: "POST",
      body: JSON.stringify({ email }),
    }),

  /** Step 2 - verify the emailed code and set a new password. */
  resetPassword: (email: string, code: string, newPassword: string) =>
    request<ResetPasswordResponse>("/api/v1/auth/reset-password", {
      method: "POST",
      body: JSON.stringify({ email, code, new_password: newPassword }),
    }),

  // ---------- jobs ----------
  triggerJob: (payload: ScrapeRequest) =>
    request<TriggerJobResponse>("/api/v1/jobs/trigger", {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  triggerBatch: (payload: BatchScrapeRequest) =>
    request<BatchScrapeResponse>("/api/v1/jobs/batch", {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  listCredentials: () => request<CredentialSummary[]>("/api/v1/credentials/"),

  /** Check whether a reusable site credential exists for a domain. */
  getSiteCredential: (domain: string) =>
    request<SiteCredentialStatus>(
      `/api/v1/credentials/domain/${encodeURIComponent(domain)}/site-credential`,
    ),

  /** Save a reusable site credential (stored encrypted for auto-login). */
  saveSiteCredential: (domain: string, payload: { email: string; password: string; username?: string }) =>
    request<{ message: string }>(
      `/api/v1/credentials/domain/${encodeURIComponent(domain)}/site-credential`,
      { method: "POST", body: JSON.stringify(payload) },
    ),

  getJob: (jobId: string) => request<JobDetail>(`/api/v1/jobs/${encodeURIComponent(jobId)}`),

  getUserJobs: (userId: string) =>
    request<UserJobsResponse>(`/api/v1/jobs/user/${encodeURIComponent(userId)}`),

  getAllJobs: () => request<UserJobsResponse>("/api/v1/jobs/all"),

  searchArticles: (q: string, size = 20) =>
    request<SearchApiResponse>(`/api/v1/articles/search?q=${encodeURIComponent(q)}&size=${size}`),

  getJobArticles: (jobId: string) =>
    request<JobArticlesResponse>(`/api/v1/articles/job/${encodeURIComponent(jobId)}`),

  getJobSummary: (jobId: string) =>
    request<{
      job_id: string
      total: number
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
    }>(`/api/v1/articles/job/${encodeURIComponent(jobId)}/summary`),

  getItem: (itemId: string) =>
    request<ArticleItemDetail>(`/api/v1/articles/${encodeURIComponent(itemId)}`),

  listBuckets: () => request<BucketOverviewResponse>("/api/v1/storage/"),

  listBucketItems: (bucketKey: string, prefix = "") =>
    request<BucketItemsResponse>(
      `/api/v1/storage/${encodeURIComponent(bucketKey)}?prefix=${encodeURIComponent(prefix)}`,
    ),

  /** URL of a single MinIO object (for browser download links). */
  getStorageObjectUrl: storageObjectUrl,

  /** URL of a converted parsed object (pdf/docx/csv/html/txt/json export). */
  getStorageExportUrl: (bucketKey: string, name: string, format: string) =>
    storageExportUrl(bucketKey, name, format),

  /** Download several objects as one ZIP archive (blob trigger). */
  downloadMany: async (bucketKey: string, names: string[], format: string): Promise<Blob> => {
    const base = getConfig().apiBaseUrl.replace(/\/+$/, "")
    const token = getSession()?.token
    let res: Response
    try {
      res = await fetch(`${base}/api/v1/storage/${encodeURIComponent(bucketKey)}/download-many`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: JSON.stringify({ names, format }),
      })
    } catch {
      throw new ApiError(0, "Cannot reach the API. Is the backend running?")
    }
    if (!res.ok) {
      const text = await res.text()
      let body: unknown = null
      try {
        body = JSON.parse(text)
      } catch {
        body = null
      }
      throw new ApiError(res.status, extractDetail(body, `${res.status} ${res.statusText}`))
    }
    return res.blob()
  },

  /** Raw text/JSON content of a single MinIO object (for inline preview). */
  getStorageObjectText: async (bucketKey: string, name: string): Promise<string> => {
    let res: Response
    try {
      res = await fetch(storageObjectUrl(bucketKey, name))
    } catch {
      throw new ApiError(0, "Cannot reach the API. Is the backend running?")
    }
    const text = await res.text()
    if (!res.ok) throw new ApiError(res.status, extractDetail(null, `${res.status} ${res.statusText}`))
    return text
  },

  submitEvaluation: (payload: EvaluationRequest) =>
    request<EvaluationResponse>("/api/v1/analytics/evaluations", {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  getMetrics: () => request<MetricsResponse>("/api/v1/analytics/evaluations/metrics"),

  getThreatAnalytics: (limit = 50) =>
    request<ThreatAnalyticsResponse>(`/api/v1/analytics/threats?limit=${limit}`),

  getPerformance: (limit = 25) =>
    request<PerformanceResponse>(`/api/v1/analytics/performance?limit=${limit}`),

  // ---------- monitoring ----------
  getMonitoringHealth: () => request<MonitoringHealthResponse>("/api/v1/monitoring/health"),

  getMonitoringUrls: () => request<MonitoringUrlsResponse>("/api/v1/monitoring/urls"),

  getPrometheusMetrics: () => request<PrometheusMetricsResponse>("/api/v1/monitoring/prometheus"),
}
