import { getConfig } from "./config"

export interface ApiFailure {
  service: string
  status: number
  message: string
  at: number
}

import type {
  AlertsResponse,
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
  RequiresVerificationResponse,
  VerifyOtpResponse,
  DiscoverRequest,
  DiscoverResponse,
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
  EntityMentionsResponse,
  EntitySummaryResponse,
  SemanticSearchResponse,
  ThreatAnalyticsResponse,
  EmbedTokenResponse,
  UnreadAlertsResponse,
  ThreatQuery,
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

// ---- Global API failure tracker ---------------------------------------------
// The HealthPill polls /ready, which only sees infrastructure dependencies —
// a page-level endpoint failing (dev-proxy 502, backend 500, network drop)
// never reached it, so the dot stayed green over a broken page. Every failed
// request records its service here; any later successful request to the same
// service clears it. HealthPill subscribes and turns red/amber accordingly.

const failureListeners = new Set<(failures: ApiFailure[]) => void>()
const liveFailures = new Map<string, ApiFailure>()

export const apiFailures = liveFailures

export function subscribeApiFailures(fn: (failures: ApiFailure[]) => void): () => void {
  failureListeners.add(fn)
  fn([...liveFailures.values()])
  return () => failureListeners.delete(fn)
}

function serviceOf(path: string): string {
  const m = path.match(/^\/api\/v\d+\/([^/?#]+)/)
  return m ? m[1] : "api"
}

function emitApiFailures() {
  const snapshot = [...liveFailures.values()]
  for (const fn of failureListeners) fn(snapshot)
}

function recordApiFailure(path: string, status: number, message: string) {
  const service = serviceOf(path)
  const prev = liveFailures.get(service)
  if (prev && prev.status === status && prev.message === message) {
    prev.at = Date.now()
    return
  }
  liveFailures.set(service, { service, status, message, at: Date.now() })
  emitApiFailures()
}

function recordApiSuccess(path: string) {
  if (liveFailures.delete(serviceOf(path))) emitApiFailures()
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

// ---------- Recording failures from the request helper ----------

request.__recordFailure = recordApiFailure as (path: string, status: number, message: string) => void
request.__recordSuccess = recordApiSuccess as (path: string) => void

// ---------- session (token) persistence ----------
// Lives here so the API client can attach the bearer token without a circular
// import through React context.

const SESSION_KEY = "dukascraper.session.v1" // legacy single-session key
/** Session of the most recent login, used to seed a brand-new tab. */
const LAST_SESSION_KEY = "dukascraper.lastSession.v1"
/** Random id for this tab, kept in sessionStorage so it dies with the tab. */
const TAB_KEY = "dukascraper.tab.v1"

function sessionStorageKey(): string {
  return `${SESSION_KEY}.${tabId()}`
}

/** Stable id for this tab. Survives refresh, unique per tab. */
function tabId(): string {
  try {
    const existing = sessionStorage.getItem(TAB_KEY)
    if (existing) return existing
    const fresh =
      typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
        ? crypto.randomUUID()
        : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`
    sessionStorage.setItem(TAB_KEY, fresh)
    return fresh
  } catch {
    // Private mode / storage disabled: fall back to a process-stable id so the
    // app still works for this page view.
    return "ephemeral"
  }
}

function parseSession(raw: string | null): StoredSession | null {
  if (!raw) return null
  try {
    const parsed = JSON.parse(raw) as StoredSession
    return parsed.token && parsed.user ? parsed : null
  } catch {
    return null
  }
}

/**
 * Move a session written by an older build (one shared localStorage key) into
 * the new layout, so upgrading does not sign everyone out.
 */
function migrateLegacySession(): void {
  try {
    const legacy = localStorage.getItem(SESSION_KEY)
    if (!legacy) return
    if (!localStorage.getItem(LAST_SESSION_KEY)) {
      localStorage.setItem(LAST_SESSION_KEY, legacy)
    }
    localStorage.removeItem(SESSION_KEY)
  } catch {
    /* ignore */
  }
}

export interface StoredSession {
  token: string
  refreshToken?: string
  user: UserInfo
}

type SessionListener = (session: StoredSession | null) => void
const sessionListeners = new Set<SessionListener>()

export function getSession(): StoredSession | null {
  try {
    const own = parseSession(sessionStorage.getItem(sessionStorageKey()))
    if (own) return own

    // A tab opened after the last sign-in has nothing of its own, so it adopts
    // that login. It is then written to this tab's own slot, which is what keeps
    // the account stable if another tab signs in as someone else afterwards.
    const shared = localStorage.getItem(LAST_SESSION_KEY)
    const adopted = parseSession(shared)
    if (adopted) sessionStorage.setItem(sessionStorageKey(), shared as string)
    return adopted
  } catch {
    return null
  }
}

export function setSession(session: StoredSession | null): void {
  try {
    const key = sessionStorageKey()
    if (session) {
      const encoded = JSON.stringify(session)
      sessionStorage.setItem(key, encoded)
      // Remembered so a later brand-new tab starts signed in.
      localStorage.setItem(LAST_SESSION_KEY, encoded)
    } else {
      const previous = sessionStorage.getItem(key)
      sessionStorage.removeItem(key)
      // Only drop the shared fallback if it was this tab's own session;
      // otherwise signing out here would sign out other tabs too.
      if (previous && localStorage.getItem(LAST_SESSION_KEY) === previous) {
        localStorage.removeItem(LAST_SESSION_KEY)
      }
    }
  } catch {
    /* ignore */
  }
  for (const listener of sessionListeners) listener(session)
}

migrateLegacySession()

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
    request.__recordFailure?.(path, 0, "API unreachable")
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
    if (res.status >= 500) {
      request.__recordFailure?.(path, res.status, extractDetail(body, `${res.status} ${res.statusText}`))
    }
    throw new ApiError(res.status, extractDetail(body, `${res.status} ${res.statusText}`))
  }
  request.__recordSuccess?.(path)
  return body as T
}

function storageObjectUrl(bucketKey: string, name: string): string {
  const base = getConfig().apiBaseUrl.replace(/\/+$/, "")
  const token = getSession()?.token
  const qs = new URLSearchParams({ name })
  if (token) qs.set("token", token)
  return `${base}/api/v1/storage/${encodeURIComponent(bucketKey)}/download?${qs.toString()}`
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
  const qs = new URLSearchParams({ name, format })
  const token = getSession()?.token
  if (token) qs.set("token", token)
  return `${base}/api/v1/storage/${encodeURIComponent(bucketKey)}/export?${qs.toString()}`
}

export const api = {
  health: () => request<{ status: string }>("/health"),

  /** Readiness: 200 when all dependencies are up, 503 (degraded) otherwise. */
  ready: async (): Promise<{ ready: boolean; failed?: string[] }> => {
    const base = getConfig().apiBaseUrl.replace(/\/+$/, "")
    const token = getSession()?.token
    const res = await fetch(`${base}/ready`, {
      headers: token ? { Authorization: `Bearer ${token}` } : undefined,
    })
    if (res.status === 503) {
      // Degraded: parse the per-dependency checks so the UI can name what is down.
      let checks: Record<string, boolean> = {}
      try {
        const body = (await res.json()) as { checks?: Record<string, boolean> }
        checks = body.checks ?? {}
      } catch {
        /* ignore body parse errors */
      }
      const failed = Object.entries(checks)
        .filter(([, ok]) => !ok)
        .map(([name]) => name)
      return { ready: false, failed }
    }
    return { ready: res.ok }
  },

  // ---------- auth ----------
  login: (username: string, password: string) =>
    request<LoginResponse>("/api/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),

  verifyOtp: (userId: string, code: string) =>

    request<VerifyOtpResponse>("/api/v1/auth/verify-otp", { method: "POST", body: JSON.stringify({ user_id: userId, code }) }),



  resendOtp: (userId: string) =>

    request<RequiresVerificationResponse>("/api/v1/auth/verify-otp/resend", { method: "POST", body: JSON.stringify({ user_id: userId }) }),



  createUser: (payload: { full_name: string; username: string; email: string; password: string; role: string }) =>

    request<UserInfo>("/api/v1/auth/users", { method: "POST", body: JSON.stringify(payload) }),



  discoverJob: (payload: DiscoverRequest) =>

    request<DiscoverResponse>("/api/v1/jobs/discover", { method: "POST", body: JSON.stringify(payload) }),



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

  /** Confirm the email-verification token sent in the welcome email. */
  /** Confirm an address with the emailed 6-digit code.
   *  `user_id` identifies the account; the code is the proof of inbox
   *  ownership. The link from the email deliberately cannot verify on its own. */
  verifyEmailConfirm: (payload: { user_id: string; code: string }) =>
    request<{ message: string }>("/api/v1/auth/email-verification/confirm", {
      method: "POST",
      body: JSON.stringify(payload),
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

  /**
   * Pause a running job. Pages already being fetched finish and are kept; the
   * job stops after that and resumes from the same point later.
   */
  pauseJob: (jobId: string) =>
    request<{ job_id: string; status: string; message: string }>(
      `/api/v1/jobs/${encodeURIComponent(jobId)}/pause`,
      { method: "POST" },
    ),

  /** Resume a paused job, continuing from where it stopped. */
  resumeJob: (jobId: string) =>
    request<{ job_id: string; status: string; message: string }>(
      `/api/v1/jobs/${encodeURIComponent(jobId)}/resume`,
      { method: "POST" },
    ),

  searchArticles: (q: string, size = 20) =>
    request<SearchApiResponse>(`/api/v1/articles/search?q=${encodeURIComponent(q)}&size=${size}`),

  getJobArticles: (jobId: string) =>
    request<JobArticlesResponse>(`/api/v1/articles/job/${encodeURIComponent(jobId)}`),

  getJobSummary: (
    jobId: string,
    params?: { limit?: number; offset?: number; include_raw?: boolean },
  ) =>
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
    }>(
      `/api/v1/articles/job/${encodeURIComponent(jobId)}/summary` +
        (params
          ? `?${new URLSearchParams(
              Object.entries(params)
                .filter(([, v]) => v !== undefined)
                .map(([k, v]) => [k, String(v)]),
            ).toString()}`
          : "")
    ),

  getItem: (itemId: string) =>
    request<ArticleItemDetail>(`/api/v1/articles/${encodeURIComponent(itemId)}`),

  listBuckets: () => request<BucketOverviewResponse>("/api/v1/storage/"),

  /**
   * One page of bucket objects.
   *
   * Paged on the server with a keyset cursor: `after` is the last key of the
   * previous page. Fetching the whole bucket and paginating in the browser was
   * the cause of Storage taking up to a minute - 16.5k rows in one 2.1 MB
   * response, re-downloaded on every refresh. `total` is null while more pages
   * remain, so the UI shows "Showing X+" rather than claiming an exact count.
   */
  listBucketItems: (
    bucketKey: string,
    prefix = "",
    opts: { limit?: number; after?: string; job_id?: string; site?: string } = {},
  ) => {
    const params = new URLSearchParams()
    if (prefix) params.set("prefix", prefix)
    if (opts.limit) params.set("limit", String(opts.limit))
    if (opts.after) params.set("after", opts.after)
    if (opts.job_id) params.set("job_id", opts.job_id)
    if (opts.site) params.set("site", opts.site)
    const qs = params.toString()
    return request<BucketItemsResponse>(
      `/api/v1/storage/${encodeURIComponent(bucketKey)}${qs ? `?${qs}` : ""}`,
    )
  },

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
      res = await fetch(storageObjectUrl(bucketKey, name), {
        headers: (() => {
          const token = getSession()?.token
          return token ? { Authorization: `Bearer ${token}` } : undefined
        })(),
      })
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

  /**
   * Flagged-content list.
   *
   * Paginated rather than truncated to the newest N: with thousands of
   * classified items the old shape made everything past the first page
   * unreachable. The filters narrow both the page and the counts, so the
   * numbers on screen always describe the rows underneath them.
   */
  getThreatAnalytics: (opts: ThreatQuery = {}) => {
    const params = new URLSearchParams()
    if (opts.offset !== undefined) params.set("offset", String(opts.offset))
    if (opts.limit !== undefined) params.set("limit", String(opts.limit))
    if (opts.category) params.set("category", opts.category)
    if (opts.severity !== undefined) params.set("severity", String(opts.severity))
    if (opts.source_type) params.set("source_type", opts.source_type)
    const qs = params.toString()
    return request<ThreatAnalyticsResponse>(
      `/api/v1/analytics/threats${qs ? `?${qs}` : ""}`,
    )
  },

  getPerformance: (limit = 25) =>
    request<PerformanceResponse>(`/api/v1/analytics/performance?limit=${limit}`),

  getIntelligenceEntities: (limit = 100) =>
    request<EntitySummaryResponse>(`/api/v1/analytics/intelligence/entities?limit=${limit}`),

  searchEntityMentions: (entity: string, limit = 50) =>
    request<EntityMentionsResponse>(
      `/api/v1/analytics/intelligence/entities/search?entity=${encodeURIComponent(entity)}&limit=${limit}`,
    ),

  semanticSearch: (q: string, size = 20) =>
    request<SemanticSearchResponse>(
      `/api/v1/articles/search/semantic?q=${encodeURIComponent(q)}&size=${size}`,
    ),

  // ---------- monitoring ----------
  getMonitoringHealth: () => request<MonitoringHealthResponse>("/api/v1/monitoring/health"),

  getMonitoringUrls: () => request<MonitoringUrlsResponse>("/api/v1/monitoring/urls"),

  getPrometheusMetrics: () => request<PrometheusMetricsResponse>("/api/v1/monitoring/prometheus"),

  /**
   * Absolute origin for the monitoring embed proxy.
   *
   * Embeds must be absolute (they go in an iframe `src`, not a fetch), and must
   * point at the API rather than Grafana directly: Grafana >= 13 sends
   * `X-Frame-Options: deny` unconditionally, which the browser refuses to frame.
   *
   * The proxy mounts Grafana and Prometheus at the API *root* (`/grafana`,
   * `/prometheus`), not under `/api/v1/monitoring/...`: Grafana builds every
   * asset, API and websocket URL from its own `root_url`, so it has to sit at a
   * real sub-path for those URLs to resolve inside the frame.
   */
  monitoringProxyBase: () => getConfig().apiBaseUrl.replace(/\/+$/, ""),

  getMonitoringEmbedToken: () =>
    request<EmbedTokenResponse>("/api/v1/monitoring/embed-token"),

  // ---------- alerts ----------
  getAlerts: (params: {
    unread_only?: boolean
    min_severity?: number
    limit?: number
    offset?: number
  } = {}) => {
    const qs = new URLSearchParams()
    if (params.unread_only) qs.set("unread_only", "true")
    if (params.min_severity) qs.set("min_severity", String(params.min_severity))
    if (params.limit) qs.set("limit", String(params.limit))
    if (params.offset) qs.set("offset", String(params.offset))
    const suffix = qs.toString()
    return request<AlertsResponse>(`/api/v1/alerts${suffix ? `?${suffix}` : ""}`)
  },

  getUnreadAlerts: () => request<UnreadAlertsResponse>("/api/v1/alerts/unread"),

  markAlertRead: (alertId: string) =>
    request<{ alert_id: string; read: boolean }>(
      `/api/v1/alerts/${encodeURIComponent(alertId)}/read`,
      { method: "POST" },
    ),

  markAllAlertsRead: () =>
    request<{ marked: number; unread: number }>("/api/v1/alerts/read-all", {
      method: "POST",
    }),
}
