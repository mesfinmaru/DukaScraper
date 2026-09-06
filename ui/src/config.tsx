import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react"
import type { ReactNode } from "react"
import { api, getSession, onSessionChange, setSession } from "./api"
import type { StoredSession } from "./api"

export interface AppConfig {
  /** Base URL of the FastAPI gateway. Empty string means same-origin. */
  apiBaseUrl: string
}

const DEFAULT_CONFIG: AppConfig = {
  apiBaseUrl: "",
}

let currentConfig: AppConfig = DEFAULT_CONFIG

/** Direct (non-hook) access for the API client. */
export function getConfig(): AppConfig {
  return currentConfig
}

interface ConfigContextValue {
  config: AppConfig
}

const ConfigContext = createContext<ConfigContextValue | null>(null)

// ---------- auth session ----------

interface AuthContextValue {
  session: StoredSession | null
  login: (username: string, password: string) => Promise<void>
  logout: () => void
  changePassword: (currentPassword: string, newPassword: string) => Promise<void>
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AppProviders({ children }: { children: ReactNode }) {
  const [session, setSessionState] = useState<StoredSession | null>(getSession)

  useEffect(() => onSessionChange(setSessionState), [])

  const login = useCallback(async (username: string, password: string) => {
    const res = await api.login(username, password)
    setSession({ token: res.access_token, refreshToken: res.refresh_token, user: res.user })
  }, [])

  const logout = useCallback(() => setSession(null), [])

  const changePassword = useCallback(
    async (currentPassword: string, newPassword: string) => {
      const res = await api.changePassword(currentPassword, newPassword)
      const current = getSession()
      if (current) {
        setSession({ ...current, user: { ...current.user, must_change_password: res.must_change_password } })
      }
    },
    [],
  )

  const configValue = useMemo(
    () => ({ config: DEFAULT_CONFIG }),
    [],
  )
  const authValue = useMemo(
    () => ({ session, login, logout, changePassword }),
    [session, login, logout, changePassword],
  )

  return (
    <ConfigContext.Provider value={configValue}>
      <AuthContext.Provider value={authValue}>{children}</AuthContext.Provider>
    </ConfigContext.Provider>
  )
}

// eslint-disable-next-line react-refresh/only-export-components
export function useConfig(): ConfigContextValue {
  const ctx = useContext(ConfigContext)
  if (!ctx) throw new Error("useConfig must be used within AppProviders")
  return ctx
}

// eslint-disable-next-line react-refresh/only-export-components
export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error("useAuth must be used within AppProviders")
  return ctx
}
