import { HashRouter, Navigate, Route, Routes } from "react-router-dom"
import { AppProviders, useAuth } from "./config"
import { Layout } from "./components/Layout"
import { ThemeProvider } from "./theme"
import Dashboard from "./pages/Dashboard"
import NewCrawl from "./pages/NewCrawl"
import Jobs from "./pages/Jobs"
import JobDetail from "./pages/JobDetail"
import JobArticles from "./pages/JobArticles"
import Search from "./pages/Search"
import Storage from "./pages/Storage"
import Analytics from "./pages/Analytics"
import Alerts from "./pages/Alerts"
import LoginPage from "./pages/LoginPage"
import VerifyOtpPage from "./pages/VerifyOtpPage"
import ForgotPasswordPage from "./pages/ForgotPasswordPage"
import ChangePasswordPage from "./pages/ChangePasswordPage"
import Monitoring from "./pages/Monitoring"
import UsersPage from "./pages/UsersPage"
import VerifyEmailPage from "./pages/VerifyEmailPage"

function AuthenticatedRoutes() {
  const { session } = useAuth()
  const isAdmin = session?.user.role === "admin"
  const mustChange = session?.user.must_change_password

  return (
    <Routes>
      {session ? (
        mustChange ? (
          <Route path="*" element={<ChangePasswordPage />} />
        ) : (
          <Route element={<Layout />}>
            <Route index element={<Dashboard />} />
            <Route path="/new-crawl" element={<NewCrawl />} />
            <Route path="/jobs" element={<Jobs />} />
            <Route path="/jobs/:jobId" element={<JobDetail />} />
            <Route path="/jobs/:jobId/articles" element={<JobArticles />} />
            <Route path="/search" element={<Search />} />
            <Route path="/storage" element={<Storage />} />
            <Route path="/analytics" element={<Analytics />} />
            <Route path="/alerts" element={<Alerts />} />
            {isAdmin && <Route path="/monitoring" element={<Monitoring />} />}
            {isAdmin && <Route path="/users" element={<UsersPage />} />}
            <Route path="*" element={<Navigate to="/" replace />} />
          </Route>
        )
      ) : (
        <>
          <Route path="/verify-otp" element={<VerifyOtpPage />} />
          <Route path="/verify-email" element={<VerifyEmailPage />} />
          <Route path="/forgot-password" element={<ForgotPasswordPage />} />
          <Route path="*" element={<LoginPage />} />
        </>
      )}
    </Routes>
  )
}

export default function App() {
  return (
    <AppProviders>
      <ThemeProvider>
        <HashRouter>
          <AuthenticatedRoutes />
        </HashRouter>
      </ThemeProvider>
    </AppProviders>
  )
}
