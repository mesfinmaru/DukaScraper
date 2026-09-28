import { useEffect, useState } from "react"
import { NavLink, Outlet, useLocation } from "react-router-dom"
import {
  Activity,
  BarChart3,
  HardDrive,
  LayoutDashboard,
  LogOut,
  Menu,
  Rocket,
  Search,
  ShieldCheck,
  UserRound,
  Users,
  Waypoints,
} from "lucide-react"
import { api } from "../api"
import { useAuth } from "../config"
import { cn } from "../utils"
import { ThemeToggle } from "../theme"
import { HealthPill } from "./HealthPill"
import { Logo } from "./Logo"

const NAV_ITEMS = [
  { to: "/", label: "Dashboard", icon: LayoutDashboard, end: true },
  { to: "/new-crawl", label: "New Crawl", icon: Rocket, end: false },
  { to: "/jobs", label: "Jobs", icon: Waypoints, end: false },
  { to: "/search", label: "Search", icon: Search, end: false },
  { to: "/storage", label: "Storage", icon: HardDrive, end: false },
  { to: "/analytics", label: "Analytics", icon: BarChart3, end: false },
]

const ADMIN_NAV_ITEMS = [
  { to: "/monitoring", label: "Monitoring", icon: Activity, end: false },
  { to: "/users", label: "Users", icon: Users, end: false },
]

function SidebarContent({ onNavigate }: { onNavigate?: () => void }) {
  const { session } = useAuth()
  const navItems = [...NAV_ITEMS, ...(session?.user.role === "admin" ? ADMIN_NAV_ITEMS : [])]

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center px-5 py-5">
        <Logo className="h-8 w-auto" />
      </div>

      <nav className="flex-1 space-y-1 px-3 py-2">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end}
            onClick={onNavigate}
            className={({ isActive }) =>
              cn(
                "flex items-center gap-3 rounded-lg border border-transparent px-3 py-2.5 text-sm font-medium transition",
                isActive
                  ? "bg-indigo-600 text-white shadow-[0_0_16px_rgba(34,197,94,0.35)] ring-1 ring-emerald-300/30"
                  : "text-gray-400 hover:bg-white/5 hover:text-gray-100",
              )
            }
          >
            <item.icon className="h-4 w-4 shrink-0" />
            {item.label}
          </NavLink>
        ))}
      </nav>
    </div>
  )
}

export function Layout() {
  const { session, logout } = useAuth()
  const location = useLocation()
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [online, setOnline] = useState<boolean | null>(null)

  useEffect(() => {
    setSidebarOpen(false)
  }, [location.pathname])

  useEffect(() => {
    let cancelled = false
    const check = async () => {
      try {
        await api.health()
        if (!cancelled) setOnline(true)
      } catch {
        if (!cancelled) setOnline(false)
      }
    }
    void check()
    const t = setInterval(check, 20000)
    return () => {
      cancelled = true
      clearInterval(t)
    }
  }, [])

  return (
    <div className="flex min-h-screen">
      <aside className="sticky top-0 hidden h-screen w-64 shrink-0 flex-col border-r border-slate-800/70 bg-[#050b08] md:flex">
        <SidebarContent />
      </aside>

      {sidebarOpen && (
        <div className="fixed inset-0 z-40 md:hidden">
          <div
            className="absolute inset-0 bg-gray-900/50 backdrop-blur-sm"
            onClick={() => setSidebarOpen(false)}
          />
          <aside className="absolute inset-y-0 left-0 w-64 border-r border-slate-800/70 bg-[#050b08]">
            <button
              type="button"
              aria-label="Close menu"
              onClick={() => setSidebarOpen(false)}
              className="absolute top-4 right-3 rounded-md p-1.5 text-gray-400 transition hover:bg-white/10 hover:text-white"
            >
              <Menu className="h-4 w-4 rotate-90" />
            </button>
            <SidebarContent onNavigate={() => setSidebarOpen(false)} />
          </aside>
        </div>
      )}

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="sticky top-0 z-30 flex h-16 items-center gap-3 border-b border-slate-800/70 bg-black/90 px-4 backdrop-blur sm:px-6">
          <button
            type="button"
            aria-label="Open menu"
            onClick={() => setSidebarOpen(true)}
            className="rounded-md p-1.5 text-slate-400 transition hover:bg-slate-900 hover:text-slate-100 md:hidden"
          >
            <Menu className="h-5 w-5" />
          </button>

          <div className="ml-auto flex items-center gap-3">
            <HealthPill online={online} />
            <ThemeToggle />
            <span className="hidden h-6 w-px bg-slate-800 sm:block" />
            {session && (
              <>
                <span
                  className="inline-flex items-center gap-2 rounded-full border border-slate-800 bg-slate-950 px-3 py-1.5 text-xs font-medium text-slate-200 shadow-sm"
                  title={`Logged in as ${session.user.username} (${session.user.role})`}
                >
                  {session.user.role === "admin" ? (
                    <ShieldCheck className="h-3.5 w-3.5 text-violet-500" />
                  ) : (
                    <UserRound className="h-3.5 w-3.5 text-indigo-500" />
                  )}
                  <span className="font-mono">{session.user.username}</span>
                </span>
                <button
                  type="button"
                  onClick={logout}
                  title="Sign out"
                  className="cursor-pointer rounded-full border border-slate-800 bg-slate-950 p-2 text-slate-400 shadow-sm transition hover:border-rose-500 hover:text-rose-500"
                >
                  <LogOut className="h-3.5 w-3.5" />
                </button>
              </>
            )}
          </div>
        </header>

        <main className="mx-auto w-full max-w-7xl flex-1 px-4 py-8 sm:px-6 lg:px-8">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
