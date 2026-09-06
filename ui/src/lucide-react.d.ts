declare module 'lucide-react' {
  import type { ComponentType, SVGProps } from 'react'
  interface LucideIconProps extends SVGProps<SVGSVGElement> {
    size?: number
    strokeWidth?: number
    absoluteStrokeWidth?: boolean
  }
  type LucideIcon = ComponentType<LucideIconProps>
  export const Activity: LucideIcon
  export const AlertTriangle: LucideIcon
  export const Archive: LucideIcon
  export const ArrowLeft: LucideIcon
  export const Ban: LucideIcon
  export const BarChart3: LucideIcon
  export const Check: LucideIcon
  export const CheckCircle2: LucideIcon
  export const CheckSquare: LucideIcon
  export const ChevronDown: LucideIcon
  export const ChevronRight: LucideIcon
  export const CircleSlash: LucideIcon
  export const CircleX: LucideIcon
  export const ClipboardCheck: LucideIcon
  export const Clock3: LucideIcon
  export const Copy: LucideIcon
  export const Cpu: LucideIcon
  export const Database: LucideIcon
  export const Download: LucideIcon
  export const ExternalLink: LucideIcon
  export const Eye: LucideIcon
  export const FileDown: LucideIcon
  export const FileSearch: LucideIcon
  export const FileText: LucideIcon
  export const FolderSearch: LucideIcon
  export const Gauge: LucideIcon
  export const HardDrive: LucideIcon
  export const History: LucideIcon
  export const Inbox: LucideIcon
  export const Info: LucideIcon
  export const KeyRound: LucideIcon
  export const LayoutDashboard: LucideIcon
  export const Loader2: LucideIcon
  export const Lock: LucideIcon
  export const LogIn: LucideIcon
  export const LogOut: LucideIcon
  export const Mail: LucideIcon
  export const Menu: LucideIcon
  export const Pause: LucideIcon
  export const Play: LucideIcon
  export const Plus: LucideIcon
  export const Power: LucideIcon
  export const Radar: LucideIcon
  export const Radio: LucideIcon
  export const RefreshCcw: LucideIcon
  export const RefreshCw: LucideIcon
  export const Rocket: LucideIcon
  export const RotateCw: LucideIcon
  export const Save: LucideIcon
  export const Search: LucideIcon
  export const Settings: LucideIcon
  export const ShieldAlert: LucideIcon
  export const ShieldCheck: LucideIcon
  export const Square: LucideIcon
  export const Trash2: LucideIcon
  export const UserPlus: LucideIcon
  export const UserRound: LucideIcon
  export const Users: LucideIcon
  export const Waypoints: LucideIcon
  export const X: LucideIcon
  export const XCircle: LucideIcon
  export const Zap: LucideIcon
}