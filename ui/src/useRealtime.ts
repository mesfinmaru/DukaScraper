import { useEffect, useLayoutEffect, useRef, useState } from "react"

export interface RealtimeSocketOptions {
  /** WebSocket URL; null/"" disables the socket. */
  url: string | null
  /** Whether live updates are currently wanted. */
  enabled: boolean
  /** Called for every parsed JSON message the server sends. */
  onEvent?: (payload: Record<string, unknown>) => void
  /** Called once when the socket (re)connects. */
  onOpen?: () => void
  /** Called periodically while the socket is down (best-effort fallback). */
  onFallback?: () => void
  /** Fallback refresh cadence in ms while disconnected (default 15000). */
  fallbackMs?: number
}

const RECONNECT_BASE_MS = 2000
const RECONNECT_MAX_MS = 30000

/**
 * Keep a WebSocket open with exponential backoff reconnects. While the socket
 * is down, `onFallback` fires every `fallbackMs` so pages never go stale.
 */
export function useRealtimeSocket({
  url,
  enabled,
  onEvent,
  onOpen,
  onFallback,
  fallbackMs = 15000,
}: RealtimeSocketOptions): { connected: boolean } {
  const [connected, setConnected] = useState(false)

  // Keep latest callbacks without re-subscribing on every render.
  const cbRef = useRef({ onEvent, onOpen, onFallback })
  useLayoutEffect(() => {
    cbRef.current = { onEvent, onOpen, onFallback }
  }, [onEvent, onOpen, onFallback])

  useEffect(() => {
    if (!enabled || !url) {
      setConnected(false)
      return
    }

    let socket: WebSocket | null = null
    let closedByEffect = false
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null
    let fallbackTimer: ReturnType<typeof setInterval> | null = null
    let delay = RECONNECT_BASE_MS

    const clearTimers = () => {
      if (reconnectTimer) clearTimeout(reconnectTimer)
      if (fallbackTimer) clearInterval(fallbackTimer)
      reconnectTimer = null
      fallbackTimer = null
    }

    const startFallback = () => {
      if (fallbackTimer) clearInterval(fallbackTimer)
      fallbackTimer = setInterval(() => cbRef.current.onFallback?.(), fallbackMs)
    }

    let generation = 0

    const connect = () => {
      if (closedByEffect || socket?.readyState === WebSocket.OPEN || socket?.readyState === WebSocket.CONNECTING) {
        return
      }
      const myGen = ++generation
      let ws: WebSocket
      try {
        ws = new WebSocket(url)
        socket = ws
      } catch {
        setConnected(false)
        scheduleReconnect()
        return
      }

      ws.onopen = () => {
        if (myGen !== generation) return
        delay = RECONNECT_BASE_MS
        setConnected(true)
        if (fallbackTimer) {
          clearInterval(fallbackTimer)
          fallbackTimer = null
        }
        cbRef.current.onOpen?.()
      }

      ws.onmessage = (event) => {
        if (myGen !== generation) return
        try {
          const payload = JSON.parse(String(event.data)) as Record<string, unknown>
          cbRef.current.onEvent?.(payload)
        } catch {
          /* ignore malformed frames */
        }
      }

      ws.onclose = () => {
        if (myGen !== generation) return
        socket = null
        setConnected(false)
        scheduleReconnect()
      }

      ws.onerror = () => {
        if (myGen === generation) ws.close()
      }
    }

    const scheduleReconnect = () => {
      if (closedByEffect || reconnectTimer) return
      startFallback()
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null
        connect()
      }, delay)
      delay = Math.min(delay * 2, RECONNECT_MAX_MS)
    }

    connect()

    return () => {
      closedByEffect = true
      generation += 1
      clearTimers()
      if (socket) {
        socket.onclose = null
        socket.onerror = null
        socket.onopen = null
        socket.onmessage = null
        try {
          socket.close()
        } catch {
          /* already closed */
        }
        socket = null
      }
      setConnected(false)
    }
  }, [url, enabled, fallbackMs])

  return { connected }
}
