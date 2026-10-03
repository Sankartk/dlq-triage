import { useCallback, useEffect, useRef, useState } from "react";

export function useHashRoute(): [string, (to: string) => void] {
  const read = () => window.location.hash.replace(/^#/, "") || "/";
  const [route, setRoute] = useState(read);
  useEffect(() => {
    const on = () => setRoute(read());
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  return [route, (to) => { window.location.hash = to; }];
}

export interface Loaded<T> {
  data: T | null;
  error: Error | null;
  loading: boolean;
  reload: () => void;
}

// Loads on mount and every intervalMs; a failed refresh keeps the last data.
export function usePoll<T>(load: () => Promise<T>, intervalMs: number, deps: unknown[]): Loaded<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const loadRef = useRef(load);
  loadRef.current = load;

  const run = useCallback(async (alive: { v: boolean }) => {
    try {
      const d = await loadRef.current();
      if (alive.v) { setData(d); setError(null); }
    } catch (e) {
      if (alive.v) setError(e as Error);
    } finally {
      if (alive.v) setLoading(false);
    }
  }, []);

  const [tick, setTick] = useState(0);
  useEffect(() => {
    const alive = { v: true };
    setLoading(true);
    void run(alive);
    const id = window.setInterval(() => void run(alive), intervalMs);
    return () => { alive.v = false; window.clearInterval(id); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, tick, run, ...deps]);

  return { data, error, loading, reload: () => setTick((t) => t + 1) };
}

export function ago(iso: string | null | undefined): string {
  if (!iso) return "-";
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}
