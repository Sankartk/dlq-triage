const TOKEN_KEY = "dlq-triage-token";

export const getToken = () => sessionStorage.getItem(TOKEN_KEY) ?? "";
export const setToken = (t: string) => sessionStorage.setItem(TOKEN_KEY, t);
export const clearToken = () => sessionStorage.removeItem(TOKEN_KEY);

export class ApiError extends Error {
  constructor(message: string, public status: number) {
    super(message);
  }
}

export async function gql<T>(query: string, variables?: Record<string, unknown>): Promise<T> {
  const res = await fetch("/graphql", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${getToken()}` },
    body: JSON.stringify({ query, variables }),
  });
  let body: { data?: T; errors?: { message: string }[] } = {};
  try {
    body = await res.json();
  } catch {
    // fall through to the status check below
  }
  if (body.errors?.length) throw new ApiError(body.errors.map((e) => e.message).join("; "), res.status);
  if (!res.ok || !body.data) throw new ApiError(`request failed (${res.status})`, res.status);
  return body.data;
}

export interface Depth { visible: number; inFlight: number; delayed: number }
export interface ScanStatus { at: string; seen: number; new: number; error: string | null; skipped: boolean }
export interface Group {
  queue: string; key: string; errorSignature: string; shape: string; count: number; pending: number;
  firstSeen: string; lastSeen: string; aiSummary: string | null; aiSummaryAt: string | null;
}
export interface Queue {
  name: string; depth: Depth | null; destDepth: Depth | null; totalPending: number;
  groups: Group[]; lastScan: ScanStatus | null;
}
export interface Attribute { name: string; dataType: string; value: string }
export interface Message {
  id: string; status: string; receiveCount: number; sentAt: string | null; ingestedAt: string;
  replayedAt: string | null; attributes: Attribute[]; body: string | null;
}
export interface Job {
  id: string; queue: string; group: string; dryRun: boolean; status: string; requestedBy: string;
  ratePerSecond: number; maxMessages: number; total: number; replayed: number; skipped: number;
  failed: number; error: string | null; createdAt: string; finishedAt: string | null;
}
export interface AuditEntry {
  id: string; at: string; actor: string; action: string; queue: string; group: string | null;
  jobId: string | null; detail: string;
}
