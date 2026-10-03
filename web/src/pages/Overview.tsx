import { gql, Queue } from "../api";
import { ago, usePoll } from "../hooks";

const QUERY = `{ queues { name totalPending
  depth { visible inFlight delayed } destDepth { visible inFlight delayed }
  lastScan { at seen new error skipped }
  groups { queue key errorSignature shape count pending firstSeen lastSeen aiSummary } } }`;

export function Overview({ go }: { go: (to: string) => void }) {
  const { data, error } = usePoll(() => gql<{ queues: Queue[] }>(QUERY).then((d) => d.queues), 5000, []);

  if (!data && !error) return <p className="muted">Loading...</p>;
  return (
    <>
      {error && <p className="err" role="alert">Could not refresh: {error.message}</p>}
      {data?.length === 0 && <p className="muted">No queues are configured.</p>}
      {data?.map((q) => (
        <section key={q.name} className="card">
          <div className="row between">
            <h2>{q.name}</h2>
            <ScanLine scan={q.lastScan} />
          </div>
          <div className="stats">
            <Stat label="Waiting in DLQ" value={q.depth ? q.depth.visible : null} note={q.depth ? `${q.depth.inFlight} in flight` : "queue unreachable"} />
            <Stat label="Pending in groups" value={q.totalPending} note="not yet replayed" />
            <Stat label="Destination" value={q.destDepth ? q.destDepth.visible : null} note={q.destDepth ? `${q.destDepth.inFlight} in flight` : "unreachable"} />
          </div>
          {q.groups.length === 0 ? (
            <p className="muted">No dead-lettered messages have been seen.</p>
          ) : (
            <table>
              <thead>
                <tr><th>Failure</th><th className="num">Pending</th><th className="num">Total</th><th>Last seen</th></tr>
              </thead>
              <tbody>
                {[...q.groups].sort((a, b) => b.pending - a.pending || b.count - a.count).map((g) => (
                  <tr key={g.key} className="click" tabIndex={0}
                      onClick={() => go(`/q/${encodeURIComponent(q.name)}/g/${g.key}`)}
                      onKeyDown={(e) => { if (e.key === "Enter") go(`/q/${encodeURIComponent(q.name)}/g/${g.key}`); }}>
                    <td>
                      <div className="sig">{g.errorSignature}</div>
                      <div className="muted small mono">{g.shape}</div>
                    </td>
                    <td className="num">{g.pending}</td>
                    <td className="num">{g.count}</td>
                    <td className="muted">{ago(g.lastSeen)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      ))}
    </>
  );
}

function Stat({ label, value, note }: { label: string; value: number | null; note: string }) {
  return (
    <div className="stat">
      <div className="muted small">{label}</div>
      <div className="big">{value ?? "?"}</div>
      <div className="muted small">{note}</div>
    </div>
  );
}

function ScanLine({ scan }: { scan: Queue["lastScan"] }) {
  if (!scan) return <span className="muted small">not scanned yet</span>;
  if (scan.error) return <span className="err small">last scan failed {ago(scan.at)}: {scan.error}</span>;
  if (scan.skipped) return <span className="muted small">scan paused during replay</span>;
  return <span className="muted small">scanned {ago(scan.at)}: {scan.seen} seen, {scan.new} new</span>;
}
