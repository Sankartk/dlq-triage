import { AuditEntry, gql, Job, Queue } from "../api";
import { ago, usePoll } from "../hooks";

const QUERY = `{ queues { name }
  audit(limit:100){ id at actor action queue group detail } }`;

export function History({ go }: { go: (to: string) => void }) {
  const { data, error } = usePoll(() => gql<{ queues: Pick<Queue, "name">[]; audit: AuditEntry[] }>(QUERY), 5000, []);
  if (!data && !error) return <p className="muted">Loading...</p>;
  return (
    <section className="card">
      <h2>Audit log</h2>
      {error && <p className="err" role="alert">{error.message}</p>}
      {data?.audit.length === 0 ? <p className="muted">Nothing recorded yet.</p> : (
        <table>
          <thead><tr><th>When</th><th>Who</th><th>Action</th><th>Queue</th><th>Detail</th></tr></thead>
          <tbody>
            {data?.audit.map((a) => (
              <tr key={a.id} className={a.group ? "click" : ""} onClick={() => a.group && go(`/q/${encodeURIComponent(a.queue)}/g/${a.group}`)}>
                <td className="muted" title={a.at}>{ago(a.at)}</td><td>{a.actor}</td><td className="mono small">{a.action}</td><td>{a.queue}</td><td>{a.detail}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

export type { Job };
