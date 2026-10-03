import { useState } from "react";
import { ApiError, gql, Group, Job, Message } from "../api";
import { ago, usePoll } from "../hooks";
import type { Me } from "../App";

const GROUP = `query($q:String!,$k:String!,$limit:Int){
  group(queue:$q,key:$k){ queue key errorSignature shape count pending firstSeen lastSeen aiSummary aiSummaryAt
    messages(limit:$limit){ id status receiveCount sentAt ingestedAt attributes{name dataType value} body } }
  replayJobs(queue:$q,limit:50){ id group dryRun status requestedBy total replayed skipped failed error createdAt finishedAt ratePerSecond maxMessages } }`;

const START = `mutation($in:ReplayInput!){ startReplay(input:$in){ id dryRun status total replayed skipped failed error } }`;
const CANCEL = `mutation($id:ID!){ cancelReplay(id:$id) }`;
const SUMMARIZE = `mutation($q:String!,$k:String!){ summarizeGroup(queue:$q,key:$k){ aiSummary aiSummaryAt } }`;

interface Data { group: (Group & { messages: Message[] }) | null; replayJobs: Job[] }

export function GroupPage({ queue, groupKey, me, aiEnabled, go }:
  { queue: string; groupKey: string; me: Me; aiEnabled: boolean; go: (to: string) => void }) {
  const { data, error, reload } = usePoll(
    () => gql<Data>(GROUP, { q: queue, k: groupKey, limit: 10 }), 3000, [queue, groupKey]);
  const [rate, setRate] = useState(5);
  const [max, setMax] = useState(1000);
  const [dry, setDry] = useState<Job | null>(null);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState("");

  const g = data?.group;
  const canOperate = me.role === "operator";
  const jobs = (data?.replayJobs ?? []).filter((j) => j.group === groupKey);
  const active = jobs.find((j) => !j.dryRun && j.status === "running");

  const act = async (fn: () => Promise<void>) => {
    setBusy(true); setActionError("");
    try { await fn(); reload(); } catch (e) { setActionError((e as ApiError).message); } finally { setBusy(false); }
  };

  const start = (dryRun: boolean) => act(async () => {
    const d = await gql<{ startReplay: Job }>(START, { in: { queue, group: groupKey, dryRun, ratePerSecond: rate, maxMessages: max } });
    if (dryRun) setDry(d.startReplay); else setDry(null);
  });

  if (!data && !error) return <p className="muted">Loading...</p>;
  if (!g) return <><p className="err">{error ? error.message : "This failure group no longer exists."}</p><a href="#/">Back</a></>;

  const wouldReplay = dry ? dry.total : 0;

  return (
    <>
      <p><a href="#/" onClick={(e) => { e.preventDefault(); go("/"); }}>Failures</a> / {queue}</p>
      <section className="card">
        <h2 className="sig">{g.errorSignature}</h2>
        <p className="muted mono small">{g.shape}</p>
        <div className="stats">
          <div className="stat"><div className="muted small">Pending</div><div className="big">{g.pending}</div></div>
          <div className="stat"><div className="muted small">Seen in total</div><div className="big">{g.count}</div></div>
          <div className="stat"><div className="muted small">First / last seen</div><div>{ago(g.firstSeen)} / {ago(g.lastSeen)}</div></div>
        </div>
      </section>

      <section className="card">
        <h3>Summary</h3>
        {g.aiSummary ? (
          <>
            <p>{g.aiSummary}</p>
            <p className="muted small">Machine-generated from the error text and payload structure only, {ago(g.aiSummaryAt)}. Check it before acting on it.</p>
          </>
        ) : (
          <p className="muted">{aiEnabled ? "No summary yet." : "AI summaries are not enabled on this server."}</p>
        )}
        {canOperate && aiEnabled && (
          <button disabled={busy} onClick={() => act(async () => { await gql(SUMMARIZE, { q: queue, k: groupKey }); })}>
            {g.aiSummary ? "Regenerate" : "Generate summary"}
          </button>
        )}
      </section>

      <section className="card">
        <h3>Replay</h3>
        {!canOperate ? (
          <p className="muted">Your role is read-only. Replay needs an operator token.</p>
        ) : active ? (
          <>
            <Progress job={active} />
            <button className="danger" disabled={busy} onClick={() => act(async () => { await gql(CANCEL, { id: active.id }); })}>Cancel replay</button>
          </>
        ) : (
          <>
            <div className="row gap">
              <label>Messages per second
                <input type="number" min={0.1} step={0.5} value={rate} onChange={(e) => { setRate(Number(e.target.value)); setDry(null); }} />
              </label>
              <label>At most
                <input type="number" min={1} value={max} onChange={(e) => { setMax(Number(e.target.value)); setDry(null); }} />
              </label>
            </div>
            <div className="row gap">
              <button disabled={busy || g.pending === 0} onClick={() => void start(true)}>1. Dry run</button>
              <button className="danger" disabled={busy || !dry || wouldReplay === 0} onClick={() => {
                if (window.confirm(`Send ${wouldReplay} message(s) back to the destination queue at ${rate}/s? This cannot be undone.`)) void start(false);
              }}>2. Replay {dry ? wouldReplay : ""} message(s)</button>
            </div>
            {dry && <p className="muted small">Dry run: {dry.total} message(s) would be sent. Nothing was changed.</p>}
            <p className="muted small">Delivery is at-least-once. The consuming service must tolerate seeing a message twice.</p>
          </>
        )}
        {actionError && <p className="err" role="alert">{actionError}</p>}
      </section>

      <section className="card">
        <h3>Sample messages</h3>
        <table>
          <thead><tr><th>Id</th><th>Status</th><th className="num">Receives</th><th>Ingested</th></tr></thead>
          <tbody>
            {g.messages.map((m) => (
              <MessageRow key={m.id} m={m} />
            ))}
          </tbody>
        </table>
        {me.role !== "operator" && <p className="muted small">Message bodies and attribute values are hidden for read-only users.</p>}
      </section>

      <section className="card">
        <h3>Replay history for this failure</h3>
        {jobs.length === 0 ? <p className="muted">None yet.</p> : (
          <table>
            <thead><tr><th>Started</th><th>Type</th><th>Status</th><th className="num">Sent</th><th className="num">Failed</th><th>By</th></tr></thead>
            <tbody>
              {jobs.map((j) => (
                <tr key={j.id}>
                  <td>{ago(j.createdAt)}</td><td>{j.dryRun ? "dry run" : "replay"}</td><td>{j.status}{j.error ? `: ${j.error}` : ""}</td>
                  <td className="num">{j.dryRun ? "-" : `${j.replayed}/${j.total}`}</td><td className="num">{j.failed}</td><td>{j.requestedBy}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </>
  );
}

function Progress({ job }: { job: Job }) {
  const done = job.replayed + job.skipped + job.failed;
  const pct = job.total ? Math.min(100, Math.round((done / job.total) * 100)) : 0;
  return (
    <div>
      <div className="bar" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}><div style={{ width: `${pct}%` }} /></div>
      <p className="small">{job.replayed} sent, {job.failed} failed, {job.skipped} skipped of {job.total}</p>
    </div>
  );
}

function MessageRow({ m }: { m: Message }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <tr className="click" onClick={() => setOpen(!open)}>
        <td className="mono small">{m.id}</td><td>{m.status}</td><td className="num">{m.receiveCount}</td><td className="muted">{ago(m.ingestedAt)}</td>
      </tr>
      {open && (
        <tr><td colSpan={4}>
          {m.body !== null ? <pre>{m.body}</pre> : <p className="muted small">Body hidden for your role.</p>}
          {m.attributes.length > 0 && (
            <ul className="small">{m.attributes.map((a) => <li key={a.name}><b>{a.name}</b> ({a.dataType}): {a.value || "hidden"}</li>)}</ul>
          )}
        </td></tr>
      )}
    </>
  );
}
