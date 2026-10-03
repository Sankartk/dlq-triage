import { useEffect, useState } from "react";
import { ApiError, clearToken, getToken, gql, setToken } from "./api";
import { useHashRoute } from "./hooks";
import { Overview } from "./pages/Overview";
import { GroupPage } from "./pages/GroupPage";
import { History } from "./pages/History";

export interface Me { name: string; role: string }

export function App() {
  const [me, setMe] = useState<Me | null>(null);
  const [aiEnabled, setAiEnabled] = useState(false);
  const [loginError, setLoginError] = useState("");
  const [checking, setChecking] = useState(!!getToken());
  const [route, go] = useHashRoute();

  const signIn = async () => {
    try {
      const d = await gql<{ me: Me; aiEnabled: boolean }>("{ me { name role } aiEnabled }");
      setMe(d.me);
      setAiEnabled(d.aiEnabled);
      setLoginError("");
    } catch (e) {
      clearToken();
      setMe(null);
      setLoginError(e instanceof ApiError && e.status === 401 ? "That token was not accepted." : (e as Error).message);
    } finally {
      setChecking(false);
    }
  };

  useEffect(() => {
    if (getToken()) void signIn();
  }, []);

  if (checking) return <div className="center muted">Loading...</div>;
  if (!me) return <Login error={loginError} onSubmit={(t) => { setToken(t); setChecking(true); void signIn(); }} />;

  const parts = route.split("/").filter(Boolean);
  let page;
  if (parts[0] === "q" && parts[2] === "g" && parts[1] && parts[3]) {
    page = <GroupPage queue={decodeURIComponent(parts[1])} groupKey={decodeURIComponent(parts[3])} me={me} aiEnabled={aiEnabled} go={go} />;
  } else if (parts[0] === "history") {
    page = <History go={go} />;
  } else {
    page = <Overview go={go} />;
  }

  return (
    <>
      <header className="top">
        <a className="brand" href="#/">dlq-triage</a>
        <nav>
          <a href="#/" className={parts[0] === undefined || parts[0] === "q" ? "on" : ""}>Failures</a>
          <a href="#/history" className={parts[0] === "history" ? "on" : ""}>History</a>
        </nav>
        <span className="who">
          {me.name} <span className={`pill ${me.role}`}>{me.role}</span>
          <button className="link" onClick={() => { clearToken(); setMe(null); }}>Sign out</button>
        </span>
      </header>
      <main>{page}</main>
    </>
  );
}

function Login({ error, onSubmit }: { error: string; onSubmit: (t: string) => void }) {
  const [t, setT] = useState("");
  return (
    <form className="login" onSubmit={(e) => { e.preventDefault(); if (t.trim()) onSubmit(t.trim()); }}>
      <h1>dlq-triage</h1>
      <label htmlFor="tok">Access token</label>
      <input id="tok" type="password" autoComplete="off" value={t} onChange={(e) => setT(e.target.value)} autoFocus />
      {error && <p className="err" role="alert">{error}</p>}
      <button type="submit">Sign in</button>
      <p className="muted small">The token is kept for this browser tab only.</p>
    </form>
  );
}
