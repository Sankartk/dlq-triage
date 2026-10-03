package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sankartk/dlq-triage/internal/ai"
	"github.com/Sankartk/dlq-triage/internal/config"
	"github.com/Sankartk/dlq-triage/internal/domain"
	"github.com/Sankartk/dlq-triage/internal/ingest"
	"github.com/Sankartk/dlq-triage/internal/queue"
	"github.com/Sankartk/dlq-triage/internal/replay"
	"github.com/Sankartk/dlq-triage/internal/store"
)

const (
	dlqURL  = "mem://orders-dlq"
	destURL = "mem://orders"
	viewerT = "viewer-token-0123456789"
	opT     = "operator-token-0123456789"
)

type env struct {
	t    *testing.T
	srv  *httptest.Server
	st   *store.SQLite
	mem  *queue.Memory
	eng  *replay.Engine
	sum  *fakeSummarizer
	keys []string // group keys, biggest first
}

type fakeSummarizer struct {
	calls atomic.Int32
	err   error
	last  ai.Input
}

func (f *fakeSummarizer) Summarize(_ context.Context, in ai.Input) (string, error) {
	f.calls.Add(1)
	f.last = in
	if f.err != nil {
		return "", f.err
	}
	return "Summary: " + in.ErrorSig, nil
}

type nullLogger struct{}

func newEnv(t *testing.T, summarizer ai.Summarizer) *env {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	mem := queue.NewMemory()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	n := 0
	eng := replay.New(context.Background(), st, mem, replay.Config{
		Visibility: 30 * time.Millisecond, PollInterval: 5 * time.Millisecond, Patience: 200 * time.Millisecond, TagAttributes: true,
	}, log, func() string { n++; return fmt.Sprintf("job-%d", n) })
	t.Cleanup(eng.Shutdown)

	e := &env{t: t, st: st, mem: mem, eng: eng}
	if fs, ok := summarizer.(*fakeSummarizer); ok {
		e.sum = fs
	}
	metrics := NewMetrics(prometheus.NewRegistry())
	auth := NewAuthenticator([]config.Token{
		{Name: "vera", Token: viewerT, Role: config.RoleViewer},
		{Name: "olga", Token: opT, Role: config.RoleOperator},
	}, false)
	srv, err := NewServer(Options{
		Deps: Deps{
			Store: st, Queue: mem, Engine: eng, Summarizer: summarizer, Log: log,
			Queues: []replay.Queue{{Name: "orders", DLQURL: dlqURL, DestURL: destURL}},
		},
		Auth: auth, Metrics: metrics,
		Ready: func(ctx context.Context) error { return st.Ping(ctx) },
	})
	if err != nil {
		t.Fatal(err)
	}
	e.srv = httptest.NewServer(srv.Handler())
	t.Cleanup(e.srv.Close)
	e.seed()
	return e
}

func (e *env) seed() {
	e.t.Helper()
	ing := ingest.New(e.st, e.mem, ingest.Config{
		Fingerprint: fingerprintConfig(), Visibility: 30 * time.Millisecond,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i := 1; i <= 4; i++ {
		e.mem.Seed(dlqURL, domain.Message{
			Body:         fmt.Sprintf(`{"loan":"SECRET-%d","ssn":"123-45-6789"}`, i),
			ReceiveCount: 3,
			Attributes: map[string]domain.Attribute{
				"ErrorMessage": domain.StringAttr(fmt.Sprintf("Loan %d not found", i)),
				"Owner":        domain.StringAttr("alice@example.com"),
			},
		})
	}
	e.mem.Seed(dlqURL, domain.Message{Body: `{"loan":"T"}`, Attributes: map[string]domain.Attribute{"ErrorMessage": domain.StringAttr("Timeout after 30 seconds")}})
	if _, err := ing.Scan(context.Background(), ingest.Source{Name: "orders", DLQURL: dlqURL}); err != nil {
		e.t.Fatal(err)
	}
	groups, _ := e.st.ListGroups(context.Background(), "orders")
	for _, g := range groups {
		e.keys = append(e.keys, g.Key)
	}
}

type gqlResp struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func (e *env) do(token, query string, vars map[string]any) (int, gqlResp) {
	e.t.Helper()
	b, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/graphql", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out gqlResp
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (e *env) ok(token, query string, vars map[string]any, into any) {
	e.t.Helper()
	status, r := e.do(token, query, vars)
	if status != 200 || len(r.Errors) > 0 {
		e.t.Fatalf("status=%d errors=%+v", status, r.Errors)
	}
	if into != nil {
		if err := json.Unmarshal(r.Data, into); err != nil {
			e.t.Fatal(err)
		}
	}
}

func (e *env) fails(token, query string, vars map[string]any, want string) {
	e.t.Helper()
	_, r := e.do(token, query, vars)
	if len(r.Errors) == 0 {
		e.t.Fatalf("expected an error containing %q, got data %s", want, r.Data)
	}
	if !strings.Contains(r.Errors[0].Message, want) {
		e.t.Fatalf("error %q does not contain %q", r.Errors[0].Message, want)
	}
}

func TestRejectsMissingAndWrongTokens(t *testing.T) {
	e := newEnv(t, nil)
	for name, tok := range map[string]string{"none": "", "wrong": "nope-nope-nope-nope-nope", "prefix": viewerT[:10], "suffix": viewerT + "x"} {
		status, _ := e.do(tok, `{ me { name } }`, nil)
		if status != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", name, status)
		}
	}
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/graphql", strings.NewReader(`{"query":"{ me { name } }"}`))
	req.Header.Set("Authorization", "Basic "+viewerT)
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") == "" {
		t.Errorf("basic auth: status=%d", resp.StatusCode)
	}
}

func TestMeReturnsAuthenticatedPrincipal(t *testing.T) {
	e := newEnv(t, nil)
	var out struct{ Me struct{ Name, Role string } }
	e.ok(viewerT, `{ me { name role } }`, nil, &out)
	if out.Me.Name != "vera" || out.Me.Role != "viewer" {
		t.Fatalf("me = %+v", out.Me)
	}
}

func TestQueueOverviewShowsGroupsAndDepth(t *testing.T) {
	e := newEnv(t, nil)
	var out struct {
		Queue struct {
			Name         string
			TotalPending int
			Depth        struct{ Visible, InFlight int }
			DestDepth    struct{ Visible int }
			LastScan     *struct{ Seen int }
			Groups       []struct {
				Key            string
				ErrorSignature string
				Count, Pending int
			}
		}
	}
	e.ok(viewerT, `{ queue(name: "orders") { name totalPending depth { visible inFlight } destDepth { visible }
	  lastScan { seen } groups { key errorSignature count pending } } }`, nil, &out)
	q := out.Queue
	if q.Name != "orders" || q.TotalPending != 5 || len(q.Groups) != 2 {
		t.Fatalf("queue = %+v", q)
	}
	if q.Groups[0].Pending != 4 || q.Groups[0].ErrorSignature != "Loan <n> not found" || q.Groups[1].Pending != 1 {
		t.Fatalf("groups = %+v", q.Groups)
	}
	if q.Depth.Visible+q.Depth.InFlight != 5 {
		t.Fatalf("depth = %+v", q.Depth)
	}
}

func TestUnknownQueueIsNullOrError(t *testing.T) {
	e := newEnv(t, nil)
	var out struct{ Queue *struct{ Name string } }
	e.ok(viewerT, `{ queue(name: "nope") { name } }`, nil, &out)
	if out.Queue != nil {
		t.Fatalf("queue = %+v", out.Queue)
	}
	e.fails(viewerT, `{ replayJobs(queue: "nope") { id } }`, nil, "unknown queue")
	e.fails(viewerT, `{ audit(queue: "nope") { id } }`, nil, "unknown queue")
}

func TestViewerCannotSeeBodiesOrAttributeValues(t *testing.T) {
	e := newEnv(t, nil)
	q := `query($k: String!) { group(queue: "orders", key: $k) { messages { id body attributes { name dataType value } } } }`
	var viewer, op struct {
		Group struct {
			Messages []struct {
				ID         string
				Body       *string
				Attributes []struct{ Name, DataType, Value string }
			}
		}
	}
	e.ok(viewerT, q, map[string]any{"k": e.keys[0]}, &viewer)
	e.ok(opT, q, map[string]any{"k": e.keys[0]}, &op)

	if len(viewer.Group.Messages) != 4 || len(op.Group.Messages) != 4 {
		t.Fatalf("messages: viewer=%d operator=%d", len(viewer.Group.Messages), len(op.Group.Messages))
	}
	for _, m := range viewer.Group.Messages {
		if m.Body != nil {
			t.Fatalf("viewer saw a body: %q", *m.Body)
		}
		for _, a := range m.Attributes {
			if a.Value != "[hidden]" || a.Name == "" || a.DataType != "String" {
				t.Fatalf("viewer attribute = %+v", a)
			}
		}
	}
	for _, m := range op.Group.Messages {
		if m.Body == nil || !strings.Contains(*m.Body, "SECRET-") {
			t.Fatalf("operator should see the body: %+v", m.Body)
		}
		var sawOwner bool
		for _, a := range m.Attributes {
			if a.Name == "Owner" && a.Value == "alice@example.com" {
				sawOwner = true
			}
		}
		if !sawOwner {
			t.Fatalf("operator attributes = %+v", m.Attributes)
		}
	}
}

func TestViewerCannotMutate(t *testing.T) {
	e := newEnv(t, &fakeSummarizer{})
	start := `mutation($k: String!) { startReplay(input: {queue: "orders", group: $k, dryRun: false, ratePerSecond: 10, maxMessages: 10}) { id } }`
	e.fails(viewerT, start, map[string]any{"k": e.keys[0]}, "operator role")
	e.fails(viewerT, `mutation { cancelReplay(id: "x") }`, nil, "operator role")
	e.fails(viewerT, `mutation($k: String!) { summarizeGroup(queue: "orders", key: $k) { key } }`, map[string]any{"k": e.keys[0]}, "operator role")

	if n := len(e.mem.Messages(destURL)); n != 0 {
		t.Fatalf("viewer caused %d messages to be sent", n)
	}
	if e.sum.calls.Load() != 0 {
		t.Fatal("viewer triggered an AI call")
	}
}

func TestOperatorDryRunThenReplay(t *testing.T) {
	e := newEnv(t, nil)
	mut := `mutation($k: String!, $dry: Boolean!) { startReplay(input: {queue: "orders", group: $k, dryRun: $dry, ratePerSecond: 100, maxMessages: 100})
	  { id status dryRun total requestedBy } }`
	var dry struct {
		StartReplay struct {
			ID, Status, RequestedBy string
			DryRun                  bool
			Total                   int
		}
	}
	e.ok(opT, mut, map[string]any{"k": e.keys[0], "dry": true}, &dry)
	if !dry.StartReplay.DryRun || dry.StartReplay.Total != 4 || dry.StartReplay.RequestedBy != "olga" {
		t.Fatalf("dry run = %+v", dry.StartReplay)
	}
	if len(e.mem.Messages(destURL)) != 0 {
		t.Fatal("dry run sent messages")
	}

	var real struct{ StartReplay struct{ ID string } }
	e.ok(opT, mut, map[string]any{"k": e.keys[0], "dry": false}, &real)
	e.eng.Wait()

	var job struct {
		ReplayJob struct {
			Status                  string
			Replayed, Failed, Total int
			RequestedBy             string
			FinishedAt              *string
		}
	}
	e.ok(viewerT, `query($id: ID!) { replayJob(id: $id) { status replayed failed total requestedBy finishedAt } }`, map[string]any{"id": real.StartReplay.ID}, &job)
	j := job.ReplayJob
	if j.Status != "completed" || j.Replayed != 4 || j.Failed != 0 || j.RequestedBy != "olga" || j.FinishedAt == nil {
		t.Fatalf("job = %+v", j)
	}
	if n := len(e.mem.Messages(destURL)); n != 4 {
		t.Fatalf("destination has %d messages", n)
	}

	var audit struct {
		Audit []struct{ Actor, Action string }
	}
	e.ok(viewerT, `{ audit(queue: "orders") { actor action } }`, nil, &audit)
	var actions []string
	for i := len(audit.Audit) - 1; i >= 0; i-- {
		actions = append(actions, audit.Audit[i].Actor+":"+audit.Audit[i].Action)
	}
	want := "olga:replay.dryrun olga:replay.start olga:replay.finish"
	if strings.Join(actions, " ") != want {
		t.Fatalf("audit = %v, want %s", actions, want)
	}
}

func TestReplayInputIsValidatedAndActorCannotBeSpoofed(t *testing.T) {
	e := newEnv(t, nil)
	k := e.keys[0]
	cases := map[string]string{
		"rate zero":     fmt.Sprintf(`mutation { startReplay(input: {queue: "orders", group: "%s", dryRun: false, ratePerSecond: 0, maxMessages: 5}) { id } }`, k),
		"rate too high": fmt.Sprintf(`mutation { startReplay(input: {queue: "orders", group: "%s", dryRun: false, ratePerSecond: 100000, maxMessages: 5}) { id } }`, k),
		"max zero":      fmt.Sprintf(`mutation { startReplay(input: {queue: "orders", group: "%s", dryRun: false, ratePerSecond: 5, maxMessages: 0}) { id } }`, k),
	}
	for name, q := range cases {
		e.fails(opT, q, nil, "invalid replay request")
		_ = name
	}
	e.fails(opT, `mutation { startReplay(input: {queue: "nope", group: "x", dryRun: true, ratePerSecond: 1, maxMessages: 1}) { id } }`, nil, "unknown queue")
	e.fails(opT, `mutation { startReplay(input: {queue: "orders", group: "nope", dryRun: true, ratePerSecond: 1, maxMessages: 1}) { id } }`, nil, "group not found")
	// The schema has no actor field, so a client cannot choose who it is.
	e.fails(opT, fmt.Sprintf(`mutation { startReplay(input: {queue: "orders", group: "%s", dryRun: true, ratePerSecond: 1, maxMessages: 1, actor: "root"}) { id } }`, k), nil, "actor")
}

func TestSecondRealReplayOfSameGroupIsRejected(t *testing.T) {
	e := newEnv(t, nil)
	mut := fmt.Sprintf(`mutation { startReplay(input: {queue: "orders", group: "%s", dryRun: false, ratePerSecond: 1, maxMessages: 10}) { id } }`, e.keys[0])
	var first struct{ StartReplay struct{ ID string } }
	e.ok(opT, mut, nil, &first)
	e.fails(opT, mut, nil, "already running")
	var cancelled struct{ CancelReplay bool }
	e.ok(opT, `mutation($id: ID!) { cancelReplay(id: $id) }`, map[string]any{"id": first.StartReplay.ID}, &cancelled)
	if !cancelled.CancelReplay {
		t.Fatal("cancel returned false for a running job")
	}
	e.eng.Wait()
	var job struct{ ReplayJob struct{ Status string } }
	e.ok(opT, `query($id: ID!) { replayJob(id: $id) { status } }`, map[string]any{"id": first.StartReplay.ID}, &job)
	if job.ReplayJob.Status != "cancelled" {
		t.Fatalf("status = %q", job.ReplayJob.Status)
	}
	e.fails(opT, `mutation { cancelReplay(id: "does-not-exist") }`, nil, "job not found")
	var again struct{ CancelReplay bool }
	e.ok(opT, `mutation($id: ID!) { cancelReplay(id: $id) }`, map[string]any{"id": first.StartReplay.ID}, &again)
	if again.CancelReplay {
		t.Fatal("cancelling a finished job should report false")
	}
}

func TestSummarizeStoresAndAudits(t *testing.T) {
	sum := &fakeSummarizer{}
	e := newEnv(t, sum)
	var flag struct{ AiEnabled bool }
	e.ok(viewerT, `{ aiEnabled }`, nil, &flag)
	if !flag.AiEnabled {
		t.Fatal("aiEnabled should be true with a summarizer")
	}
	var out struct {
		SummarizeGroup struct {
			AiSummary   string
			AiSummaryAt *string
		}
	}
	e.ok(opT, `mutation($k: String!) { summarizeGroup(queue: "orders", key: $k) { aiSummary aiSummaryAt } }`, map[string]any{"k": e.keys[0]}, &out)
	if out.SummarizeGroup.AiSummary != "Summary: Loan <n> not found" || out.SummarizeGroup.AiSummaryAt == nil {
		t.Fatalf("summary = %+v", out.SummarizeGroup)
	}
	// What the AI saw: signature and shape only.
	if sum.last.ErrorSig != "Loan <n> not found" || strings.Contains(sum.last.Shape, "SECRET") || strings.Contains(sum.last.Shape, "123-45") {
		t.Fatalf("summarizer input = %+v", sum.last)
	}
	var again struct{ Group struct{ AiSummary string } }
	e.ok(viewerT, `query($k: String!) { group(queue: "orders", key: $k) { aiSummary } }`, map[string]any{"k": e.keys[0]}, &again)
	if again.Group.AiSummary == "" {
		t.Fatal("summary was not persisted")
	}
	var audit struct{ Audit []struct{ Action string } }
	e.ok(viewerT, `{ audit(queue: "orders", limit: 1) { action } }`, nil, &audit)
	if len(audit.Audit) != 1 || audit.Audit[0].Action != "group.summarize" {
		t.Fatalf("audit = %+v", audit.Audit)
	}
}

func TestSummarizeFailuresDoNotLeakDetails(t *testing.T) {
	sum := &fakeSummarizer{err: errors.New("provider said: key sk-secret-123 is invalid")}
	e := newEnv(t, sum)
	_, r := e.do(opT, `mutation($k: String!) { summarizeGroup(queue: "orders", key: $k) { key } }`, map[string]any{"k": e.keys[0]})
	if len(r.Errors) == 0 || strings.Contains(r.Errors[0].Message, "sk-secret") {
		t.Fatalf("errors = %+v", r.Errors)
	}
}

func TestSummarizeWhenAIDisabled(t *testing.T) {
	e := newEnv(t, nil)
	var flag struct{ AiEnabled bool }
	e.ok(viewerT, `{ aiEnabled }`, nil, &flag)
	if flag.AiEnabled {
		t.Fatal("aiEnabled should be false by default")
	}
	e.fails(opT, `mutation($k: String!) { summarizeGroup(queue: "orders", key: $k) { key } }`, map[string]any{"k": e.keys[0]}, "not enabled")
}

func TestListLimitsAreClamped(t *testing.T) {
	e := newEnv(t, nil)
	var out struct {
		Group struct{ Messages []struct{ ID string } }
	}
	e.ok(viewerT, `query($k: String!) { group(queue: "orders", key: $k) { messages(limit: 999999, offset: 999999999) { id } } }`, map[string]any{"k": e.keys[0]}, &out)
	if len(out.Group.Messages) != 0 {
		t.Fatalf("huge offset returned %d messages", len(out.Group.Messages))
	}
	e.ok(viewerT, `query($k: String!) { group(queue: "orders", key: $k) { messages(limit: -5) { id } } }`, map[string]any{"k": e.keys[0]}, &out)
	if len(out.Group.Messages) != 4 {
		t.Fatalf("negative limit should fall back to the default, got %d", len(out.Group.Messages))
	}
}

func TestQueryDepthIsLimited(t *testing.T) {
	e := newEnv(t, nil)
	e.ok(viewerT, `{ queue(name: "orders") { groups { messages { attributes { name } } } } }`, nil, nil)

	// Introspection types refer to each other, so a query can nest without bound.
	// Without a depth limit this would be an easy way to burn CPU.
	nest := strings.Repeat("ofType { ", 30) + "name" + strings.Repeat(" }", 30)
	e.fails(viewerT, `{ __schema { types { fields { type { `+nest+` } } } } }`, nil, "depth")
}

func TestMalformedAndOversizedRequests(t *testing.T) {
	e := newEnv(t, nil)
	post := func(body io.Reader) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/graphql", body)
		req.Header.Set("Authorization", "Bearer "+viewerT)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if r := post(strings.NewReader("not json")); r.StatusCode != http.StatusBadRequest {
		t.Errorf("bad json: %d", r.StatusCode)
	}
	if r := post(strings.NewReader(`{"query": ""}`)); r.StatusCode != http.StatusBadRequest {
		t.Errorf("empty query: %d", r.StatusCode)
	}
	big := `{"query": "{ me { name } }", "pad": "` + strings.Repeat("a", maxBodyBytes+10) + `"}`
	if r := post(strings.NewReader(big)); r.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body: %d", r.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, e.srv.URL+"/graphql", nil)
	req.Header.Set("Authorization", "Bearer "+viewerT)
	if r, _ := http.DefaultClient.Do(req); r.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET: %d", r.StatusCode)
	}
	_, r := e.do(viewerT, `{ nonexistent }`, nil)
	if len(r.Errors) == 0 {
		t.Error("unknown field should produce an error")
	}
}

func TestHealthReadinessAndMetricsAreReachable(t *testing.T) {
	e := newEnv(t, nil)
	get := func(path string) (int, string) {
		resp, err := http.Get(e.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if s, b := get("/healthz"); s != 200 || !strings.Contains(b, "ok") {
		t.Errorf("healthz = %d %s", s, b)
	}
	if s, b := get("/readyz"); s != 200 || !strings.Contains(b, "ready") {
		t.Errorf("readyz = %d %s", s, b)
	}
	e.do(viewerT, `{ me { name } }`, nil)
	e.do("bad-token-bad-token-1", `{ me { name } }`, nil)
	s, b := get("/metrics")
	if s != 200 || !strings.Contains(b, "dlqtriage_graphql_requests_total") || !strings.Contains(b, "dlqtriage_auth_failures_total 1") {
		t.Errorf("metrics = %d\n%.400s", s, b)
	}
	if strings.Contains(b, "SECRET") || strings.Contains(b, "alice@example.com") {
		t.Error("metrics leaked message content")
	}
}

func TestSecurityHeaders(t *testing.T) {
	e := newEnv(t, nil)
	resp, _ := http.Get(e.srv.URL + "/healthz")
	for h, want := range map[string]string{"X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Cache-Control": "no-store"} {
		if got := resp.Header.Get(h); got != want {
			t.Errorf("%s = %q, want %q", h, got, want)
		}
	}
	if !strings.Contains(resp.Header.Get("Content-Security-Policy"), "default-src 'self'") {
		t.Error("missing content security policy")
	}
}

func TestQueueServiceOutageDegradesInsteadOfFailing(t *testing.T) {
	e := newEnv(t, nil)
	e.srv.Close()
	st := e.st
	srv, err := NewServer(Options{
		Deps: Deps{
			Store: st, Queue: failingQueue{}, Engine: e.eng, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
			Queues: []replay.Queue{{Name: "orders", DLQURL: dlqURL, DestURL: destURL}}, QueueTimeout: 200 * time.Millisecond,
		},
		Auth: NewAuthenticator([]config.Token{{Name: "vera", Token: viewerT, Role: config.RoleViewer}}, false),
	})
	if err != nil {
		t.Fatal(err)
	}
	e.srv = httptest.NewServer(srv.Handler())
	defer e.srv.Close()

	var out struct {
		Queue struct {
			Depth        *struct{ Visible int }
			TotalPending int
			Groups       []struct{ Key string }
		}
	}
	e.ok(viewerT, `{ queue(name: "orders") { depth { visible } totalPending groups { key } } }`, nil, &out)
	if out.Queue.Depth != nil || out.Queue.TotalPending != 5 || len(out.Queue.Groups) != 2 {
		t.Fatalf("queue = %+v", out.Queue)
	}
}

type failingQueue struct{ queue.Queue }

func (failingQueue) Depth(context.Context, string) (domain.QueueDepth, error) {
	return domain.QueueDepth{}, errors.New("sqs unreachable")
}

func TestOpenModeAllowsLocalUseWithoutTokens(t *testing.T) {
	st, _ := store.OpenSQLite(":memory:")
	defer st.Close()
	srv, err := NewServer(Options{
		Deps: Deps{Store: st, Queue: queue.NewMemory(), Queues: []replay.Queue{{Name: "q", DLQURL: "a", DestURL: "b"}}},
		Auth: NewAuthenticator(nil, true),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, _ := http.Post(ts.URL+"/graphql", "application/json", strings.NewReader(`{"query":"{ me { name role } }"}`))
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(b), `"local"`) {
		t.Fatalf("open mode: %d %s", resp.StatusCode, b)
	}
}

func TestAuthenticatorWithTokensIgnoresAllowNoAuth(t *testing.T) {
	a := NewAuthenticator([]config.Token{{Name: "a", Token: viewerT, Role: config.RoleViewer}}, true)
	if a.Open() {
		t.Fatal("configured tokens must always be enforced")
	}
	req, _ := http.NewRequest(http.MethodPost, "/graphql", nil)
	if _, ok := a.Authenticate(req); ok {
		t.Fatal("request without a token was accepted")
	}
}
