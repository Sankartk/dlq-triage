package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/graph-gophers/graphql-go"

	"github.com/Sankartk/dlq-triage/internal/ai"
	"github.com/Sankartk/dlq-triage/internal/domain"
	"github.com/Sankartk/dlq-triage/internal/queue"
	"github.com/Sankartk/dlq-triage/internal/replay"
	"github.com/Sankartk/dlq-triage/internal/store"
)

// Deps are the collaborators the resolvers need.
type Deps struct {
	Store      store.Store
	Queue      queue.Queue
	Engine     *replay.Engine
	Summarizer ai.Summarizer
	Scans      *ScanTracker
	Queues     []replay.Queue
	Log        *slog.Logger
	// QueueTimeout bounds live queue lookups so a slow queue service cannot stall the API.
	QueueTimeout time.Duration
	Now          func() time.Time
}

const (
	maxListLimit  = 200
	maxPageOffset = 100000
)

var (
	errForbidden = errors.New("this action requires the operator role")
	errNoAuth    = errors.New("authentication required")
)

// Resolver is the root GraphQL resolver.
type Resolver struct{ d Deps }

func newResolver(d Deps) *Resolver {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.QueueTimeout <= 0 {
		d.QueueTimeout = 3 * time.Second
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Summarizer == nil {
		d.Summarizer = ai.Disabled{}
	}
	return &Resolver{d: d}
}

func (r *Resolver) queueByName(name string) (replay.Queue, bool) {
	for _, q := range r.d.Queues {
		if q.Name == name {
			return q, true
		}
	}
	return replay.Queue{}, false
}

func principal(ctx context.Context) (Principal, error) {
	p, ok := PrincipalFrom(ctx)
	if !ok {
		return Principal{}, errNoAuth
	}
	return p, nil
}

func operator(ctx context.Context) (Principal, error) {
	p, err := principal(ctx)
	if err != nil {
		return p, err
	}
	if !p.CanOperate() {
		return p, errForbidden
	}
	return p, nil
}

func clamp(v *int32, def, max int) int {
	if v == nil {
		return def
	}
	n := int(*v)
	if n < 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// --- Query ---------------------------------------------------------------

type meResolver struct{ p Principal }

func (m meResolver) Name() string { return m.p.Name }
func (m meResolver) Role() string { return m.p.Role }

func (r *Resolver) Me(ctx context.Context) (meResolver, error) {
	p, err := principal(ctx)
	return meResolver{p}, err
}

func (r *Resolver) AiEnabled(ctx context.Context) (bool, error) {
	if _, err := principal(ctx); err != nil {
		return false, err
	}
	_, disabled := r.d.Summarizer.(ai.Disabled)
	return !disabled, nil
}

func (r *Resolver) Queues(ctx context.Context) ([]*queueResolver, error) {
	if _, err := principal(ctx); err != nil {
		return nil, err
	}
	out := make([]*queueResolver, 0, len(r.d.Queues))
	for _, q := range r.d.Queues {
		out = append(out, &queueResolver{r: r, q: q})
	}
	return out, nil
}

func (r *Resolver) Queue(ctx context.Context, args struct{ Name string }) (*queueResolver, error) {
	if _, err := principal(ctx); err != nil {
		return nil, err
	}
	q, ok := r.queueByName(args.Name)
	if !ok {
		return nil, nil
	}
	return &queueResolver{r: r, q: q}, nil
}

func (r *Resolver) Group(ctx context.Context, args struct{ Queue, Key string }) (*groupResolver, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := r.queueByName(args.Queue); !ok {
		return nil, nil
	}
	g, err := r.d.Store.GetGroup(ctx, args.Queue, args.Key)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, r.internal("load group", err)
	}
	return &groupResolver{r: r, g: g, p: p}, nil
}

func (r *Resolver) ReplayJobs(ctx context.Context, args struct {
	Queue string
	Limit *int32
}) ([]*jobResolver, error) {
	if _, err := principal(ctx); err != nil {
		return nil, err
	}
	if _, ok := r.queueByName(args.Queue); !ok {
		return nil, fmt.Errorf("unknown queue %q", args.Queue)
	}
	jobs, err := r.d.Store.ListJobs(ctx, args.Queue, clamp(args.Limit, 20, maxListLimit))
	if err != nil {
		return nil, r.internal("list jobs", err)
	}
	return wrapJobs(jobs), nil
}

func (r *Resolver) ReplayJob(ctx context.Context, args struct{ ID graphql.ID }) (*jobResolver, error) {
	if _, err := principal(ctx); err != nil {
		return nil, err
	}
	j, err := r.d.Store.GetJob(ctx, string(args.ID))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, r.internal("load job", err)
	}
	return &jobResolver{j}, nil
}

func (r *Resolver) Audit(ctx context.Context, args struct {
	Queue *string
	Limit *int32
}) ([]*auditResolver, error) {
	if _, err := principal(ctx); err != nil {
		return nil, err
	}
	queueName := ""
	if args.Queue != nil {
		queueName = *args.Queue
		if _, ok := r.queueByName(queueName); !ok {
			return nil, fmt.Errorf("unknown queue %q", queueName)
		}
	}
	entries, err := r.d.Store.ListAudit(ctx, queueName, clamp(args.Limit, 50, maxListLimit))
	if err != nil {
		return nil, r.internal("list audit", err)
	}
	out := make([]*auditResolver, len(entries))
	for i, e := range entries {
		out[i] = &auditResolver{e}
	}
	return out, nil
}

// internal logs the real error and returns a message safe to show a client.
func (r *Resolver) internal(what string, err error) error {
	r.d.Log.Error(what, "err", err)
	return errors.New("internal error")
}

// --- Mutations -----------------------------------------------------------

type replayInput struct {
	Queue         string
	Group         string
	DryRun        bool
	RatePerSecond float64
	MaxMessages   int32
}

func (r *Resolver) StartReplay(ctx context.Context, args struct{ Input replayInput }) (*jobResolver, error) {
	p, err := operator(ctx)
	if err != nil {
		return nil, err
	}
	q, ok := r.queueByName(args.Input.Queue)
	if !ok {
		return nil, fmt.Errorf("unknown queue %q", args.Input.Queue)
	}
	job, err := r.d.Engine.Start(ctx, replay.Request{
		Queue: q, GroupKey: args.Input.Group,
		Actor:  p.Name, // always the authenticated caller, never client-supplied
		DryRun: args.Input.DryRun, RatePerSec: args.Input.RatePerSecond, MaxMessages: int(args.Input.MaxMessages),
	})
	switch {
	case err == nil:
		return &jobResolver{job}, nil
	case errors.Is(err, replay.ErrInvalid), errors.Is(err, replay.ErrBusy), errors.Is(err, replay.ErrNothingToReplay):
		return nil, err
	case errors.Is(err, store.ErrNotFound):
		return nil, errors.New("group not found")
	default:
		return nil, r.internal("start replay", err)
	}
}

func (r *Resolver) CancelReplay(ctx context.Context, args struct{ ID graphql.ID }) (bool, error) {
	p, err := operator(ctx)
	if err != nil {
		return false, err
	}
	job, err := r.d.Store.GetJob(ctx, string(args.ID))
	if errors.Is(err, store.ErrNotFound) {
		return false, errors.New("job not found")
	}
	if err != nil {
		return false, r.internal("load job", err)
	}
	ok := r.d.Engine.Cancel(job.ID)
	if ok {
		_ = r.d.Store.AppendAudit(ctx, domain.AuditEntry{
			At: r.d.Now(), Actor: p.Name, Action: "replay.cancel.request", Queue: job.Queue, GroupKey: job.GroupKey, JobID: job.ID,
		})
	}
	return ok, nil
}

func (r *Resolver) SummarizeGroup(ctx context.Context, args struct{ Queue, Key string }) (*groupResolver, error) {
	p, err := operator(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := r.queueByName(args.Queue); !ok {
		return nil, fmt.Errorf("unknown queue %q", args.Queue)
	}
	g, err := r.d.Store.GetGroup(ctx, args.Queue, args.Key)
	if errors.Is(err, store.ErrNotFound) {
		return nil, errors.New("group not found")
	}
	if err != nil {
		return nil, r.internal("load group", err)
	}
	text, err := r.d.Summarizer.Summarize(ctx, ai.Input{
		Queue: g.Queue, ErrorSig: g.ErrorSig, Shape: g.Shape, Count: g.Count, FirstSeen: g.FirstSeen, LastSeen: g.LastSeen,
	})
	if errors.Is(err, ai.ErrDisabled) {
		return nil, err
	}
	if err != nil {
		r.d.Log.Warn("summarise group", "queue", g.Queue, "group", g.Key, "err", err)
		return nil, errors.New("the AI provider could not produce a summary; try again later")
	}
	now := r.d.Now()
	if err := r.d.Store.SetAISummary(ctx, g.Queue, g.Key, text, now); err != nil {
		return nil, r.internal("save summary", err)
	}
	_ = r.d.Store.AppendAudit(ctx, domain.AuditEntry{
		At: now, Actor: p.Name, Action: "group.summarize", Queue: g.Queue, GroupKey: g.Key,
		Detail: "generated an AI summary from the error signature and payload shape",
	})
	g.AISummary, g.AISummaryAt = text, &now
	return &groupResolver{r: r, g: g, p: p}, nil
}

// --- Queue ---------------------------------------------------------------

type queueResolver struct {
	r *Resolver
	q replay.Queue
}

func (q *queueResolver) Name() string { return q.q.Name }

func (q *queueResolver) depth(ctx context.Context, url string) *depthResolver {
	ctx, cancel := context.WithTimeout(ctx, q.r.d.QueueTimeout)
	defer cancel()
	d, err := q.r.d.Queue.Depth(ctx, url)
	if err != nil {
		q.r.d.Log.Warn("queue depth unavailable", "queue", q.q.Name, "err", err)
		return nil
	}
	return &depthResolver{d}
}

func (q *queueResolver) Depth(ctx context.Context) *depthResolver { return q.depth(ctx, q.q.DLQURL) }
func (q *queueResolver) DestDepth(ctx context.Context) *depthResolver {
	return q.depth(ctx, q.q.DestURL)
}

func (q *queueResolver) groups(ctx context.Context) ([]domain.Group, error) {
	g, err := q.r.d.Store.ListGroups(ctx, q.q.Name)
	if err != nil {
		return nil, q.r.internal("list groups", err)
	}
	return g, nil
}

func (q *queueResolver) TotalPending(ctx context.Context) (int32, error) {
	groups, err := q.groups(ctx)
	if err != nil {
		return 0, err
	}
	var n int32
	for _, g := range groups {
		n += int32(g.PendingCount)
	}
	return n, nil
}

func (q *queueResolver) Groups(ctx context.Context) ([]*groupResolver, error) {
	p, err := principal(ctx)
	if err != nil {
		return nil, err
	}
	groups, err := q.groups(ctx)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(groups, func(i, j int) bool { return groups[i].PendingCount > groups[j].PendingCount })
	out := make([]*groupResolver, len(groups))
	for i, g := range groups {
		out[i] = &groupResolver{r: q.r, g: g, p: p}
	}
	return out, nil
}

func (q *queueResolver) LastScan() *scanResolver {
	st, ok := q.r.d.Scans.Last(q.q.Name)
	if !ok {
		return nil
	}
	return &scanResolver{st}
}

type depthResolver struct{ d domain.QueueDepth }

func (d *depthResolver) Visible() int32  { return int32(d.d.Visible) }
func (d *depthResolver) InFlight() int32 { return int32(d.d.InFlight) }
func (d *depthResolver) Delayed() int32  { return int32(d.d.Delayed) }

type scanResolver struct{ s ScanStatus }

func (s *scanResolver) At() graphql.Time { return graphql.Time{Time: s.s.At} }
func (s *scanResolver) Seen() int32      { return int32(s.s.Seen) }
func (s *scanResolver) New() int32       { return int32(s.s.New) }
func (s *scanResolver) Skipped() bool    { return s.s.Skipped }
func (s *scanResolver) Error() *string {
	if s.s.Err == "" {
		return nil
	}
	return &s.s.Err
}

// --- Group and messages --------------------------------------------------

type groupResolver struct {
	r *Resolver
	g domain.Group
	p Principal
}

func (g *groupResolver) Queue() string          { return g.g.Queue }
func (g *groupResolver) Key() string            { return g.g.Key }
func (g *groupResolver) ErrorSignature() string { return g.g.ErrorSig }
func (g *groupResolver) Shape() string          { return g.g.Shape }
func (g *groupResolver) Count() int32           { return int32(g.g.Count) }
func (g *groupResolver) Pending() int32         { return int32(g.g.PendingCount) }
func (g *groupResolver) FirstSeen() graphql.Time {
	return graphql.Time{Time: g.g.FirstSeen}
}
func (g *groupResolver) LastSeen() graphql.Time { return graphql.Time{Time: g.g.LastSeen} }
func (g *groupResolver) AiSummary() *string {
	if g.g.AISummary == "" {
		return nil
	}
	return &g.g.AISummary
}
func (g *groupResolver) AiSummaryAt() *graphql.Time {
	if g.g.AISummaryAt == nil {
		return nil
	}
	return &graphql.Time{Time: *g.g.AISummaryAt}
}

func (g *groupResolver) Messages(ctx context.Context, args struct {
	PendingOnly *bool
	Limit       *int32
	Offset      *int32
}) ([]*messageResolver, error) {
	pending := args.PendingOnly == nil || *args.PendingOnly
	msgs, err := g.r.d.Store.ListMessages(ctx, g.g.Queue, g.g.Key, pending,
		clamp(args.Limit, 20, maxListLimit), clamp(args.Offset, 0, maxPageOffset))
	if err != nil {
		return nil, g.r.internal("list messages", err)
	}
	out := make([]*messageResolver, len(msgs))
	for i, m := range msgs {
		out[i] = &messageResolver{m: m, canSeeBody: g.p.CanOperate()}
	}
	return out, nil
}

type messageResolver struct {
	m          domain.StoredMessage
	canSeeBody bool
}

func (m *messageResolver) ID() string          { return m.m.ID }
func (m *messageResolver) Status() string      { return string(m.m.Status) }
func (m *messageResolver) ReceiveCount() int32 { return int32(m.m.ReceiveCount) }
func (m *messageResolver) SentAt() *graphql.Time {
	if m.m.SentAt.IsZero() {
		return nil
	}
	return &graphql.Time{Time: m.m.SentAt}
}
func (m *messageResolver) IngestedAt() graphql.Time { return graphql.Time{Time: m.m.IngestedAt} }
func (m *messageResolver) ReplayedAt() *graphql.Time {
	if m.m.ReplayedAt == nil {
		return nil
	}
	return &graphql.Time{Time: *m.m.ReplayedAt}
}
func (m *messageResolver) Body() *string {
	if !m.canSeeBody {
		return nil
	}
	return &m.m.Body
}
func (m *messageResolver) Attributes() []*attributeResolver {
	names := make([]string, 0, len(m.m.Attributes))
	for n := range m.m.Attributes {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]*attributeResolver, len(names))
	for i, n := range names {
		out[i] = &attributeResolver{name: n, a: m.m.Attributes[n], redact: !m.canSeeBody}
	}
	return out
}

type attributeResolver struct {
	name   string
	a      domain.Attribute
	redact bool
}

func (a *attributeResolver) Name() string     { return a.name }
func (a *attributeResolver) DataType() string { return a.a.DataType }

// Viewers see attribute names and types but not values, which can carry the
// same sensitive data as the body.
func (a *attributeResolver) Value() string {
	if a.redact {
		return "[hidden]"
	}
	return a.a.Value
}

// --- Jobs and audit ------------------------------------------------------

type jobResolver struct{ j domain.ReplayJob }

func wrapJobs(jobs []domain.ReplayJob) []*jobResolver {
	out := make([]*jobResolver, len(jobs))
	for i, j := range jobs {
		out[i] = &jobResolver{j}
	}
	return out
}

func (j *jobResolver) ID() graphql.ID         { return graphql.ID(j.j.ID) }
func (j *jobResolver) Queue() string          { return j.j.Queue }
func (j *jobResolver) Group() string          { return j.j.GroupKey }
func (j *jobResolver) DryRun() bool           { return j.j.DryRun }
func (j *jobResolver) Status() string         { return string(j.j.Status) }
func (j *jobResolver) RequestedBy() string    { return j.j.RequestedBy }
func (j *jobResolver) RatePerSecond() float64 { return j.j.RatePerSec }
func (j *jobResolver) MaxMessages() int32     { return int32(j.j.MaxMessages) }
func (j *jobResolver) Total() int32           { return int32(j.j.Total) }
func (j *jobResolver) Replayed() int32        { return int32(j.j.Replayed) }
func (j *jobResolver) Skipped() int32         { return int32(j.j.Skipped) }
func (j *jobResolver) Failed() int32          { return int32(j.j.Failed) }
func (j *jobResolver) Error() *string {
	if j.j.Error == "" {
		return nil
	}
	return &j.j.Error
}
func (j *jobResolver) CreatedAt() graphql.Time { return graphql.Time{Time: j.j.CreatedAt} }
func (j *jobResolver) FinishedAt() *graphql.Time {
	if j.j.FinishedAt == nil {
		return nil
	}
	return &graphql.Time{Time: *j.j.FinishedAt}
}

type auditResolver struct{ e domain.AuditEntry }

func (a *auditResolver) ID() graphql.ID   { return graphql.ID(fmt.Sprint(a.e.ID)) }
func (a *auditResolver) At() graphql.Time { return graphql.Time{Time: a.e.At} }
func (a *auditResolver) Actor() string    { return a.e.Actor }
func (a *auditResolver) Action() string   { return a.e.Action }
func (a *auditResolver) Queue() string    { return a.e.Queue }
func (a *auditResolver) Detail() string   { return a.e.Detail }
func (a *auditResolver) Group() *string   { return nonEmpty(a.e.GroupKey) }
func (a *auditResolver) JobId() *string   { return nonEmpty(a.e.JobID) }

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
