// Command dlq-triage groups dead-lettered messages by failure and replays
// them under control.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/Sankartk/dlq-triage/internal/ai"
	"github.com/Sankartk/dlq-triage/internal/api"
	"github.com/Sankartk/dlq-triage/internal/config"
	"github.com/Sankartk/dlq-triage/internal/fingerprint"
	"github.com/Sankartk/dlq-triage/internal/ingest"
	"github.com/Sankartk/dlq-triage/internal/queue"
	"github.com/Sankartk/dlq-triage/internal/replay"
	"github.com/Sankartk/dlq-triage/internal/store"
	"github.com/Sankartk/dlq-triage/web"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dlq-triage:", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "config.json", "path to the configuration file")
	checkOnly := flag.Bool("check", false, "validate the configuration and exit")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return nil
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if *checkOnly {
		fmt.Printf("configuration OK: %d queue(s), %d token(s), AI %s\n",
			len(cfg.Queues), len(cfg.Auth.Tokens), map[bool]string{true: "enabled", false: "disabled"}[cfg.AI.Enabled])
		return nil
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.OpenSQLite(cfg.Database)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer st.Close()
	// No job can be running at startup, so any recorded as running were cut off.
	if n, err := st.FailRunningJobs(ctx, "service restarted while the job was running", time.Now()); err != nil {
		return fmt.Errorf("recover jobs: %w", err)
	} else if n > 0 {
		log.Warn("marked interrupted replay jobs as failed", "count", n)
	}

	sqsClient, err := queue.NewSQS(ctx, queue.SQSOptions{Region: cfg.AWS.Region, Endpoint: cfg.AWS.Endpoint})
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := api.NewMetrics(reg)
	scans := api.NewScanTracker()

	queues := make([]replay.Queue, 0, len(cfg.Queues))
	sources := make([]ingest.Source, 0, len(cfg.Queues))
	for _, q := range cfg.Queues {
		queues = append(queues, replay.Queue{Name: q.Name, DLQURL: q.DLQURL, DestURL: q.DestURL})
		sources = append(sources, ingest.Source{Name: q.Name, DLQURL: q.DLQURL})
	}

	engine := replay.New(ctx, st, sqsClient, replay.Config{
		Visibility:       cfg.Ingest.Visibility.Std() * 3,
		MaxRatePerSec:    cfg.Replay.MaxRatePerSec,
		MaxMessagesLimit: cfg.Replay.MaxMessages,
		TagAttributes:    *cfg.Replay.TagAttributes,
	}, log, newJobID)
	engine.OnFinish = metrics.ObserveJob

	ing := ingest.New(st, sqsClient, ingest.Config{
		Fingerprint: fingerprint.Config{ErrorAttributes: cfg.Fingerprint.ErrorAttributes, ErrorBodyPaths: cfg.Fingerprint.ErrorBodyPaths},
		Visibility:  cfg.Ingest.Visibility.Std(),
		MaxPerScan:  cfg.Ingest.MaxPerScan,
		Interval:    cfg.Ingest.Interval.Std(),
	}, log)
	ing.Busy = engine.Running
	ing.OnScan = func(name string, res ingest.Result, err error) {
		scans.Record(name, res, err)
		metrics.ObserveScan(name, res.Seen, res.New, err)
	}

	var summarizer ai.Summarizer = ai.Disabled{}
	if cfg.AI.Enabled {
		summarizer = ai.NewOpenAI(cfg.AI.BaseURL, cfg.AI.Model, cfg.AI.APIKey, cfg.AI.Timeout.Std())
	}

	auth := api.NewAuthenticator(cfg.Auth.Tokens, cfg.Auth.AllowNoAuth)
	srv, err := api.NewServer(api.Options{
		Deps: api.Deps{
			Store: st, Queue: sqsClient, Engine: engine, Summarizer: summarizer,
			Scans: scans, Queues: queues, Log: log,
		},
		Auth: auth, Metrics: metrics,
		Ready:  st.Ping,
		Static: web.Handler(),
	})
	if err != nil {
		return err
	}

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}
	if auth.Open() {
		log.Warn("authentication is disabled (auth.allowNoAuth); anyone who can reach this port can replay messages", "listen", cfg.Listen)
	}

	ingestDone := make(chan struct{})
	go func() { ing.Run(ctx, sources); close(ingestDone) }()

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.Listen, "version", version, "queues", len(queues), "ai", cfg.AI.Enabled)
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	engine.Shutdown() // records running jobs as cancelled before the database closes
	<-ingestDone
	return nil
}

func newJobID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err) // the system random source failing is not recoverable
	}
	return hex.EncodeToString(b)
}
