package ai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var sample = Input{
	Queue: "orders", ErrorSig: "Loan <n> not found", Shape: "json{loan:string}", Count: 12,
	FirstSeen: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC), LastSeen: time.Date(2026, 10, 3, 9, 30, 0, 0, time.UTC),
}

func reply(content string) string {
	b, _ := json.Marshal(map[string]any{"choices": []map[string]any{{"message": map[string]string{"role": "assistant", "content": content}}}})
	return string(b)
}

func TestSummarizeSendsOnlySignatureAndShape(t *testing.T) {
	var gotAuth, gotPath string
	var gotBody chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.Path
		json.NewDecoder(r.Body).Decode(&gotBody)
		io.WriteString(w, reply("  The loan record is missing.  "))
	}))
	defer srv.Close()

	c := NewOpenAI(srv.URL+"/v1/", "model-x", "sk-secret", 5*time.Second)
	out, err := c.Summarize(context.Background(), sample)
	if err != nil {
		t.Fatal(err)
	}
	if out != "The loan record is missing." {
		t.Fatalf("summary = %q", out)
	}
	if gotAuth != "Bearer sk-secret" || gotPath != "/v1/chat/completions" || gotBody.Model != "model-x" {
		t.Fatalf("request auth=%q path=%q model=%q", gotAuth, gotPath, gotBody.Model)
	}
	user := gotBody.Messages[1].Content
	for _, want := range []string{"orders", "Loan <n> not found", "json{loan:string}", "12", "2026-10-01T08:00:00Z"} {
		if !strings.Contains(user, want) {
			t.Errorf("prompt missing %q:\n%s", want, user)
		}
	}
	if gotBody.Temperature > 0.5 || gotBody.MaxTokens == 0 {
		t.Errorf("temperature=%v max_tokens=%d", gotBody.Temperature, gotBody.MaxTokens)
	}
}

func TestPromptHasNoRoomForPayloadValues(t *testing.T) {
	// Input has no field that could carry a body or attribute value.
	p := BuildPrompt(Input{Queue: "q", ErrorSig: "", Shape: "empty", Count: 1})
	if !strings.Contains(p, "(none found on the messages)") {
		t.Fatalf("empty signature not flagged: %s", p)
	}
}

func TestSummarizeErrors(t *testing.T) {
	cases := map[string]struct {
		handler http.HandlerFunc
		want    string
	}{
		"unauthorized": {func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(401)
			io.WriteString(w, `{"error":{"message":"bad key"}}`)
		}, "returned 401: bad key"},
		"server error plain text": {func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			io.WriteString(w, "boom")
		}, "returned 500: boom"},
		"not json":   {func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "<html>") }, "not valid JSON"},
		"no choices": {func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"choices":[]}`) }, "no choices"},
		"empty":      {func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, reply("   ")) }, "empty summary"},
	}
	for name, c := range cases {
		srv := httptest.NewServer(c.handler)
		_, err := NewOpenAI(srv.URL, "m", "k", time.Second).Summarize(context.Background(), sample)
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want mention of %q", name, err, c.want)
		}
	}
}

func TestSummarizeNeverLeaksKeyInErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		io.WriteString(w, "denied")
	}))
	url := srv.URL
	srv.Close() // connection refused
	_, err := NewOpenAI(url, "m", "sk-very-secret", time.Second).Summarize(context.Background(), sample)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "sk-very-secret") {
		t.Fatalf("API key leaked in error: %v", err)
	}
}

func TestSummarizeTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) // lets the server notice a client disconnect
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	start := time.Now()
	_, err := NewOpenAI(srv.URL, "m", "k", 100*time.Millisecond).Summarize(context.Background(), sample)
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("timeout not enforced: err=%v after %v", err, time.Since(start))
	}
}

func TestSummarizeHonoursContextCancel(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	start := time.Now()
	if _, err := NewOpenAI(srv.URL, "m", "k", 5*time.Second).Summarize(ctx, sample); err == nil {
		t.Fatal("expected cancellation error")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("cancel took %v", time.Since(start))
	}
}

func TestSummaryIsTruncated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, reply(strings.Repeat("é", 5000)))
	}))
	defer srv.Close()
	out, err := NewOpenAI(srv.URL, "m", "k", time.Second).Summarize(context.Background(), sample)
	if err != nil {
		t.Fatal(err)
	}
	if n := len([]rune(out)); n > maxSummaryRunes+1 {
		t.Fatalf("summary has %d runes", n)
	}
}

func TestDisabled(t *testing.T) {
	if _, err := (Disabled{}).Summarize(context.Background(), sample); !errors.Is(err, ErrDisabled) {
		t.Fatalf("err = %v", err)
	}
}
