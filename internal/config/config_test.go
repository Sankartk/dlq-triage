package config

import (
	"strings"
	"testing"
	"time"
)

const valid = `{
  "queues": [{"name": "orders", "dlqUrl": "https://sqs/orders-dlq", "destUrl": "https://sqs/orders"}],
  "auth": {"tokens": [{"name": "alice", "token": "env:ALICE", "role": "operator"}]}
}`

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

var aliceEnv = env(map[string]string{"ALICE": "alice-secret-token-123456"})

func TestParseAppliesDefaultsAndExpandsEnv(t *testing.T) {
	c, err := Parse([]byte(valid), aliceEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.Auth.Tokens[0].Token != "alice-secret-token-123456" {
		t.Fatalf("token not expanded: %q", c.Auth.Tokens[0].Token)
	}
	if c.Listen != "127.0.0.1:8080" {
		t.Errorf("default listen = %q, must bind loopback only", c.Listen)
	}
	if c.Ingest.Interval.Std() != time.Minute || c.Ingest.Visibility.Std() != 10*time.Second || c.Ingest.MaxPerScan != 1000 {
		t.Errorf("ingest defaults = %+v", c.Ingest)
	}
	if c.Replay.MaxRatePerSec != 50 || c.Replay.MaxMessages != 1000 || c.Replay.TagAttributes == nil || !*c.Replay.TagAttributes {
		t.Errorf("replay defaults = %+v", c.Replay)
	}
	if len(c.Fingerprint.ErrorAttributes) == 0 || len(c.Fingerprint.ErrorBodyPaths) == 0 {
		t.Errorf("fingerprint defaults missing: %+v", c.Fingerprint)
	}
	if c.AI.Enabled {
		t.Error("AI must be off by default")
	}
}

func TestParseRejectsInvalidConfigs(t *testing.T) {
	q := `"queues": [{"name": "orders", "dlqUrl": "d", "destUrl": "o"}]`
	tok := `"auth": {"tokens": [{"name": "a", "token": "0123456789abcdef", "role": "viewer"}]}`
	cases := map[string]struct{ json, want string }{
		"no queues":        {`{"queues": [], ` + tok + `}`, "at least one queue"},
		"unknown field":    {`{` + q + `, ` + tok + `, "listn": ":1"}`, "unknown field"},
		"bad json":         {`{`, "parse config"},
		"dup queue":        {`{"queues": [{"name":"a","dlqUrl":"d","destUrl":"o"},{"name":"a","dlqUrl":"d2","destUrl":"o2"}], ` + tok + `}`, "duplicated"},
		"same urls":        {`{"queues": [{"name":"a","dlqUrl":"x","destUrl":"x"}], ` + tok + `}`, "must differ"},
		"missing dest":     {`{"queues": [{"name":"a","dlqUrl":"x"}], ` + tok + `}`, "destUrl is required"},
		"no auth":          {`{` + q + `}`, "at least one token"},
		"short token":      {`{` + q + `, "auth": {"tokens": [{"name":"a","token":"short","role":"viewer"}]}}`, "at least 16"},
		"bad role":         {`{` + q + `, "auth": {"tokens": [{"name":"a","token":"0123456789abcdef","role":"root"}]}}`, "role must be"},
		"dup token":        {`{` + q + `, "auth": {"tokens": [{"name":"a","token":"0123456789abcdef","role":"viewer"},{"name":"b","token":"0123456789abcdef","role":"viewer"}]}}`, "duplicates another token"},
		"dup token name":   {`{` + q + `, "auth": {"tokens": [{"name":"a","token":"0123456789abcdef","role":"viewer"},{"name":"a","token":"fedcba9876543210","role":"viewer"}]}}`, "name \"a\" is duplicated"},
		"env var missing":  {`{` + q + `, "auth": {"tokens": [{"name":"a","token":"env:NOPE","role":"viewer"}]}}`, "NOPE is not set"},
		"short visibility": {`{` + q + `, ` + tok + `, "ingest": {"visibility": "100ms"}}`, "ingest.visibility"},
		"bad duration":     {`{` + q + `, ` + tok + `, "ingest": {"interval": 5}}`, "duration must be a string"},
		"ai without key":   {`{` + q + `, ` + tok + `, "ai": {"enabled": true, "baseUrl": "u", "model": "m"}}`, "ai.apiKey"},
		"ai without model": {`{` + q + `, ` + tok + `, "ai": {"enabled": true, "apiKey": "k"}}`, "baseUrl and model"},
		"negative max":     {`{` + q + `, ` + tok + `, "ingest": {"maxPerScan": -1}}`, "maxPerScan"},
		"negative replay":  {`{` + q + `, ` + tok + `, "replay": {"maxMessages": -5}}`, "replay.maxRatePerSec"},
	}
	for name, c := range cases {
		_, err := Parse([]byte(c.json), aliceEnv)
		if err == nil {
			t.Errorf("%s: expected an error", name)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q does not mention %q", name, err, c.want)
		}
	}
}

func TestParseReportsAllProblemsAtOnce(t *testing.T) {
	_, err := Parse([]byte(`{"queues": [{"name": "", "dlqUrl": "", "destUrl": ""}]}`), aliceEnv)
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"name is required", "dlqUrl is required", "destUrl is required", "at least one token"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
}

func TestAllowNoAuthIsExplicit(t *testing.T) {
	c, err := Parse([]byte(`{"queues": [{"name":"a","dlqUrl":"d","destUrl":"o"}], "auth": {"allowNoAuth": true}}`), aliceEnv)
	if err != nil || len(c.Auth.Tokens) != 0 || !c.Auth.AllowNoAuth {
		t.Fatalf("c=%+v err=%v", c.Auth, err)
	}
}

func TestAIEnabledWithEnvKey(t *testing.T) {
	js := `{"queues": [{"name":"a","dlqUrl":"d","destUrl":"o"}], "auth": {"allowNoAuth": true},
	        "ai": {"enabled": true, "baseUrl": "https://api.example/v1", "model": "m", "apiKey": "env:KEY", "timeout": "5s"}}`
	c, err := Parse([]byte(js), env(map[string]string{"KEY": "sk-test"}))
	if err != nil || c.AI.APIKey != "sk-test" || c.AI.Timeout.Std() != 5*time.Second {
		t.Fatalf("ai=%+v err=%v", c.AI, err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("does-not-exist.json"); err == nil {
		t.Fatal("expected error")
	}
}
