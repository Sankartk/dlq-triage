// Package config loads and validates the service configuration.
//
// Secrets are never stored in the file: a value of the form "env:NAME" is
// replaced by the environment variable NAME when the file is loaded.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Duration is a time.Duration that reads from JSON strings such as "30s".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New("duration must be a string such as \"30s\"")
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) Std() time.Duration { return time.Duration(d) }

// Config is the full service configuration.
type Config struct {
	Listen      string      `json:"listen"`
	Database    string      `json:"database"`
	AWS         AWS         `json:"aws"`
	Queues      []Queue     `json:"queues"`
	Fingerprint Fingerprint `json:"fingerprint"`
	Ingest      Ingest      `json:"ingest"`
	Replay      Replay      `json:"replay"`
	Auth        Auth        `json:"auth"`
	AI          AI          `json:"ai"`
}

type AWS struct {
	Region   string `json:"region"`
	Endpoint string `json:"endpoint"`
}

type Queue struct {
	Name    string `json:"name"`
	DLQURL  string `json:"dlqUrl"`
	DestURL string `json:"destUrl"`
}

type Fingerprint struct {
	ErrorAttributes []string `json:"errorAttributes"`
	ErrorBodyPaths  []string `json:"errorBodyPaths"`
}

type Ingest struct {
	Interval   Duration `json:"interval"`
	Visibility Duration `json:"visibility"`
	MaxPerScan int      `json:"maxPerScan"`
}

type Replay struct {
	MaxRatePerSec float64 `json:"maxRatePerSec"`
	MaxMessages   int     `json:"maxMessages"`
	TagAttributes *bool   `json:"tagAttributes"`
}

type Token struct {
	Name  string `json:"name"`
	Token string `json:"token"`
	Role  string `json:"role"` // "viewer" or "operator"
}

type Auth struct {
	Tokens []Token `json:"tokens"`
	// AllowNoAuth lets the service start with no tokens. Meant for local use only.
	AllowNoAuth bool `json:"allowNoAuth"`
}

type AI struct {
	Enabled bool     `json:"enabled"`
	BaseURL string   `json:"baseUrl"`
	Model   string   `json:"model"`
	APIKey  string   `json:"apiKey"`
	Timeout Duration `json:"timeout"`
}

const (
	RoleViewer   = "viewer"
	RoleOperator = "operator"
	minTokenLen  = 16
)

// Load reads, expands and validates a configuration file.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return Parse(raw, os.Getenv)
}

// Parse decodes configuration JSON. getenv resolves "env:NAME" values.
func Parse(raw []byte, getenv func(string) string) (Config, error) {
	var c Config
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	var problems []string
	expand := func(field string, v *string) {
		if name, ok := strings.CutPrefix(*v, "env:"); ok {
			val := getenv(name)
			if val == "" {
				problems = append(problems, fmt.Sprintf("%s: environment variable %s is not set", field, name))
			}
			*v = val
		}
	}
	for i := range c.Auth.Tokens {
		expand(fmt.Sprintf("auth.tokens[%d].token", i), &c.Auth.Tokens[i].Token)
	}
	expand("ai.apiKey", &c.AI.APIKey)
	c.applyDefaults()
	problems = append(problems, c.validate()...)
	if len(problems) > 0 {
		return Config{}, fmt.Errorf("invalid config:\n  - %s", strings.Join(problems, "\n  - "))
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8080"
	}
	if c.Database == "" {
		c.Database = "dlq-triage.db"
	}
	if len(c.Fingerprint.ErrorAttributes) == 0 && len(c.Fingerprint.ErrorBodyPaths) == 0 {
		c.Fingerprint.ErrorAttributes = []string{"ErrorMessage", "error"}
		c.Fingerprint.ErrorBodyPaths = []string{"error.message", "errorMessage", "error"}
	}
	if c.Ingest.Interval == 0 {
		c.Ingest.Interval = Duration(60 * time.Second)
	}
	if c.Ingest.Visibility == 0 {
		c.Ingest.Visibility = Duration(10 * time.Second)
	}
	if c.Ingest.MaxPerScan == 0 {
		c.Ingest.MaxPerScan = 1000
	}
	if c.Replay.MaxRatePerSec == 0 {
		c.Replay.MaxRatePerSec = 50
	}
	if c.Replay.MaxMessages == 0 {
		c.Replay.MaxMessages = 1000
	}
	if c.Replay.TagAttributes == nil {
		t := true
		c.Replay.TagAttributes = &t
	}
	if c.AI.Timeout == 0 {
		c.AI.Timeout = Duration(30 * time.Second)
	}
}

func (c *Config) validate() []string {
	var p []string
	if len(c.Queues) == 0 {
		p = append(p, "queues: at least one queue is required")
	}
	seen := map[string]bool{}
	for i, q := range c.Queues {
		switch {
		case q.Name == "":
			p = append(p, fmt.Sprintf("queues[%d].name is required", i))
		case seen[q.Name]:
			p = append(p, fmt.Sprintf("queues[%d].name %q is duplicated", i, q.Name))
		}
		seen[q.Name] = true
		if q.DLQURL == "" {
			p = append(p, fmt.Sprintf("queues[%d].dlqUrl is required", i))
		}
		if q.DestURL == "" {
			p = append(p, fmt.Sprintf("queues[%d].destUrl is required", i))
		}
		if q.DLQURL != "" && q.DLQURL == q.DestURL {
			p = append(p, fmt.Sprintf("queues[%d]: dlqUrl and destUrl must differ", i))
		}
	}
	if len(c.Auth.Tokens) == 0 && !c.Auth.AllowNoAuth {
		p = append(p, "auth.tokens: at least one token is required (or set auth.allowNoAuth for local use)")
	}
	names := map[string]bool{}
	tokens := map[string]bool{}
	for i, t := range c.Auth.Tokens {
		if t.Name == "" {
			p = append(p, fmt.Sprintf("auth.tokens[%d].name is required", i))
		}
		if names[t.Name] {
			p = append(p, fmt.Sprintf("auth.tokens[%d].name %q is duplicated", i, t.Name))
		}
		names[t.Name] = true
		if len(t.Token) < minTokenLen {
			p = append(p, fmt.Sprintf("auth.tokens[%d].token must be at least %d characters", i, minTokenLen))
		}
		if tokens[t.Token] && t.Token != "" {
			p = append(p, fmt.Sprintf("auth.tokens[%d].token duplicates another token", i))
		}
		tokens[t.Token] = true
		if t.Role != RoleViewer && t.Role != RoleOperator {
			p = append(p, fmt.Sprintf("auth.tokens[%d].role must be %q or %q", i, RoleViewer, RoleOperator))
		}
	}
	if c.Ingest.Visibility.Std() < time.Second {
		p = append(p, "ingest.visibility must be at least 1s")
	}
	if c.Ingest.Interval.Std() < time.Second {
		p = append(p, "ingest.interval must be at least 1s")
	}
	if c.Ingest.MaxPerScan < 1 {
		p = append(p, "ingest.maxPerScan must be positive")
	}
	if c.Replay.MaxRatePerSec <= 0 || c.Replay.MaxMessages < 1 {
		p = append(p, "replay.maxRatePerSec and replay.maxMessages must be positive")
	}
	if c.AI.Enabled {
		if c.AI.BaseURL == "" || c.AI.Model == "" {
			p = append(p, "ai: baseUrl and model are required when enabled")
		}
		if c.AI.APIKey == "" {
			p = append(p, "ai.apiKey is required when enabled (use \"env:NAME\")")
		}
	}
	return p
}
