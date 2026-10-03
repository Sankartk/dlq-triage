// Package ai produces optional plain-language summaries of failure groups.
//
// The summarizer only receives the normalised error signature and the payload
// shape (key names and value types). Message bodies and attribute values are
// never sent. It cannot replay, delete or change anything.
package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrDisabled is returned when no summarizer is configured.
var ErrDisabled = errors.New("AI summaries are not enabled")

const (
	maxResponseBytes = 1 << 20
	maxSummaryRunes  = 1200
)

// Input is everything the summarizer sees about a group.
type Input struct {
	Queue     string
	ErrorSig  string
	Shape     string
	Count     int
	FirstSeen time.Time
	LastSeen  time.Time
}

// Summarizer turns a group description into text.
type Summarizer interface {
	Summarize(ctx context.Context, in Input) (string, error)
}

// Disabled is the Summarizer used when AI is off.
type Disabled struct{}

func (Disabled) Summarize(context.Context, Input) (string, error) { return "", ErrDisabled }

const systemPrompt = `You help engineers triage messages that landed in a dead-letter queue.
You are given a normalised error signature (numbers, ids and timestamps are replaced by placeholders such as <n> and <uuid>) and the structure of the payload (key paths and value types, no values).
Reply in at most 5 short sentences: what the failure most likely means, the first thing to check, and whether replaying is likely to be safe once the cause is fixed.
Be explicit about uncertainty. Do not invent system names, field values or facts that are not in the input. If the error text is empty, say that the cause cannot be determined from the data given.`

// BuildPrompt returns the user message for a group. It is exported so tests
// and reviewers can see exactly what leaves the process.
func BuildPrompt(in Input) string {
	sig := in.ErrorSig
	if strings.TrimSpace(sig) == "" {
		sig = "(none found on the messages)"
	}
	return fmt.Sprintf("Queue: %s\nMessages in group: %d\nFirst seen: %s\nLast seen: %s\nError signature: %s\nPayload shape: %s",
		in.Queue, in.Count, in.FirstSeen.UTC().Format(time.RFC3339), in.LastSeen.UTC().Format(time.RFC3339), sig, in.Shape)
}

// OpenAI talks to any server that implements the chat completions API.
type OpenAI struct {
	baseURL string
	model   string
	apiKey  string
	client  *http.Client
}

// NewOpenAI builds a client. baseURL is the API root, for example
// "https://api.openai.com/v1".
func NewOpenAI(baseURL, model, apiKey string, timeout time.Duration) *OpenAI {
	return &OpenAI{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		apiKey:  apiKey,
		client:  &http.Client{Timeout: timeout},
	}
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature float64       `json:"temperature"`
	MaxTokens   int           `json:"max_tokens"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message      chatMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (o *OpenAI) Summarize(ctx context.Context, in Input) (string, error) {
	body, err := json.Marshal(chatRequest{
		Model: o.model,
		Messages: []chatMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: BuildPrompt(in)},
		},
		Temperature: 0.2,
		MaxTokens:   400,
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+o.apiKey)

	resp, err := o.client.Do(req)
	if err != nil {
		// The error text from net/http never includes request headers.
		return "", fmt.Errorf("AI request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", fmt.Errorf("read AI response: %w", err)
	}
	var parsed chatResponse
	jsonErr := json.Unmarshal(raw, &parsed)

	if resp.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(raw))
		if jsonErr == nil && parsed.Error != nil && parsed.Error.Message != "" {
			msg = parsed.Error.Message
		}
		return "", fmt.Errorf("AI provider returned %d: %s", resp.StatusCode, truncate(msg, 200))
	}
	if jsonErr != nil {
		return "", errors.New("AI provider returned a response that is not valid JSON")
	}
	if len(parsed.Choices) == 0 {
		return "", errors.New("AI provider returned no choices")
	}
	text := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if text == "" {
		return "", errors.New("AI provider returned an empty summary")
	}
	return truncate(text, maxSummaryRunes), nil
}

func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}
