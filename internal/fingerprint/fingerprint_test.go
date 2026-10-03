package fingerprint

import (
	"strings"
	"testing"

	"github.com/Sankartk/dlq-triage/internal/domain"
)

func TestNormalizeCollapsesVolatileParts(t *testing.T) {
	a := Normalize("Loan 12345 not found for user bob@example.com at 2026-10-03T10:11:12Z (req 3f2b8c1e-aaaa-bbbb-cccc-0123456789ab)")
	b := Normalize("Loan 98765 not found for user alice@corp.io at 2026-01-01T00:00:01.250+05:30 (req 11111111-2222-3333-4444-555555555555)")
	if a != b {
		t.Fatalf("expected equal signatures:\n  %q\n  %q", a, b)
	}
	for _, leaked := range []string{"12345", "bob@", "2026", "3f2b8c1e"} {
		if strings.Contains(a, leaked) {
			t.Errorf("signature %q still contains %q", a, leaked)
		}
	}
}

func TestNormalizeKeepsDistinctFailuresApart(t *testing.T) {
	a := Normalize("Loan 1 not found")
	b := Normalize("Timeout calling pricing service after 30 seconds")
	if a == b {
		t.Fatalf("distinct failures normalised to the same signature %q", a)
	}
}

func TestNormalizeTruncates(t *testing.T) {
	got := Normalize(strings.Repeat("error ", 200))
	if len(got) > maxSignatureLen {
		t.Fatalf("signature length %d exceeds %d", len(got), maxSignatureLen)
	}
}

func TestShapeIgnoresValuesAndKeyOrder(t *testing.T) {
	a := Shape(`{"loan":{"id":"L1","amount":1000},"tags":["a","b"]}`)
	b := Shape(`{"tags":["x"],"loan":{"amount":5,"id":"Z9"}}`)
	if a != b {
		t.Fatalf("shapes differ:\n  %s\n  %s", a, b)
	}
	if strings.Contains(a, "L1") || strings.Contains(a, "1000") {
		t.Fatalf("shape leaks values: %s", a)
	}
}

func TestShapeDistinguishesTypes(t *testing.T) {
	if Shape(`{"amount":10}`) == Shape(`{"amount":"10"}`) {
		t.Fatal("number and string fields must produce different shapes")
	}
	if Shape(`{"a":null}`) == Shape(`{"a":1}`) {
		t.Fatal("null and number fields must produce different shapes")
	}
}

func TestShapeNonJSON(t *testing.T) {
	cases := map[string]string{
		"":              "empty",
		"   ":           "empty",
		"hello":         "text",
		"<a><b/></a>":   "xml",
		`{"broken": `:   "invalid-json",
		`[1,2,3]`:       "json{[]:number}",
		`{}`:            "json{:object}",
		`{"list":[]}`:   "json{list[]:empty}",
		`[{"a":1},{}]`:  "json{[].a:number,[]:object}",
		"not json {x}":  "text",
		"[unterminated": "invalid-json",
	}
	for in, want := range cases {
		if got := Shape(in); got != want {
			t.Errorf("Shape(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestComputeUsesAttributeThenBody(t *testing.T) {
	cfg := Config{ErrorAttributes: []string{"ErrorMessage"}, ErrorBodyPaths: []string{"error.message"}}

	fromAttr := Compute(domain.Message{
		Body:       `{"id":"1"}`,
		Attributes: map[string]string{"ErrorMessage": "Loan 7 not found"},
	}, cfg)
	if fromAttr.ErrorSig != "Loan <n> not found" {
		t.Fatalf("attribute signature = %q", fromAttr.ErrorSig)
	}

	fromBody := Compute(domain.Message{
		Body: `{"id":"1","error":{"message":"Loan 8 not found"}}`,
	}, cfg)
	if fromBody.ErrorSig != "Loan <n> not found" {
		t.Fatalf("body signature = %q", fromBody.ErrorSig)
	}

	none := Compute(domain.Message{Body: `{"id":"1"}`}, cfg)
	if none.ErrorSig != "" {
		t.Fatalf("expected empty signature, got %q", none.ErrorSig)
	}
}

func TestComputeSameFailureSameKeyDifferentFailureDifferentKey(t *testing.T) {
	cfg := Config{ErrorAttributes: []string{"ErrorMessage"}}
	mk := func(id, errText, body string) domain.Message {
		return domain.Message{ID: id, Body: body, Attributes: map[string]string{"ErrorMessage": errText}}
	}
	a := Compute(mk("m1", "Loan 1 not found", `{"loan":"L1"}`), cfg)
	b := Compute(mk("m2", "Loan 2 not found", `{"loan":"L2"}`), cfg)
	c := Compute(mk("m3", "Timeout after 30s", `{"loan":"L3"}`), cfg)
	d := Compute(mk("m4", "Loan 4 not found", `{"loan":"L4","extra":true}`), cfg)

	if a.Key != b.Key {
		t.Errorf("same failure, same shape should group: %s vs %s", a.Key, b.Key)
	}
	if a.Key == c.Key {
		t.Error("different error text must not group")
	}
	if a.Key == d.Key {
		t.Error("different payload shape must not group")
	}
	if len(a.Key) != 16 {
		t.Errorf("key length = %d, want 16", len(a.Key))
	}
}

func TestComputeIsStable(t *testing.T) {
	cfg := Config{ErrorAttributes: []string{"e"}}
	m := domain.Message{Body: `{"a":1,"b":{"c":"x"}}`, Attributes: map[string]string{"e": "boom 5"}}
	first := Compute(m, cfg)
	for i := 0; i < 50; i++ {
		if got := Compute(m, cfg); got != first {
			t.Fatalf("run %d produced %+v, want %+v", i, got, first)
		}
	}
}
