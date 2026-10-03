// Package fingerprint groups dead-lettered messages that failed the same way.
//
// A fingerprint combines two things:
//   - an error signature: the failure text with volatile parts (ids,
//     timestamps, numbers) replaced by placeholders, and
//   - a payload shape: the JSON key paths and value types, never the values.
//
// Grouping is purely deterministic. No payload values leave this package.
package fingerprint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	"github.com/Sankartk/dlq-triage/internal/domain"
)

const (
	maxSignatureLen = 200
	maxShapeDepth   = 5
	maxShapePaths   = 200
)

// Config says where to look for the failure reason on a message.
type Config struct {
	// ErrorAttributes are message attribute names checked in order.
	ErrorAttributes []string
	// ErrorBodyPaths are dotted JSON paths in the body checked in order.
	ErrorBodyPaths []string
}

// Result is the outcome of fingerprinting one message.
type Result struct {
	Key      string // stable group key
	ErrorSig string // normalised failure text, empty when none was found
	Shape    string // payload shape, e.g. "json{id:string,loan.amount:number}"
}

var (
	uuidRe    = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	tsRe      = regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b`)
	emailRe   = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
	hexRe     = regexp.MustCompile(`\b(?:0x)?[0-9a-fA-F]{16,}\b`)
	numberRe  = regexp.MustCompile(`\b\d+(?:\.\d+)?\b`)
	spacesRe  = regexp.MustCompile(`\s+`)
	quotedRe  = regexp.MustCompile(`'[^']{1,80}'|"[^"]{1,80}"`)
	pathLikeR = regexp.MustCompile(`(?:[A-Za-z]:\\|/)[\w./\\-]{3,}`)
)

// Compute fingerprints one message.
func Compute(msg domain.Message, cfg Config) Result {
	sig := Normalize(extractError(msg, cfg))
	shape := Shape(msg.Body)
	sum := sha256.Sum256([]byte(sig + "\x00" + shape))
	return Result{
		Key:      hex.EncodeToString(sum[:8]),
		ErrorSig: sig,
		Shape:    shape,
	}
}

// Normalize replaces the volatile parts of a failure text so that the same
// failure produces the same signature regardless of ids, times and counts.
func Normalize(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Order matters: structured tokens first, bare numbers last.
	s = uuidRe.ReplaceAllString(s, "<uuid>")
	s = tsRe.ReplaceAllString(s, "<ts>")
	s = emailRe.ReplaceAllString(s, "<email>")
	s = hexRe.ReplaceAllString(s, "<hex>")
	s = pathLikeR.ReplaceAllString(s, "<path>")
	s = quotedRe.ReplaceAllString(s, "<str>")
	s = numberRe.ReplaceAllString(s, "<n>")
	s = spacesRe.ReplaceAllString(s, " ")
	if len(s) > maxSignatureLen {
		s = s[:maxSignatureLen]
	}
	return s
}

// Shape describes the structure of a payload without including any values.
func Shape(body string) string {
	trimmed := strings.TrimSpace(body)
	switch {
	case trimmed == "":
		return "empty"
	case strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "["):
		var v any
		if err := json.Unmarshal([]byte(trimmed), &v); err != nil {
			return "invalid-json"
		}
		paths := map[string]struct{}{}
		walk(v, "", 0, paths)
		out := make([]string, 0, len(paths))
		for p := range paths {
			out = append(out, p)
		}
		sort.Strings(out)
		if len(out) > maxShapePaths {
			out = out[:maxShapePaths]
		}
		return "json{" + strings.Join(out, ",") + "}"
	case strings.HasPrefix(trimmed, "<"):
		return "xml"
	default:
		return "text"
	}
}

func walk(v any, path string, depth int, out map[string]struct{}) {
	if len(out) >= maxShapePaths {
		return
	}
	switch t := v.(type) {
	case map[string]any:
		if depth >= maxShapeDepth {
			out[join(path, "")+":object"] = struct{}{}
			return
		}
		if len(t) == 0 {
			out[join(path, "")+":object"] = struct{}{}
			return
		}
		for k, child := range t {
			walk(child, join(path, k), depth+1, out)
		}
	case []any:
		p := path + "[]"
		if len(t) == 0 {
			out[p+":empty"] = struct{}{}
			return
		}
		for _, child := range t {
			walk(child, p, depth+1, out)
		}
	case string:
		out[path+":string"] = struct{}{}
	case float64:
		out[path+":number"] = struct{}{}
	case bool:
		out[path+":bool"] = struct{}{}
	case nil:
		out[path+":null"] = struct{}{}
	}
}

func join(path, key string) string {
	if key == "" {
		return path
	}
	if path == "" {
		return key
	}
	return path + "." + key
}

func extractError(msg domain.Message, cfg Config) string {
	for _, name := range cfg.ErrorAttributes {
		if a, ok := msg.Attributes[name]; ok && strings.TrimSpace(a.Value) != "" {
			return a.Value
		}
	}
	if len(cfg.ErrorBodyPaths) == 0 {
		return ""
	}
	var root any
	if err := json.Unmarshal([]byte(msg.Body), &root); err != nil {
		return ""
	}
	for _, p := range cfg.ErrorBodyPaths {
		if s, ok := lookup(root, p); ok && strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

func lookup(root any, path string) (string, bool) {
	cur := root
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = m[part]
		if !ok {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}
