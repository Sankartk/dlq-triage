package api

import "github.com/Sankartk/dlq-triage/internal/fingerprint"

func fingerprintConfig() fingerprint.Config {
	return fingerprint.Config{ErrorAttributes: []string{"ErrorMessage"}}
}
