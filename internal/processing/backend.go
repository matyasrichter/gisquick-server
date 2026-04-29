package processing

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gisquick/gisquick-server/internal/domain"
)

// ProcessDescription holds the structured fields of an OGC API – Processes
// process description document. It is used internally when reading
// jobControlOptions at execution time to decide async vs. sync mode.
// The Raw field retains the original JSON for pass-through serialisation.
type ProcessDescription struct {
	Title             string          `json:"title,omitempty"`
	Description       string          `json:"description,omitempty"`
	Version           string          `json:"version,omitempty"`
	JobControlOptions []string        `json:"jobControlOptions,omitempty"`
	Inputs            json.RawMessage `json:"inputs,omitempty"`
	Outputs           json.RawMessage `json:"outputs,omitempty"`
	Raw               json.RawMessage `json:"-"` // full original JSON
}

// ProcessingBackend is the interface implemented by every processing backend
// (OGC API – Processes, WPS, …). Handlers obtain a concrete implementation
// through NewBackend and call these three methods during the job lifecycle.
type ProcessingBackend interface {
	// FetchProcessList retrieves the list of available process summaries from
	// the remote service.
	FetchProcessList(ctx context.Context, service domain.ProcessingService) ([]ProcessSummary, error)

	// DescribeProcess fetches the full description of a single process.
	DescribeProcess(ctx context.Context, service domain.ProcessingService, processID string) (*ProcessDescription, error)

	// Execute submits an execution request and waits for it to complete (polling
	// as needed). It returns the output results, the remote job ID (empty for
	// synchronous responses), and any error.
	Execute(ctx context.Context, job *JobRecord, service domain.ProcessingService, inputs json.RawMessage) (results []OutputResult, remoteJobID string, err error)
}

// NewBackend returns the appropriate ProcessingBackend implementation for the
// given service type. Returns nil for unknown service types — callers must
// guard against a nil return.
func NewBackend(service domain.ProcessingService, httpClient *http.Client) ProcessingBackend {
	switch service.Type {
	case domain.ProcessingServiceTypeOGCProcesses:
		// TODO: accept a logger; for now the OGCAPIBackend carries its own client.
		client := &OGCAPIClient{httpClient: httpClient}
		return &OGCAPIBackend{client: client}
	case domain.ProcessingServiceTypeWPS:
		// WPS backend — to be implemented in the next task.
		return &WPSBackend{}
	default:
		return nil
	}
}
