package processing

import (
	"encoding/json"
	"time"

	"github.com/gisquick/gisquick-server/internal/domain"
)

// OGC API - Processes response types

// Link represents a link in OGC API responses.
type Link struct {
	Href  string `json:"href"`
	Rel   string `json:"rel,omitempty"`
	Type  string `json:"type,omitempty"`
	Title string `json:"title,omitempty"`
}

// LandingPage is the response for the root endpoint.
type LandingPage struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Links       []Link `json:"links"`
}

// ConformanceDeclaration lists the conformance classes supported.
type ConformanceDeclaration struct {
	ConformsTo []string `json:"conformsTo"`
}

// ProcessSummary is a summary of a process in the process list.
type ProcessSummary struct {
	ID                 string   `json:"id"`
	Title              string   `json:"title,omitempty"`
	Description        string   `json:"description,omitempty"`
	Keywords           []string `json:"keywords,omitempty"`
	Version            string   `json:"version"` // required by OGC API processSummary schema
	JobControlOptions  []string `json:"jobControlOptions,omitempty"`
	OutputTransmission []string `json:"outputTransmission,omitempty"`
	Links              []Link   `json:"links,omitempty"`
}

// ProcessList is the response for the process list endpoint.
type ProcessList struct {
	Processes []ProcessSummary `json:"processes"`
	Links     []Link           `json:"links"`
}

// StatusInfo represents the status of a job.
type StatusInfo struct {
	ProcessID string          `json:"processID,omitempty"`
	JobID     string          `json:"jobID"`
	Type      string          `json:"type"`              // required by OGC API statusInfo schema — always "process"
	Status    string          `json:"status"`
	Message   string          `json:"message,omitempty"`
	Created   *time.Time      `json:"created,omitempty"`
	Started   *time.Time      `json:"started,omitempty"`
	Finished  *time.Time      `json:"finished,omitempty"`
	Updated   *time.Time      `json:"updated,omitempty"`
	Progress  *int            `json:"progress,omitempty"`
	Links     []Link          `json:"links,omitempty"`
	Extra     json.RawMessage `json:"-"`
}

// ProxyExecuteRequest is the body sent to the processing proxy.
type ProxyExecuteRequest struct {
	Auth    string              `json:"auth"`
	Remote  domain.RemoteConfig `json:"remote"`
	Payload json.RawMessage     `json:"payload"`
}

// ProxyExecuteResponse is the response from the processing proxy.
type ProxyExecuteResponse struct {
	JobID       string          `json:"job_id,omitempty"`
	Status      string          `json:"status"`
	StoragePath string          `json:"storage_path,omitempty"`
	StatusURL   string          `json:"status_url,omitempty"`
	Artifacts   []Artifact      `json:"artifacts,omitempty"`
	ProjectQGZ  string          `json:"project_qgz,omitempty"`
	WmsURL            string          `json:"wms_url,omitempty"`
	OgcApiFeaturesURL string          `json:"ogcapi_features_url,omitempty"`
	RemoteTrace json.RawMessage `json:"remote_trace,omitempty"`
}

// Artifact represents a result file from the processing proxy.
type Artifact struct {
	OutputID    string `json:"output_id,omitempty"`
	Path        string `json:"path,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	SourceURL   string `json:"source_url,omitempty"`
	MediaKind   string `json:"media_kind,omitempty"`
	DownloadURL string `json:"download_url,omitempty"`
}

// JobRecord is the job metadata persisted in Redis for each submitted processing job.
type JobRecord struct {
	Version        int        `json:"version"`                    // Schema version, currently 1
	JobID          string     `json:"job_id"`                     // Our generated UUID (suffix of the Redis key)
	RemoteJobID    string     `json:"remote_job_id"`              // Remote backend job ID
	ServiceID      string     `json:"service_id"`                 // UUID of the ProcessingService config
	ProcessID      string     `json:"process_id"`                 // Original process ID without prefix
	ProcessTitle   string     `json:"process_title,omitempty"`    // Human-readable process name
	Project        string     `json:"project"`                    // "user/project"
	Username       string     `json:"username,omitempty"`         // User who submitted the job
	Status         string     `json:"status"`                     // Status at submission time
	StatusURL      string     `json:"status_url"`                 // Remote URL to poll for status
	StoragePath    string     `json:"storage_path,omitempty"`     // Internal QGIS artifact storage path
	CreatedAt      time.Time  `json:"created_at"`
	Artifacts      []Artifact `json:"artifacts"`                  // Our remapped download URLs
	WmsURL                    string     `json:"wms_url,omitempty"`                     // Our WMS proxy URL
	OgcApiFeaturesURL         string     `json:"ogcapi_features_url,omitempty"`         // Our OGC API Features proxy URL
	InternalWmsURL            string     `json:"internal_wms_url,omitempty"`            // Internal QGIS Server WMS URL for proxying
	InternalOgcApiFeaturesURL string     `json:"internal_ogcapi_features_url,omitempty"` // Internal QGIS Server OGC API Features URL for proxying
}
