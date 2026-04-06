package processing

import (
	"encoding/json"
	"time"
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
	Type      string          `json:"type"`   // required by OGC API statusInfo schema — always "process"
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

// OGCStatusResponse is the status document returned by a remote OGC API backend.
type OGCStatusResponse struct {
	JobID    string `json:"jobID"`
	Status   string `json:"status"`
	Message  string `json:"message,omitempty"`
	Progress *int   `json:"progress,omitempty"`
	Links    []Link `json:"links,omitempty"`
}

// OutputResult holds a single output value from an OGC API execution result.
// Either Value (by-value) or Reference (by-reference) is set.
type OutputResult struct {
	OutputID    string
	Value       []byte // non-nil for by-value outputs
	Reference   string // URL for by-reference outputs
	ContentType string
	Filename    string // optional filename hint
}

// Artifact represents a result file saved to disk after execution.
type Artifact struct {
	OutputID    string `json:"output_id,omitempty"`
	Path        string `json:"path,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	SizeBytes   int64  `json:"size_bytes,omitempty"`
	MediaKind   string `json:"media_kind,omitempty"`
	DownloadURL string `json:"download_url,omitempty"`
}

// JobRecord is the job metadata persisted in Redis for each submitted processing job.
type JobRecord struct {
	Version      int        `json:"version"`                 // Schema version, currently 2
	JobID        string     `json:"job_id"`                  // Our generated UUID (suffix of the Redis key)
	RemoteJobID  string     `json:"remote_job_id"`           // Remote backend job ID
	ServiceID    string     `json:"service_id"`              // UUID of the ProcessingService config
	ProcessID    string     `json:"process_id"`              // Original process ID without prefix
	ProcessTitle string     `json:"process_title,omitempty"` // Human-readable process name
	Project      string     `json:"project"`                 // "user/project"
	Username     string     `json:"username,omitempty"`      // User who submitted the job
	Status       string     `json:"status"`                  // Current job status
	Message      string     `json:"message,omitempty"`       // Error or status message
	StoragePath  string     `json:"storage_path,omitempty"`  // Absolute path to job directory on disk
	ProjectFile  string     `json:"project_file,omitempty"`  // QGIS project filename within StoragePath
	CreatedAt    time.Time  `json:"created_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"` // Set when background goroutine completes
	Artifacts    []Artifact `json:"artifacts"`             // Artifacts with client-facing download URLs
	WmsURL       string     `json:"wms_url,omitempty"`     // Our WMS proxy URL (client-facing)
	OgcApiFeaturesURL string `json:"ogcapi_features_url,omitempty"` // Our OGC API Features proxy URL (client-facing)
}

// QGISCreateProjectRequest is the request body sent to the QGIS project-creation plugin.
type QGISCreateProjectRequest struct {
	JobDir string                  `json:"job_dir"`
	Files  []QGISCreateProjectFile `json:"files"`
}

// QGISCreateProjectFile describes one artifact file for the QGIS plugin.
type QGISCreateProjectFile struct {
	Path string `json:"path"`
	Type string `json:"type"`
}

// QGISCreateProjectResponse is the response from the QGIS project-creation plugin.
type QGISCreateProjectResponse struct {
	ProjectFile string `json:"project_file"`
}
