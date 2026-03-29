package processing

import (
	"encoding/json"

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
	ID          string `json:"id"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Version     string `json:"version,omitempty"`
	Links       []Link `json:"links,omitempty"`
}

// ProcessList is the response for the process list endpoint.
type ProcessList struct {
	Processes []ProcessSummary `json:"processes"`
	Links     []Link           `json:"links"`
}

// StatusInfo represents the status of a job.
type StatusInfo struct {
	JobID   string          `json:"jobID"`
	Status  string          `json:"status"`
	Type    string          `json:"type,omitempty"`
	Message string          `json:"message,omitempty"`
	Links   []Link          `json:"links,omitempty"`
	Extra   json.RawMessage `json:"-"`
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
	WmsURL      string          `json:"wms_url,omitempty"`
	WfsURL      string          `json:"wfs_url,omitempty"`
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
