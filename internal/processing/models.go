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

// ProxyExecuteRequest is the body sent to the QGIS Server processing proxy plugin.
type ProxyExecuteRequest struct {
	Auth            string                 `json:"auth"`
	Remote          domain.RemoteConfig    `json:"remote"`
	Execution       domain.ExecutionConfig `json:"execution"`
	ProjectRef      *ProjectRef            `json:"project_ref,omitempty"`
	ProjectInputs   []domain.ProjectInput  `json:"project_inputs,omitempty"`
	PayloadBindings map[string]string      `json:"payload_bindings,omitempty"`
	Payload         json.RawMessage        `json:"payload"`
}

// ProjectRef identifies the QGIS project file for layer data extraction.
type ProjectRef struct {
	Map string `json:"map"`
}

// ProxyExecuteResponse is the response from the processing proxy plugin.
type ProxyExecuteResponse struct {
	Status    string          `json:"status"`
	JobID     string          `json:"job_id,omitempty"`
	Outputs   json.RawMessage `json:"outputs,omitempty"`
	Artifacts []Artifact      `json:"artifacts,omitempty"`
	Error     string          `json:"error,omitempty"`
}

// Artifact represents a result file from the processing proxy.
type Artifact struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type,omitempty"`
	DownloadURL string `json:"download_url,omitempty"`
	WmsURL      string `json:"wms_url,omitempty"`
	WfsURL      string `json:"wfs_url,omitempty"`
}
