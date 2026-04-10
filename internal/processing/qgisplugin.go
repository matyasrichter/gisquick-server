package processing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// QGISPluginClient calls the QGIS Server project-creation plugin.
// The plugin lives on the same QGIS Server instance as the regular map server.
type QGISPluginClient struct {
	httpClient   *http.Client
	mapserverURL string
	secret       string
}

// NewQGISPluginClient creates a client for the QGIS project-creation plugin.
func NewQGISPluginClient(mapserverURL, secret string) *QGISPluginClient {
	return &QGISPluginClient{
		httpClient:   &http.Client{Timeout: 60 * time.Second},
		mapserverURL: mapserverURL,
		secret:       secret,
	}
}

// createProjectURL derives the plugin endpoint from the mapserver URL.
// The new plugin is mounted at /qgis-server/qgis_mapserv.fcgi/ogc/gisquick-project-from-file
// on the same QGIS Server instance.
func (q *QGISPluginClient) createProjectURL() (string, error) {
	parsed, err := url.Parse(q.mapserverURL)
	if err != nil {
		return "", fmt.Errorf("parsing mapserverURL: %w", err)
	}
	return fmt.Sprintf("%s://%s/qgis-server/qgis_mapserv.fcgi/ogc/gisquick-project-from-file", parsed.Scheme, parsed.Host), nil
}

// CreateProject asks the QGIS plugin to create a project file from the given artifacts.
// The plugin reads the files from jobDir (on the shared volume) and writes a .qgs file
// to the same directory. Returns the filename of the created project file.
//
// Failure is treated as non-fatal by the caller — WMS/WFS simply won't be available
// if this step fails.
func (q *QGISPluginClient) CreateProject(ctx context.Context, jobDir, serviceURL string, artifacts []Artifact) (string, error) {
	endpoint, err := q.createProjectURL()
	if err != nil {
		return "", err
	}

	files := make([]QGISCreateProjectFile, 0, len(artifacts))
	for _, a := range artifacts {
		files = append(files, QGISCreateProjectFile{
			Path: a.Path,
			Type: a.ContentType,
		})
	}

	reqBody := QGISCreateProjectRequest{
		JobDir:     jobDir,
		ServiceURL: serviceURL,
		Files:      files,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling gisquick-project-from-file request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("building gisquick-project-from-file request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if q.secret != "" {
		req.Header.Set("Authorization", "Token "+q.secret)
	}

	resp, err := q.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("calling gisquick-project-from-file plugin: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading gisquick-project-from-file response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("gisquick-project-from-file plugin returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result QGISCreateProjectResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("decoding gisquick-project-from-file response: %w", err)
	}
	if result.ProjectFile == "" {
		return "", fmt.Errorf("gisquick-project-from-file plugin returned empty project_file")
	}
	return result.ProjectFile, nil
}
