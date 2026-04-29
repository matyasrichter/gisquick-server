package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gisquick/gisquick-server/internal/domain"
	"go.uber.org/zap"
)

const (
	maxPollInterval     = 30 * time.Second
	initialPollInterval = 1 * time.Second
)

// OGCAPIClient handles direct HTTP communication with OGC API Processes backends.
type OGCAPIClient struct {
	httpClient *http.Client
	log        *zap.SugaredLogger
}

// NewOGCAPIClient creates a new OGC API client with a 60-second per-request timeout.
func NewOGCAPIClient(log *zap.SugaredLogger) *OGCAPIClient {
	return &OGCAPIClient{
		httpClient: &http.Client{Timeout: 60 * time.Second},
		log:        log,
	}
}

// ForwardRequest forwards an HTTP request to a URL and returns the raw response.
// Used for fetching remote process lists and descriptions.
func (c *OGCAPIClient) ForwardRequest(method, url string, body io.Reader, headers http.Header) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	for k, vals := range headers {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	return c.httpClient.Do(req)
}

// Execute calls a remote OGC API Processes backend, handles both sync (200) and
// async (201 + polling) responses, and returns the raw output results.
// The caller is responsible for setting an appropriate deadline on ctx.
func (c *OGCAPIClient) Execute(ctx context.Context, remote domain.RemoteConfig, payload []byte) (remoteJobID string, results []OutputResult, err error) {
	executeURL := remote.ExecuteURL
	if executeURL == "" {
		return "", nil, fmt.Errorf("remote.ExecuteURL is empty")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, executeURL, strings.NewReader(string(payload)))
	if err != nil {
		return "", nil, fmt.Errorf("building execute request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "respond-async")
	for k, v := range remote.Headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("sending execute request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// Synchronous response — results are in the body.
		results, err = c.parseResultsBody(resp.Body)
		return "", results, err

	case http.StatusCreated:
		// Asynchronous — poll the Location URL.
		statusURL := resp.Header.Get("Location")
		if statusURL == "" {
			statusURL = remote.StatusURL
		}
		if statusURL == "" {
			return "", nil, fmt.Errorf("async execute: no Location header and no StatusURL configured")
		}
		return c.pollAndFetch(ctx, statusURL, remote)

	default:
		body, _ := io.ReadAll(resp.Body)
		return "", nil, fmt.Errorf("execute returned status %d: %s", resp.StatusCode, string(body))
	}
}

// pollAndFetch polls the status URL until the job succeeds or fails, then fetches results.
func (c *OGCAPIClient) pollAndFetch(ctx context.Context, statusURL string, remote domain.RemoteConfig) (remoteJobID string, results []OutputResult, err error) {
	interval := initialPollInterval

	for {
		select {
		case <-ctx.Done():
			return "", nil, ctx.Err()
		case <-time.After(interval):
		}

		status, err := c.fetchStatus(ctx, statusURL, remote.Headers)
		if err != nil {
			return "", nil, fmt.Errorf("polling status: %w", err)
		}

		c.log.Debugw("OGC job poll", "jobID", status.JobID, "status", status.Status)

		switch status.Status {
		case "successful":
			resultsURL := c.findResultsURL(status.Links)
			if resultsURL == "" {
				if remote.ResultURL != "" {
					resultsURL = remote.ResultURL
				} else {
					// Derive from status URL: replace trailing /status with /results
					resultsURL = strings.TrimSuffix(statusURL, "/") + "/results"
					// Many backends use {jobsURL}/{jobID}/results
				}
			}
			results, err := c.fetchResults(ctx, resultsURL, remote.Headers)
			return status.JobID, results, err

		case "failed":
			return status.JobID, nil, fmt.Errorf("remote job failed: %s", status.Message)

		case "dismissed":
			return status.JobID, nil, fmt.Errorf("remote job was dismissed")

		default:
			// accepted / running — keep polling with backoff
			interval *= 2
			if interval > maxPollInterval {
				interval = maxPollInterval
			}
		}
	}
}

// fetchStatus retrieves the current status of a remote job.
func (c *OGCAPIClient) fetchStatus(ctx context.Context, statusURL string, headers map[string]string) (*OGCStatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var status OGCStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("decoding status response: %w", err)
	}
	return &status, nil
}

// fetchResults retrieves and parses the results document from the results URL.
func (c *OGCAPIClient) fetchResults(ctx context.Context, resultsURL string, headers map[string]string) ([]OutputResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resultsURL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching results: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("results endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	return c.parseResultsBody(resp.Body)
}

// parseResultsBody parses the OGC API results document.
//
// OGC API - Processes Part 1 defines the results document as a map where each
// key is an output ID and each value is either:
//   - A qualified value object: {"value": <any>, "mediaType": "..."}
//   - A link object:           {"href": "...", "rel": "...", "type": "..."}
func (c *OGCAPIClient) parseResultsBody(r io.Reader) ([]OutputResult, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading results body: %w", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding results JSON: %w", err)
	}

	var outputs []OutputResult
	for outputID, rawVal := range raw {
		out, err := parseOutputValue(outputID, rawVal)
		if err != nil {
			c.log.Warnw("skipping unparseable output", "output", outputID, zap.Error(err))
			continue
		}
		outputs = append(outputs, out)
	}
	return outputs, nil
}

// parseOutputValue parses a single output value from the results document.
func parseOutputValue(outputID string, raw json.RawMessage) (OutputResult, error) {
	// Try link object first (by-reference)
	var link struct {
		Href  string `json:"href"`
		Type  string `json:"type"`
		Title string `json:"title"`
	}
	if err := json.Unmarshal(raw, &link); err == nil && link.Href != "" {
		return OutputResult{
			OutputID:    outputID,
			Reference:   link.Href,
			ContentType: link.Type,
		}, nil
	}

	// Try qualified value object (by-value): {"value": ..., "mediaType": "..."}
	var qualified struct {
		Value     json.RawMessage `json:"value"`
		MediaType string          `json:"mediaType"`
	}
	if err := json.Unmarshal(raw, &qualified); err == nil && qualified.Value != nil {
		valueBytes, err := marshalOutputValue(qualified.Value)
		if err != nil {
			return OutputResult{}, fmt.Errorf("marshaling value for output %q: %w", outputID, err)
		}
		return OutputResult{
			OutputID:    outputID,
			Value:       valueBytes,
			ContentType: qualified.MediaType,
		}, nil
	}

	// Fallback: treat the whole raw JSON as a by-value output
	return OutputResult{
		OutputID:    outputID,
		Value:       []byte(raw),
		ContentType: "application/json",
	}, nil
}

// marshalOutputValue converts a JSON-decoded value back to bytes for writing to disk.
// If the value is a JSON string (e.g. base64), it unwraps the string content.
func marshalOutputValue(raw json.RawMessage) ([]byte, error) {
	// If the JSON value is a string, return its unquoted content (handles base64-encoded binaries).
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		return []byte(s), nil
	}
	return []byte(raw), nil
}

// OGCAPIBackend implements ProcessingBackend for OGC API – Processes services.
// It delegates all HTTP communication to an OGCAPIClient.
type OGCAPIBackend struct {
	client *OGCAPIClient
}

// FetchProcessList retrieves the process list from the remote OGC API endpoint.
func (b *OGCAPIBackend) FetchProcessList(ctx context.Context, service domain.ProcessingService) ([]ProcessSummary, error) {
	base := strings.TrimRight(service.URL, "/")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/processes", nil)
	if err != nil {
		return nil, fmt.Errorf("building process list request: %w", err)
	}
	for k, v := range service.Headers {
		req.Header.Set(k, v)
	}
	resp, err := b.client.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching process list: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("process list returned status %d", resp.StatusCode)
	}

	var list ProcessList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("decoding process list: %w", err)
	}
	return list.Processes, nil
}

// DescribeProcess fetches the full description of a single process from the
// remote OGC API endpoint.
func (b *OGCAPIBackend) DescribeProcess(ctx context.Context, service domain.ProcessingService, processID string) (*ProcessDescription, error) {
	base := strings.TrimRight(service.URL, "/")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/processes/"+processID, nil)
	if err != nil {
		return nil, fmt.Errorf("building describe request for %q: %w", processID, err)
	}
	for k, v := range service.Headers {
		req.Header.Set(k, v)
	}
	resp, err := b.client.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching process description for %q: %w", processID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("process description for %q returned status %d", processID, resp.StatusCode)
	}

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading process description for %q: %w", processID, err)
	}

	var desc ProcessDescription
	if err := json.Unmarshal(raw, &desc); err != nil {
		return nil, fmt.Errorf("parsing process description for %q: %w", processID, err)
	}
	desc.Raw = json.RawMessage(raw)
	return &desc, nil
}

// Execute submits a job to the remote OGC API – Processes endpoint and waits
// for it to complete. It returns the output results and the remote job ID.
// The job parameter is present for interface compatibility; the backend does
// not mutate it — mutation is the caller's responsibility.
func (b *OGCAPIBackend) Execute(ctx context.Context, job *JobRecord, service domain.ProcessingService, inputs json.RawMessage) ([]OutputResult, string, error) {
	remote := domain.RemoteConfig{
		ExecuteURL: strings.TrimRight(service.URL, "/") + "/processes/" + job.ProcessID + "/execution",
		Type:       string(service.Type),
		Headers:    service.Headers,
	}
	remoteJobID, results, err := b.client.Execute(ctx, remote, []byte(inputs))
	return results, remoteJobID, err
}

// findResultsURL looks for the results link in an OGC status response.
func (c *OGCAPIClient) findResultsURL(links []Link) string {
	// Prefer a results link that explicitly asks for JSON.
	for _, l := range links {
		if !strings.Contains(l.Rel, "results") {
			continue
		}
		if l.Type == "application/json" {
			return l.Href
		}
		href := strings.ToLower(l.Href)
		if strings.Contains(href, "f=json") || strings.Contains(href, "format=json") || strings.HasSuffix(href, ".json") {
			return l.Href
		}
	}

	// Fallback to any results link, including links using res=results.
	for _, l := range links {
		href := strings.ToLower(l.Href)
		if strings.Contains(l.Rel, "results") || strings.Contains(href, "res=results") {
			return l.Href
		}
	}
	return ""
}
