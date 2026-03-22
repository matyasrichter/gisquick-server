package processing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ProcessProxy abstracts communication with OGC API Processes backends.
type ProcessProxy interface {
	FetchProcessList(serviceURL string) ([]ProcessSummary, error)
	ForwardRequest(method, url string, body io.Reader, headers http.Header) (*http.Response, error)
	ExecuteViaProxy(ctx context.Context, proxyURL string, req *ProxyExecuteRequest) (*ProxyExecuteResponse, error)
}

// ProxyClient handles HTTP communication with OGC API Processes backends.
type ProxyClient struct {
	httpClient *http.Client
}

// NewProxyClient creates a new proxy client with default timeout.
func NewProxyClient() *ProxyClient {
	return &ProxyClient{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// FetchProcessList fetches the process list from a backend service.
func (p *ProxyClient) FetchProcessList(serviceURL string) ([]ProcessSummary, error) {
	resp, err := p.httpClient.Get(strings.TrimRight(serviceURL, "/") + "/processes")
	if err != nil {
		return nil, fmt.Errorf("fetching process list from %s: %w", serviceURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("backend %s returned status %d", serviceURL, resp.StatusCode)
	}

	var result ProcessList
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding process list from %s: %w", serviceURL, err)
	}
	return result.Processes, nil
}

// ForwardRequest forwards an HTTP request to a backend URL and returns the raw response.
func (p *ProxyClient) ForwardRequest(method, url string, body io.Reader, headers http.Header) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	for k, vals := range headers {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	return p.httpClient.Do(req)
}

// ExecuteViaProxy sends an execution request through the QGIS Server processing proxy plugin.
func (p *ProxyClient) ExecuteViaProxy(ctx context.Context, proxyURL string, req *ProxyExecuteRequest) (*ProxyExecuteResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshaling proxy request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating proxy request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Token "+req.Auth)

	resp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("sending proxy request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading proxy response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("proxy returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result ProxyExecuteResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decoding proxy response: %w", err)
	}
	return &result, nil
}

// PrefixProcessID adds a service UUID prefix to a process ID.
func PrefixProcessID(serviceID string, processID string) string {
	return serviceID + ":" + processID
}

// ParsePrefixedID parses a prefixed ID (processID or jobID) into service UUID and original ID.
func ParsePrefixedID(prefixedID string) (string, string, error) {
	parts := strings.SplitN(prefixedID, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid prefixed ID format: %s", prefixedID)
	}
	return parts[0], parts[1], nil
}
