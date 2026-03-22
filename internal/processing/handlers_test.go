package processing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gisquick/gisquick-server/internal/domain"
	"github.com/gisquick/gisquick-server/internal/mock"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// newTestHandlers creates a Handlers instance suitable for unit tests.
func newTestHandlers(projects *mock.ProjectService, proxy ProcessProxy) *Handlers {
	return &Handlers{
		projects: projects,
		proxy:    proxy,
		log:      zap.NewNop().Sugar(),
	}
}

// newEchoCtx returns an Echo context backed by a response recorder.
func newEchoCtx(method, path, body string) (echo.Context, *httptest.ResponseRecorder) {
	e := echo.New()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Set("project", "user/proj")
	return c, rec
}

// --- Pure function tests ---

func TestPrefixProcessID(t *testing.T) {
	got := PrefixProcessID("abc", "buffer")
	if got != "abc:buffer" {
		t.Errorf("expected 'abc:buffer', got %q", got)
	}
}

func TestParsePrefixedID(t *testing.T) {
	// round-trip with UUID-style prefix
	svcID, id, err := ParsePrefixedID("abc:clip")
	if err != nil || svcID != "abc" || id != "clip" {
		t.Errorf("round-trip failed: svcID=%q id=%q err=%v", svcID, id, err)
	}

	// error: no colon
	_, _, err = ParsePrefixedID("nocolon")
	if err == nil {
		t.Error("expected error for missing colon")
	}
}

func TestExtractJobID(t *testing.T) {
	// normal jobs URL
	if id := extractJobID("http://example.com/jobs/abc123"); id != "abc123" {
		t.Errorf("expected 'abc123', got %q", id)
	}
	// trailing path after job id
	if id := extractJobID("http://example.com/jobs/xyz/results"); id != "xyz" {
		t.Errorf("expected 'xyz', got %q", id)
	}
	// no jobs segment
	if id := extractJobID("http://example.com/processes/buffer"); id != "" {
		t.Errorf("expected empty string, got %q", id)
	}
}

func TestServiceRequestValidate(t *testing.T) {
	cases := []struct {
		req     serviceRequest
		wantErr bool
	}{
		{serviceRequest{URL: "http://x", Type: domain.ProcessingServiceTypeWPS}, false},
		{serviceRequest{URL: "http://x", Type: domain.ProcessingServiceTypeOGCProcesses}, false},
		{serviceRequest{URL: "", Type: domain.ProcessingServiceTypeWPS}, true},
		{serviceRequest{URL: "http://x", Type: ""}, true},
		{serviceRequest{URL: "http://x", Type: "invalid"}, true},
	}
	for _, tc := range cases {
		err := tc.req.validate()
		if (err != nil) != tc.wantErr {
			t.Errorf("validate(%+v) err=%v, wantErr=%v", tc.req, err, tc.wantErr)
		}
	}
}

// --- Handler tests ---

func TestHandleGetProcessingConfig(t *testing.T) {
	cfg := domain.ProcessingConfig{
		Services: []domain.ProcessingService{
			{URL: "http://wps.example.com", Type: domain.ProcessingServiceTypeWPS},
		},
	}
	projects := &mock.ProjectService{
		GetProcessingConfigFunc: func(n string) (domain.ProcessingConfig, error) {
			return cfg, nil
		},
	}

	h := newTestHandlers(projects, nil)
	c, rec := newEchoCtx(http.MethodGet, "/", "")

	if err := h.HandleGetProcessingConfig()(c); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var got domain.ProcessingConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if len(got.Services) != 1 || got.Services[0].URL != "http://wps.example.com" {
		t.Errorf("unexpected response: %+v", got)
	}
}

func TestHandleAddProcessingServiceWPS(t *testing.T) {
	var saved domain.ProcessingConfig
	projects := &mock.ProjectService{
		GetProcessingConfigFunc: func(n string) (domain.ProcessingConfig, error) {
			return domain.ProcessingConfig{}, nil
		},
		UpdateProcessingConfigFunc: func(n string, cfg domain.ProcessingConfig) error {
			saved = cfg
			return nil
		},
	}

	// WPS type: proxy.FetchProcessList must NOT be called
	h := newTestHandlers(projects, nil) // nil proxy is safe for WPS
	body := `{"url":"http://wps.example.com","type":"wps","name":"My WPS"}`
	c, rec := newEchoCtx(http.MethodPost, "/", body)

	if err := h.HandleAddProcessingService()(c); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(saved.Services) != 1 {
		t.Fatalf("expected 1 saved service, got %d", len(saved.Services))
	}
	svc := saved.Services[0]
	if svc.URL != "http://wps.example.com" || svc.Type != domain.ProcessingServiceTypeWPS {
		t.Errorf("unexpected saved service: %+v", svc)
	}
	if svc.ID == "" {
		t.Error("expected non-empty service ID")
	}
}

func TestHandleAddProcessingServiceOGC(t *testing.T) {
	// Real httptest server serving a /processes endpoint
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/processes" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ProcessList{
				Processes: []ProcessSummary{
					{ID: "buffer"},
					{ID: "clip"},
				},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	var saved domain.ProcessingConfig
	projects := &mock.ProjectService{
		GetProcessingConfigFunc: func(n string) (domain.ProcessingConfig, error) {
			return domain.ProcessingConfig{}, nil
		},
		UpdateProcessingConfigFunc: func(n string, cfg domain.ProcessingConfig) error {
			saved = cfg
			return nil
		},
	}

	h := newTestHandlers(projects, NewProxyClient())
	body := `{"url":"` + ts.URL + `","type":"ogcapi-processes","name":"OGC"}`
	c, rec := newEchoCtx(http.MethodPost, "/", body)

	if err := h.HandleAddProcessingService()(c); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(saved.Services) != 1 {
		t.Fatalf("expected 1 saved service, got %d", len(saved.Services))
	}
	svc := saved.Services[0]
	if svc.ID == "" {
		t.Error("expected non-empty service ID")
	}
	if len(svc.Processes) != 2 {
		t.Errorf("expected 2 auto-populated processes, got %d: %v", len(svc.Processes), svc.Processes)
	}
	if _, ok := svc.Processes["buffer"]; !ok {
		t.Error("expected 'buffer' in processes")
	}
	if _, ok := svc.Processes["clip"]; !ok {
		t.Error("expected 'clip' in processes")
	}
}

func TestHandleUpdateProcessingService(t *testing.T) {
	const svcID = "550e8400-e29b-41d4-a716-446655440000"
	initial := domain.ProcessingConfig{
		Services: []domain.ProcessingService{
			{ID: svcID, URL: "http://old.example.com", Type: domain.ProcessingServiceTypeWPS, Name: "Old"},
		},
	}
	var saved domain.ProcessingConfig
	projects := &mock.ProjectService{
		GetProcessingConfigFunc: func(n string) (domain.ProcessingConfig, error) {
			return initial, nil
		},
		UpdateProcessingConfigFunc: func(n string, cfg domain.ProcessingConfig) error {
			saved = cfg
			return nil
		},
	}

	h := newTestHandlers(projects, nil)
	body := `{"url":"http://new.example.com","type":"wps","name":"New"}`
	c, rec := newEchoCtx(http.MethodPut, "/", body)
	c.SetParamNames("id")
	c.SetParamValues(svcID)

	if err := h.HandleUpdateProcessingService()(c); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(saved.Services) != 1 {
		t.Fatalf("expected 1 service, got %d", len(saved.Services))
	}
	svc := saved.Services[0]
	if svc.URL != "http://new.example.com" || svc.Name != "New" {
		t.Errorf("unexpected updated service: %+v", svc)
	}
	if svc.ID != svcID {
		t.Errorf("expected ID %q to be preserved, got %q", svcID, svc.ID)
	}
}

func TestHandleDeleteProcessingService(t *testing.T) {
	const firstID = "aaaaaaaa-0000-0000-0000-000000000001"
	const secondID = "bbbbbbbb-0000-0000-0000-000000000002"
	initial := domain.ProcessingConfig{
		Services: []domain.ProcessingService{
			{ID: firstID, URL: "http://first.example.com", Type: domain.ProcessingServiceTypeWPS},
			{ID: secondID, URL: "http://second.example.com", Type: domain.ProcessingServiceTypeWPS},
		},
	}
	var saved domain.ProcessingConfig
	projects := &mock.ProjectService{
		GetProcessingConfigFunc: func(n string) (domain.ProcessingConfig, error) {
			return initial, nil
		},
		UpdateProcessingConfigFunc: func(n string, cfg domain.ProcessingConfig) error {
			saved = cfg
			return nil
		},
	}

	h := newTestHandlers(projects, nil)
	c, rec := newEchoCtx(http.MethodDelete, "/", "")
	c.SetParamNames("id")
	c.SetParamValues(firstID)

	if err := h.HandleDeleteProcessingService()(c); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(saved.Services) != 1 {
		t.Fatalf("expected 1 remaining service, got %d", len(saved.Services))
	}
	if saved.Services[0].URL != "http://second.example.com" {
		t.Errorf("wrong service remaining: %+v", saved.Services[0])
	}
}
