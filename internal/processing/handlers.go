package processing

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/gisquick/gisquick-server/internal/application"
	"github.com/gisquick/gisquick-server/internal/domain"
	"github.com/gofrs/uuid"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// jobGeoServices holds the internal WMS/WFS URLs for a completed processing job.
type jobGeoServices struct {
	wmsURL string
	wfsURL string
}

// Handlers provides HTTP handlers for the processing module.
type Handlers struct {
	projects     application.ProjectService
	proxy        ProcessProxy
	log          *zap.SugaredLogger
	mapserverURL string
	proxySecret  string
	mu           sync.RWMutex
	jobURLs      map[string]jobGeoServices // prefixedJobID → internal geo service URLs
}

func NewHandlers(projects application.ProjectService, log *zap.SugaredLogger, mapserverURL, proxySecret string) *Handlers {
	return &Handlers{
		projects:     projects,
		proxy:        NewProxyClient(),
		log:          log,
		mapserverURL: mapserverURL,
		proxySecret:  proxySecret,
		jobURLs:      make(map[string]jobGeoServices),
	}
}

// baseURL returns the base URL for OGC API Processes endpoints for a project.
func baseURL(c echo.Context, projectName string) string {
	scheme := "http"
	if c.Request().TLS != nil {
		scheme = "https"
	}
	if fwd := c.Request().Header.Get("X-Forwarded-Proto"); fwd != "" {
		scheme = fwd
	}
	host := c.Request().Host
	if fwdHost := c.Request().Header.Get("X-Forwarded-Host"); fwdHost != "" {
		host = fwdHost
	}

	return fmt.Sprintf("%s://%s/api/map/ogc-processes/%s", scheme, host, projectName)
}

// findOGCServiceByID finds an OGC service by its UUID among OGC-type services.
func findOGCServiceByID(services []domain.ProcessingService, serviceID string) (domain.ProcessingService, bool) {
	for _, s := range services {
		if s.ID == serviceID {
			return s, true
		}
	}
	return domain.ProcessingService{}, false
}

// ogcServices filters configured services to only OGC API Processes backends.
// this is temporary until we support WPS
func ogcServices(cfg domain.ProcessingConfig) []domain.ProcessingService {
	var services []domain.ProcessingService
	for _, s := range cfg.Services {
		if s.Type == domain.ProcessingServiceTypeOGCProcesses {
			services = append(services, s)
		}
	}
	return services
}

// serviceRequest is the shared request body for processing service CRUD operations.
type serviceRequest struct {
	URL       string                          `json:"url"`
	Type      domain.ProcessingServiceType    `json:"type"`
	Name      string                          `json:"name"`
	Processes map[string]domain.ProcessConfig `json:"processes,omitempty"`
}

func (r *serviceRequest) validate() error {
	if r.URL == "" || r.Type == "" {
		return fmt.Errorf("both 'url' and 'type' are required")
	}
	if r.Type != domain.ProcessingServiceTypeWPS && r.Type != domain.ProcessingServiceTypeOGCProcesses {
		return fmt.Errorf("invalid type; expected 'wps' or 'ogcapi-processes'")
	}
	return nil
}

func (r *serviceRequest) toService() domain.ProcessingService {
	return domain.ProcessingService{
		URL:       r.URL,
		Type:      r.Type,
		Name:      r.Name,
		Processes: r.Processes,
	}
}

// remoteProcessList is used to parse the process list response from an OGC API backend.
type remoteProcessList struct {
	Processes []struct {
		ID string `json:"id"`
	} `json:"processes"`
}

// fetchRemoteProcesses retrieves the full process descriptions from an OGC API backend.
// The returned map is keyed by process ID. Title and Description are populated from the remote;
// other ProcessConfig fields are left at their zero values for the caller to overlay.
func (h *Handlers) fetchRemoteProcesses(svcURL string) (map[string]domain.ProcessConfig, error) {
	base := strings.TrimRight(svcURL, "/")

	resp, err := h.proxy.ForwardRequest(http.MethodGet, base+"/processes", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("fetching process list: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("process list returned status %d", resp.StatusCode)
	}

	var list remoteProcessList
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("decoding process list: %w", err)
	}

	result := make(map[string]domain.ProcessConfig, len(list.Processes))
	for _, p := range list.Processes {
		descResp, err := h.proxy.ForwardRequest(http.MethodGet, base+"/processes/"+p.ID, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("fetching process %q: %w", p.ID, err)
		}
		defer descResp.Body.Close()
		if descResp.StatusCode < 200 || descResp.StatusCode >= 300 {
			return nil, fmt.Errorf("process %q description returned status %d", p.ID, descResp.StatusCode)
		}

		raw, err := io.ReadAll(descResp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading process %q description: %w", p.ID, err)
		}

		var meta struct {
			Title string `json:"title"`
		}
		_ = json.Unmarshal(raw, &meta)

		result[p.ID] = domain.ProcessConfig{
			Title:       meta.Title,
			Description: json.RawMessage(raw),
		}
	}
	return result, nil
}

// mergeProcessConfigs builds the final process map for a service by starting from the
// remotely fetched descriptions and overlaying the proxy config from any user-provided overrides.
func mergeProcessConfigs(fetched map[string]domain.ProcessConfig, overrides map[string]domain.ProcessConfig) map[string]domain.ProcessConfig {
	for id, override := range overrides {
		if cfg, ok := fetched[id]; ok {
			cfg.Remote = override.Remote
			fetched[id] = cfg
		}
	}
	return fetched
}

// HandleAddProcessingService handles POST requests to append a processing service.
func (h *Handlers) HandleAddProcessingService() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)

		var req serviceRequest
		if err := c.Bind(&req); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data")
		}
		if err := req.validate(); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		cfg, err := h.projects.GetProcessingConfig(projectName)
		if err != nil {
			h.log.Errorw("reading processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to read processing config")
		}

		svc := req.toService()
		id, err := uuid.NewV4()
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate service ID")
		}
		svc.ID = id.String()

		if svc.Type == domain.ProcessingServiceTypeOGCProcesses {
			fetched, err := h.fetchRemoteProcesses(svc.URL)
			if err != nil {
				h.log.Errorw("fetching remote process descriptions", "url", svc.URL, zap.Error(err))
				return echo.NewHTTPError(http.StatusBadGateway, "Failed to fetch process descriptions from remote")
			}
			svc.Processes = mergeProcessConfigs(fetched, req.Processes)
		}

		cfg.Services = append(cfg.Services, svc)

		if err := h.projects.UpdateProcessingConfig(projectName, cfg); err != nil {
			h.log.Errorw("saving processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to save processing config")
		}
		return c.JSON(http.StatusOK, cfg)
	}
}

// HandleUpdateProcessingService handles PUT requests to update a processing service by ID.
func (h *Handlers) HandleUpdateProcessingService() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		serviceID := c.Param("id")

		var req serviceRequest
		if err := c.Bind(&req); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data")
		}
		if err := req.validate(); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		cfg, err := h.projects.GetProcessingConfig(projectName)
		if err != nil {
			h.log.Errorw("reading processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to read processing config")
		}

		idx := -1
		for i, s := range cfg.Services {
			if s.ID == serviceID {
				idx = i
				break
			}
		}
		if idx == -1 {
			return echo.NewHTTPError(http.StatusNotFound, "Service not found")
		}

		updated := req.toService()
		updated.ID = serviceID

		if updated.Type == domain.ProcessingServiceTypeOGCProcesses {
			fetched, err := h.fetchRemoteProcesses(updated.URL)
			if err != nil {
				h.log.Errorw("fetching remote process descriptions", "url", updated.URL, zap.Error(err))
				return echo.NewHTTPError(http.StatusBadGateway, "Failed to fetch process descriptions from remote")
			}
			updated.Processes = mergeProcessConfigs(fetched, req.Processes)
		}

		cfg.Services[idx] = updated

		if err := h.projects.UpdateProcessingConfig(projectName, cfg); err != nil {
			h.log.Errorw("saving processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to save processing config")
		}
		return c.JSON(http.StatusOK, cfg)
	}
}

// HandleDeleteProcessingService handles DELETE requests to remove a processing service by ID.
func (h *Handlers) HandleDeleteProcessingService() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		serviceID := c.Param("id")

		cfg, err := h.projects.GetProcessingConfig(projectName)
		if err != nil {
			h.log.Errorw("reading processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to read processing config")
		}

		idx := -1
		for i, s := range cfg.Services {
			if s.ID == serviceID {
				idx = i
				break
			}
		}
		if idx == -1 {
			return echo.NewHTTPError(http.StatusNotFound, "Service not found")
		}

		cfg.Services = append(cfg.Services[:idx], cfg.Services[idx+1:]...)

		if err := h.projects.UpdateProcessingConfig(projectName, cfg); err != nil {
			h.log.Errorw("saving processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to save processing config")
		}
		return c.JSON(http.StatusOK, cfg)
	}
}

// HandleGetProcessingConfig handles GET requests to fetch the processing services configuration.
func (h *Handlers) HandleGetProcessingConfig() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)

		cfg, err := h.projects.GetProcessingConfig(projectName)
		if err != nil {
			h.log.Errorw("reading processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to read processing config")
		}
		return c.JSON(http.StatusOK, cfg)
	}
}

// HandleLandingPage returns the OGC API Processes landing page.
func (h *Handlers) HandleLandingPage() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		base := baseURL(c, projectName)

		landing := LandingPage{
			Title:       "OGC API - Processes",
			Description: fmt.Sprintf("Processing API for project %s", projectName),
			Links: []Link{
				{Href: base, Rel: "self", Type: "application/json", Title: "This document"},
				{Href: base + "/conformance", Rel: "http://www.opengis.net/def/rel/ogc/1.0/conformance", Type: "application/json", Title: "OGC API - Processes conformance classes"},
				{Href: base + "/processes", Rel: "http://www.opengis.net/def/rel/ogc/1.0/processes", Type: "application/json", Title: "Processes"},
			},
		}
		return c.JSON(http.StatusOK, landing)
	}
}

// HandleConformance returns the conformance declaration.
func (h *Handlers) HandleConformance() echo.HandlerFunc {
	return func(c echo.Context) error {
		conf := ConformanceDeclaration{
			ConformsTo: []string{
				"http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/core",
				"http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/json",
			},
		}
		return c.JSON(http.StatusOK, conf)
	}
}

// HandleProcessList returns an aggregated list of processes from local config.
func (h *Handlers) HandleProcessList() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		base := baseURL(c, projectName)

		cfg, err := h.projects.GetProcessingConfig(projectName)
		if err != nil {
			h.log.Errorw("reading processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to read processing config")
		}

		services := ogcServices(cfg)
		var allProcesses []ProcessSummary

		for _, svc := range services {
			for processID := range svc.Processes {
				prefixedID := PrefixProcessID(svc.ID, processID)
				allProcesses = append(allProcesses, ProcessSummary{
					ID:    prefixedID,
					Title: svc.Processes[processID].Title,
					Links: []Link{
						{Href: base + "/processes/" + prefixedID, Rel: "self", Type: "application/json", Title: "Process description"},
					},
				})
			}
		}
		if allProcesses == nil {
			allProcesses = []ProcessSummary{}
		}

		result := ProcessList{
			Processes: allProcesses,
			Links: []Link{
				{Href: base + "/processes", Rel: "self", Type: "application/json"},
			},
		}
		return c.JSON(http.StatusOK, result)
	}
}

// HandleProcessDescription returns the stored description of a specific process.
func (h *Handlers) HandleProcessDescription() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		processID := c.Param("processId")

		svcID, originalID, err := ParsePrefixedID(processID)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid process ID format")
		}

		cfg, err := h.projects.GetProcessingConfig(projectName)
		if err != nil {
			h.log.Errorw("reading processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to read processing config")
		}

		services := ogcServices(cfg)
		svc, found := findOGCServiceByID(services, svcID)
		if !found {
			return echo.NewHTTPError(http.StatusNotFound, "Process not found")
		}

		procCfg, ok := svc.Processes[originalID]
		if !ok || len(procCfg.Description) == 0 {
			return echo.NewHTTPError(http.StatusNotFound, "Process description not configured")
		}

		var desc map[string]json.RawMessage
		if err := json.Unmarshal(procCfg.Description, &desc); err != nil {
			h.log.Errorw("parsing stored process description", "process", originalID, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Invalid stored process description")
		}

		prefixedID := PrefixProcessID(svcID, originalID)
		idBytes, _ := json.Marshal(prefixedID)
		desc["id"] = json.RawMessage(idBytes)

		base := baseURL(c, projectName)
		links := []Link{
			{Href: base + "/processes/" + prefixedID, Rel: "self", Type: "application/json"},
		}
		linksBytes, _ := json.Marshal(links)
		desc["links"] = json.RawMessage(linksBytes)

		return c.JSON(http.StatusOK, desc)
	}
}

// HandleExecute forwards a process execution request to the appropriate backend.
// If the process has a proxy configuration, the request is routed through the
// QGIS Server processing proxy plugin. Otherwise, it is forwarded directly.
func (h *Handlers) HandleExecute() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		processID := c.Param("processId")

		svcID, originalID, err := ParsePrefixedID(processID)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid process ID format")
		}

		cfg, err := h.projects.GetProcessingConfig(projectName)
		if err != nil {
			h.log.Errorw("reading processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to read processing config")
		}

		services := ogcServices(cfg)
		svc, found := findOGCServiceByID(services, svcID)
		if !found {
			return echo.NewHTTPError(http.StatusNotFound, "Process not found")
		}

		// Check if this process has a proxy configuration
		procCfg, hasProxyCfg := svc.Processes[originalID]
		if hasProxyCfg && h.proxySecret != "" && h.mapserverURL != "" {
			return h.executeViaProxy(c, projectName, svc, originalID, procCfg)
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Processing service is misconfigured")
	}
}

// executeViaProxy routes the execution through the QGIS Server processing proxy plugin.
func (h *Handlers) executeViaProxy(c echo.Context, projectName string, svc domain.ProcessingService, processID string, procCfg domain.ProcessConfig) error {
	// Read the client's request body as the payload
	payload, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to read request body")
	}

	// Build the remote config, defaulting execute_url if not set
	remote := procCfg.Remote
	if remote.ExecuteURL == "" {
		remote.ExecuteURL = strings.TrimRight(svc.URL, "/") + "/processes/" + processID + "/execution"
	}
	if remote.Type == "" {
		remote.Type = string(svc.Type)
	}

	// Build the proxy request
	proxyReq := &ProxyExecuteRequest{
		Auth:    h.proxySecret,
		Remote:  remote,
		Payload: payload,
	}

	// Derive the proxy URL from the mapserver URL
	proxyURL, err := h.proxyExecuteURL()
	if err != nil {
		h.log.Errorw("building proxy URL", zap.Error(err))
		return echo.NewHTTPError(http.StatusInternalServerError, "Processing proxy not configured")
	}

	result, err := h.proxy.ExecuteViaProxy(c.Request().Context(), proxyURL, proxyReq)
	if err != nil {
		h.log.Errorw("executing via proxy", "process", processID, zap.Error(err))
		return echo.NewHTTPError(http.StatusBadGateway, "Failed to execute via processing proxy")
	}

	base := baseURL(c, projectName)

	// Remap jobID to composite svcID:jobID format
	prefixedJobID := PrefixProcessID(svc.ID, result.JobID)
	result.JobID = prefixedJobID

	// Remap artifact download URLs to our own artifact endpoint
	for i, artifact := range result.Artifacts {
		if artifact.DownloadURL == "" {
			continue
		}
		var filename string
		if u, parseErr := url.Parse(artifact.DownloadURL); parseErr == nil {
			parts := strings.Split(strings.TrimRight(u.Path, "/"), "/")
			if len(parts) > 0 {
				filename = parts[len(parts)-1]
			}
		}
		if filename != "" {
			result.Artifacts[i].DownloadURL = base + "/jobs/" + prefixedJobID + "/artifacts/" + filename
		}
	}

	// Remap WMS/WFS URLs to our proxy endpoints and store originals in memory
	if result.WmsURL != "" || result.WfsURL != "" {
		h.mu.Lock()
		h.jobURLs[prefixedJobID] = jobGeoServices{
			wmsURL: result.WmsURL,
			wfsURL: result.WfsURL,
		}
		h.mu.Unlock()
		if result.WmsURL != "" {
			result.WmsURL = base + "/jobs/" + prefixedJobID + "/wms"
		}
		if result.WfsURL != "" {
			result.WfsURL = base + "/jobs/" + prefixedJobID + "/wfs"
		}
	}

	return c.JSON(http.StatusOK, result)
}

// proxyExecuteURL derives the processing proxy execute URL from the mapserver URL.
func (h *Handlers) proxyExecuteURL() (string, error) {
	parsed, err := url.Parse(h.mapserverURL)
	if err != nil {
		return "", fmt.Errorf("parsing mapserver URL: %w", err)
	}
	return fmt.Sprintf("%s://%s/qgis-server/qgis_mapserv.fcgi/ogc/processing-proxy/execute", parsed.Scheme, parsed.Host), nil
}

// proxyArtifactURL derives the processing proxy artifact URL from the mapserver URL.
func (h *Handlers) proxyArtifactURL(jobID, filename string) (string, error) {
	parsed, err := url.Parse(h.mapserverURL)
	if err != nil {
		return "", fmt.Errorf("parsing mapserver URL: %w", err)
	}
	return fmt.Sprintf("%s://%s/qgis-server/qgis_mapserv.fcgi/ogc/processing-proxy/jobs/%s/%s", parsed.Scheme, parsed.Host, jobID, filename), nil
}

// HandleArtifactDownload proxies artifact download requests to the QGIS Server processing proxy plugin.
func (h *Handlers) HandleArtifactDownload() echo.HandlerFunc {
	return func(c echo.Context) error {
		jobIDParam := c.Param("jobId")
		filename := c.Param("filename")

		// Prevent directory traversal
		if strings.Contains(jobIDParam, "..") || strings.Contains(filename, "..") ||
			strings.Contains(filename, "/") {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid path")
		}

		_, originalJobID, err := ParsePrefixedID(jobIDParam)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid job ID format")
		}

		artifactURL, err := h.proxyArtifactURL(originalJobID, filename)
		if err != nil {
			h.log.Errorw("building artifact URL", zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Processing proxy not configured")
		}

		headers := make(http.Header)
		headers.Set("Authorization", "Token "+h.proxySecret)

		resp, err := h.proxy.ForwardRequest(http.MethodGet, artifactURL, nil, headers)
		if err != nil {
			h.log.Errorw("fetching artifact", "url", artifactURL, zap.Error(err))
			return echo.NewHTTPError(http.StatusBadGateway, "Failed to fetch artifact")
		}
		defer resp.Body.Close()

		return h.proxyResponse(c, resp)
	}
}

// proxyGeoService proxies a WMS or WFS request to the internal QGIS Server URL stored for the job.
// It injects the MAP query parameter from the stored URL while passing through the client's own params.
func (h *Handlers) proxyGeoService(c echo.Context, jobID, serviceType string) error {
	if strings.Contains(jobID, "..") {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid job ID")
	}

	h.mu.RLock()
	urls, ok := h.jobURLs[jobID]
	h.mu.RUnlock()

	if !ok {
		return echo.NewHTTPError(http.StatusNotFound, "Geo service not found for this job")
	}

	var storedURL string
	switch serviceType {
	case "wms":
		storedURL = urls.wmsURL
	case "wfs":
		storedURL = urls.wfsURL
	}
	if storedURL == "" {
		return echo.NewHTTPError(http.StatusNotFound, "Service not available for this job")
	}

	parsedTarget, err := url.Parse(storedURL)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Invalid stored service URL")
	}

	// Build final query: start from client params, inject MAP from the stored URL
	finalQuery := make(url.Values)
	for k, v := range c.Request().URL.Query() {
		finalQuery[k] = v
	}
	if mapParam := parsedTarget.Query().Get("MAP"); mapParam != "" {
		finalQuery.Set("MAP", mapParam)
	}

	finalURL := fmt.Sprintf("%s://%s%s?%s", parsedTarget.Scheme, parsedTarget.Host, parsedTarget.Path, finalQuery.Encode())

	headers := make(http.Header)
	headers.Set("Authorization", "Token "+h.proxySecret)
	if ct := c.Request().Header.Get("Content-Type"); ct != "" {
		headers.Set("Content-Type", ct)
	}

	resp, err := h.proxy.ForwardRequest(c.Request().Method, finalURL, c.Request().Body, headers)
	if err != nil {
		h.log.Errorw("proxying geo service request", "url", finalURL, zap.Error(err))
		return echo.NewHTTPError(http.StatusBadGateway, "Failed to reach geo service")
	}
	defer resp.Body.Close()

	return h.proxyResponse(c, resp)
}

// HandleWMSProxy proxies WMS requests for a processing job result to the internal QGIS Server.
func (h *Handlers) HandleWMSProxy() echo.HandlerFunc {
	return func(c echo.Context) error {
		return h.proxyGeoService(c, c.Param("jobId"), "wms")
	}
}

// HandleWFSProxy proxies WFS requests for a processing job result to the internal QGIS Server.
func (h *Handlers) HandleWFSProxy() echo.HandlerFunc {
	return func(c echo.Context) error {
		return h.proxyGeoService(c, c.Param("jobId"), "wfs")
	}
}

// HandleJobStatus forwards a job status request to the appropriate backend.
func (h *Handlers) HandleJobStatus() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		jobID := c.Param("jobId")

		svc, originalJobID, err := h.resolveServiceByJobID(projectName, jobID)
		if err != nil {
			return err
		}

		backendURL := strings.TrimRight(svc.URL, "/") + "/jobs/" + originalJobID
		resp, err := h.proxy.ForwardRequest(http.MethodGet, backendURL, nil, nil)
		if err != nil {
			h.log.Errorw("forwarding job status request", "url", backendURL, zap.Error(err))
			return echo.NewHTTPError(http.StatusBadGateway, "Failed to reach processing backend")
		}
		defer resp.Body.Close()

		return h.proxyResponse(c, resp)
	}
}

// HandleJobResults forwards a job results request to the appropriate backend.
func (h *Handlers) HandleJobResults() echo.HandlerFunc {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		jobID := c.Param("jobId")

		svc, originalJobID, err := h.resolveServiceByJobID(projectName, jobID)
		if err != nil {
			return err
		}

		backendURL := strings.TrimRight(svc.URL, "/") + "/jobs/" + originalJobID + "/results"
		resp, err := h.proxy.ForwardRequest(http.MethodGet, backendURL, nil, nil)
		if err != nil {
			h.log.Errorw("forwarding job results request", "url", backendURL, zap.Error(err))
			return echo.NewHTTPError(http.StatusBadGateway, "Failed to reach processing backend")
		}
		defer resp.Body.Close()

		return h.proxyResponse(c, resp)
	}
}

// resolveServiceByJobID looks up the backend service for a prefixed job ID.
func (h *Handlers) resolveServiceByJobID(projectName, jobID string) (domain.ProcessingService, string, error) {
	svcID, originalJobID, err := ParsePrefixedID(jobID)
	if err != nil {
		return domain.ProcessingService{}, "", echo.NewHTTPError(http.StatusBadRequest, "Invalid job ID format")
	}

	cfg, err := h.projects.GetProcessingConfig(projectName)
	if err != nil {
		h.log.Errorw("reading processing config", "project", projectName, zap.Error(err))
		return domain.ProcessingService{}, "", echo.NewHTTPError(http.StatusInternalServerError, "Failed to read processing config")
	}

	services := ogcServices(cfg)
	svc, found := findOGCServiceByID(services, svcID)
	if !found {
		return domain.ProcessingService{}, "", echo.NewHTTPError(http.StatusNotFound, "Job not found")
	}

	return svc, originalJobID, nil
}

// proxyResponse writes the backend response to the client.
func (h *Handlers) proxyResponse(c echo.Context, resp *http.Response) error {
	// Copy relevant headers
	for _, header := range []string{"Content-Type", "Content-Disposition"} {
		if v := resp.Header.Get(header); v != "" {
			c.Response().Header().Set(header, v)
		}
	}

	c.Response().WriteHeader(resp.StatusCode)

	if resp.Body != nil {
		_, err := io.Copy(c.Response(), resp.Body)
		return err
	}
	return nil
}

// extractJobID extracts the job ID from a Location header URL.
// Expects a URL ending in /jobs/{jobID} or /jobs/{jobID}/...
func extractJobID(location string) string {
	parts := strings.Split(strings.TrimRight(location, "/"), "/")
	for i, p := range parts {
		if p == "jobs" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// MarshalJSON implements custom JSON marshaling for StatusInfo to include extra fields.
func (s StatusInfo) MarshalJSON() ([]byte, error) {
	type Alias StatusInfo
	data, err := json.Marshal(Alias(s))
	if err != nil {
		return nil, err
	}
	if s.Extra == nil {
		return data, nil
	}
	// Merge extra fields
	data[len(data)-1] = ','
	data = append(data, s.Extra[1:]...)
	return data, nil
}
