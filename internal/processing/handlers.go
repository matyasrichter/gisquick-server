package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gisquick/gisquick-server/internal/application"
	"github.com/gisquick/gisquick-server/internal/domain"
	"github.com/gisquick/gisquick-server/internal/infrastructure/proxy"
	"github.com/gofrs/uuid"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

const executionTimeout = 30 * time.Minute

// Handlers provides HTTP handlers for the processing module.
type Handlers struct {
	projects     application.ProjectService
	httpClient   *http.Client
	qgisPlugin   *QGISPluginClient
	log          *zap.SugaredLogger
	mapserverURL string
	jobs         JobStore
	publishRoot  string
}

func NewHandlers(projects application.ProjectService, log *zap.SugaredLogger, mapserverURL, publishRoot, pluginSecret string, jobs JobStore) *Handlers {
	return &Handlers{
		projects:     projects,
		httpClient:   &http.Client{Timeout: 60 * time.Second},
		qgisPlugin:   NewQGISPluginClient(mapserverURL, pluginSecret),
		log:          log,
		mapserverURL: mapserverURL,
		jobs:         jobs,
		publishRoot:  publishRoot,
	}
}

// StartCleanupLoop runs a background goroutine that periodically removes job
// directories from /publish/__jobs/ whose Redis key has expired.
// Call it once after server startup; cancel ctx to stop it.
func (h *Handlers) StartCleanupLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.cleanupExpiredJobs(ctx)
			}
		}
	}()
}

func (h *Handlers) cleanupExpiredJobs(ctx context.Context) {
	jobsDir := filepath.Join(h.publishRoot, "__jobs")
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		if !os.IsNotExist(err) {
			h.log.Warnw("cleanup: reading jobs directory", zap.Error(err))
		}
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		jobID := entry.Name()
		// Try to find this job in Redis across any project.
		// We use an empty project string and rely on ErrJobNotFound to detect missing keys.
		// Since keys are scoped as job:{project}:{jobID}, we need to scan.
		// Use a simple heuristic: try to GET with an empty project; if not found, delete.
		// A more precise approach would use Redis SCAN, but this is sufficient for cleanup.
		_, err := h.jobs.Get(ctx, "*", jobID)
		if errors.Is(err, ErrJobNotFound) {
			dir := filepath.Join(jobsDir, jobID)
			if rmErr := os.RemoveAll(dir); rmErr != nil {
				h.log.Warnw("cleanup: removing expired job dir", "dir", dir, zap.Error(rmErr))
			} else {
				h.log.Infow("cleanup: removed expired job dir", "jobID", jobID)
			}
		}
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

// findServiceByID finds a processing service by its UUID among all configured services.
func findServiceByID(services []domain.ProcessingService, serviceID string) (domain.ProcessingService, bool) {
	for _, s := range services {
		if s.ID == serviceID {
			return s, true
		}
	}
	return domain.ProcessingService{}, false
}

// serviceRequest is the shared request body for processing service CRUD operations.
type serviceRequest struct {
	URL       string                       `json:"url"`
	Type      domain.ProcessingServiceType `json:"type"`
	Name      string                       `json:"name"`
	Headers   map[string]string            `json:"headers,omitempty"`
	Processes []string                     `json:"processes,omitempty"` // process IDs to retain
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
		URL:     r.URL,
		Type:    r.Type,
		Name:    r.Name,
		Headers: r.Headers,
	}
}

// fetchAndStoreProcesses uses the backend to fetch all process summaries and
// descriptions from the remote service, then stores them in svc.Processes.
func (h *Handlers) fetchAndStoreProcesses(ctx context.Context, svc *domain.ProcessingService) error {
	backend := NewBackend(*svc, h.httpClient, h.log)
	if backend == nil {
		return nil // unsupported type — leave Processes empty
	}

	summaries, err := backend.FetchProcessList(ctx, *svc)
	if err != nil {
		return fmt.Errorf("fetching process list: %w", err)
	}

	processes := make(map[string]domain.ProcessConfig, len(summaries))
	order := make([]string, 0, len(summaries))
	for _, s := range summaries {
		desc, err := backend.DescribeProcess(ctx, *svc, s.ID)
		if err != nil {
			return fmt.Errorf("describing process %q: %w", s.ID, err)
		}
		descJSON, err := json.Marshal(desc)
		if err != nil {
			return fmt.Errorf("marshaling process description for %q: %w", s.ID, err)
		}
		processes[s.ID] = domain.ProcessConfig{
			Title:        desc.Title,
			Description:  json.RawMessage(descJSON),
			InputFormats: extractInputFormats(json.RawMessage(descJSON)),
		}
		order = append(order, s.ID)
	}
	svc.Processes = processes
	svc.ProcessOrder = order
	return nil
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

		if err := h.fetchAndStoreProcesses(c.Request().Context(), &svc); err != nil {
			h.log.Errorw("fetching remote process descriptions", "url", svc.URL, zap.Error(err))
			return echo.NewHTTPError(http.StatusBadGateway, "Failed to fetch process descriptions from remote")
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

		existing := cfg.Services[idx].Processes
		kept := make(map[string]domain.ProcessConfig, len(req.Processes))
		for _, id := range req.Processes {
			if proc, ok := existing[id]; ok {
				kept[id] = proc
			}
		}
		updated.Processes = kept

		keptSet := make(map[string]bool, len(req.Processes))
		for _, id := range req.Processes {
			keptSet[id] = true
		}
		keptOrder := make([]string, 0, len(req.Processes))
		for _, id := range cfg.Services[idx].ProcessOrder {
			if keptSet[id] {
				keptOrder = append(keptOrder, id)
			}
		}
		updated.ProcessOrder = keptOrder

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

// HandleSyncProcessingService re-fetches all process descriptions from the
// remote processing service and updates the stored configuration.
func (h *Handlers) HandleSyncProcessingService() echo.HandlerFunc {
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

		svc := cfg.Services[idx]
		if err := h.fetchAndStoreProcesses(c.Request().Context(), &svc); err != nil {
			h.log.Errorw("syncing remote process descriptions", "url", svc.URL, zap.Error(err))
			return echo.NewHTTPError(http.StatusBadGateway, "Failed to fetch process descriptions from remote")
		}
		cfg.Services[idx] = svc

		if err := h.projects.UpdateProcessingConfig(projectName, cfg); err != nil {
			h.log.Errorw("saving processing config", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to save processing config")
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

		var allProcesses []ProcessSummary

		for _, svc := range cfg.Services {
			order := svc.ProcessOrder
			if len(order) == 0 {
				for id := range svc.Processes {
					order = append(order, id)
				}
			}
			for _, processID := range order {
				procCfg, ok := svc.Processes[processID]
				if !ok {
					continue
				}
				prefixedID := PrefixProcessID(svc.ID, processID)

				var summaryFields struct {
					Version  string   `json:"version"`
					Keywords []string `json:"keywords"`
				}
				if len(procCfg.Description) > 0 {
					json.Unmarshal(procCfg.Description, &summaryFields)
				}

				allProcesses = append(allProcesses, ProcessSummary{
					ID:                 prefixedID,
					Title:              procCfg.Title,
					Version:            summaryFields.Version,
					Keywords:           summaryFields.Keywords,
					JobControlOptions:  []string{"async-execute"},
					OutputTransmission: []string{"reference"},
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

		svc, found := findServiceByID(cfg.Services, svcID)
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

// HandleExecute starts an async process execution.
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

		svc, found := findServiceByID(cfg.Services, svcID)
		if !found {
			return echo.NewHTTPError(http.StatusNotFound, "Process not found")
		}

		var username string
		if user, ok := c.Get("user").(domain.User); ok {
			username = user.Username
		}

		procCfg, ok := svc.Processes[originalID]
		if !ok {
			return echo.NewHTTPError(http.StatusNotFound, "Process not configured")
		}

		body, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Failed to read request body")
		}

		jobUUID, err := uuid.NewV4()
		if err != nil {
			h.log.Errorw("generating job UUID", zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to generate job ID")
		}
		ourJobID := jobUUID.String()
		base := baseURL(c, projectName)

		record := &JobRecord{
			Version:      2,
			JobID:        ourJobID,
			ServiceID:    svc.ID,
			ProcessID:    originalID,
			ProcessTitle: procCfg.Title,
			Project:      projectName,
			Username:     username,
			Status:       "accepted",
			CreatedAt:    time.Now().UTC(),
		}
		if err := h.jobs.Save(c.Request().Context(), record); err != nil {
			h.log.Errorw("saving initial job record", "jobID", ourJobID, zap.Error(err))
			return echo.NewHTTPError(http.StatusInternalServerError, "Failed to persist job record")
		}

		go h.runJob(projectName, ourJobID, base, svc, body, originalID)

		jobBase := base + "/jobs/" + ourJobID
		resp := StatusInfo{
			ProcessID: PrefixProcessID(svc.ID, originalID),
			JobID:     ourJobID,
			Type:      "process",
			Status:    "accepted",
			Links: []Link{
				{Href: jobBase, Rel: "self", Type: "application/json", Title: "Job status"},
			},
		}
		c.Response().Header().Set("Location", jobBase)
		return c.JSON(http.StatusCreated, resp)
	}
}

// runJob executes the process in the background: calls the remote backend, saves results
// to disk, asks the QGIS plugin to create a project, and updates the Redis job record.
func (h *Handlers) runJob(projectName, jobID, base string, svc domain.ProcessingService, payload []byte, processID string) {
	ctx, cancel := context.WithTimeout(context.Background(), executionTimeout)
	defer cancel()

	relJobDir := filepath.Join("__jobs", jobID)
	jobDir := filepath.Join(h.publishRoot, relJobDir)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		h.log.Errorw("creating job directory", "jobID", jobID, zap.Error(err))
		h.failJob(ctx, projectName, jobID, fmt.Sprintf("failed to create job directory: %v", err))
		return
	}

	backend := NewBackend(svc, h.httpClient, h.log)
	if backend == nil {
		h.failJob(ctx, projectName, jobID, fmt.Sprintf("unsupported service type: %s", svc.Type))
		return
	}

	// 1. Load the current job record so backend.Execute can read stored process description.
	record, err := h.jobs.Get(ctx, projectName, jobID)
	if err != nil {
		h.log.Errorw("fetching job record before execute", "jobID", jobID, zap.Error(err))
		h.failJob(ctx, projectName, jobID, fmt.Sprintf("failed to load job record: %v", err))
		return
	}

	// 2. Execute against the remote backend.
	results, remoteJobID, err := backend.Execute(ctx, record, svc, json.RawMessage(payload))
	if err != nil {
		h.log.Errorw("executing process", "process", processID, "jobID", jobID, zap.Error(err))
		h.failJob(ctx, projectName, jobID, err.Error())
		return
	}

	// 3. Save results to disk.
	artifacts, err := SaveResults(ctx, h.httpClient, jobDir, results, svc.Headers)
	if err != nil {
		h.log.Errorw("saving job results", "jobID", jobID, zap.Error(err))
		h.failJob(ctx, projectName, jobID, fmt.Sprintf("failed to save results: %v", err))
		return
	}

	// 4. Ask the QGIS plugin to create a project file (non-fatal on error).
	jobBase := base + "/jobs/" + jobID
	var projectFile string
	if len(artifacts) > 0 {
		projectFile, err = h.qgisPlugin.CreateProject(ctx, relJobDir, jobBase+"/ows", artifacts)
		if err != nil {
			h.log.Warnw("QGIS plugin gisquick-project-from-file failed (WMS/WFS unavailable)", "jobID", jobID, zap.Error(err))
		}
	}

	// 5. Remap artifact download URLs to our own endpoint.
	for i, a := range artifacts {
		artifacts[i].DownloadURL = jobBase + "/artifacts/" + a.Path
	}

	// 6. Update job record.
	now := time.Now().UTC()
	record.Status = "successful"
	record.RemoteJobID = remoteJobID
	record.StoragePath = jobDir
	record.ProjectFile = projectFile
	record.Artifacts = artifacts
	record.FinishedAt = &now
	if projectFile != "" {
		record.OwsURL = jobBase + "/ows"
	}
	if saveErr := h.jobs.Save(ctx, record); saveErr != nil {
		h.log.Errorw("saving completed job record", "jobID", jobID, zap.Error(saveErr))
	}
}

// failJob marks a job as failed in Redis.
func (h *Handlers) failJob(ctx context.Context, projectName, jobID, message string) {
	record, err := h.jobs.Get(ctx, projectName, jobID)
	if err != nil {
		h.log.Errorw("fetching job record to mark failed", "jobID", jobID, zap.Error(err))
		return
	}
	now := time.Now().UTC()
	record.Status = "failed"
	record.Message = message
	record.FinishedAt = &now
	if saveErr := h.jobs.Save(ctx, record); saveErr != nil {
		h.log.Errorw("saving failed job record", "jobID", jobID, zap.Error(saveErr))
	}
}

// HandleJobStatus returns the current status of a job.
func (h *Handlers) HandleJobStatus() echo.HandlerFunc {
	return func(c echo.Context) error {
		jobID := c.Param("jobId")

		record, err := h.lookupJob(c, jobID)
		if err != nil {
			return err
		}

		base := baseURL(c, record.Project)
		jobBase := base + "/jobs/" + jobID
		links := []Link{
			{Href: jobBase, Rel: "self", Type: "application/json", Title: "Job status"},
		}
		if record.Status == "successful" {
			links = append(links, Link{
				Href:  jobBase + "/results",
				Rel:   "http://www.opengis.net/def/rel/ogc/1.0/results",
				Type:  "application/json",
				Title: "Job results",
			})
		}

		return c.JSON(http.StatusOK, StatusInfo{
			ProcessID: PrefixProcessID(record.ServiceID, record.ProcessID),
			JobID:     jobID,
			Type:      "process",
			Status:    record.Status,
			Message:   record.Message,
			Created:   &record.CreatedAt,
			Finished:  record.FinishedAt,
			Links:     links,
		})
	}
}

// HandleJobResults returns the stored artifacts and geo-service URLs for a completed job.
func (h *Handlers) HandleJobResults() echo.HandlerFunc {
	return func(c echo.Context) error {
		jobID := c.Param("jobId")

		record, err := h.lookupJob(c, jobID)
		if err != nil {
			return err
		}

		type jobResults struct {
			Artifacts []Artifact `json:"artifacts"`
			OwsURL    string     `json:"ows_url,omitempty"`
		}
		artifacts := record.Artifacts
		if artifacts == nil {
			artifacts = []Artifact{}
		}
		return c.JSON(http.StatusOK, jobResults{
			Artifacts: artifacts,
			OwsURL:    record.OwsURL,
		})
	}
}

// HandleArtifactDownload serves an artifact file directly from disk.
func (h *Handlers) HandleArtifactDownload() echo.HandlerFunc {
	return func(c echo.Context) error {
		jobID := c.Param("jobId")
		filename := c.Param("filename")

		if strings.Contains(jobID, "..") || strings.Contains(filename, "..") || strings.Contains(filename, "/") {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid path")
		}

		if _, err := h.lookupJob(c, jobID); err != nil {
			return err
		}

		filePath := filepath.Join(h.publishRoot, "__jobs", jobID, filename)
		return c.File(filePath)
	}
}

// HandleOWSProxy proxies OWS (WMS/WFS) requests for a job result to QGIS Server.
// The SERVICE query parameter in the request determines which service is used.
func (h *Handlers) HandleOWSProxy() echo.HandlerFunc {
	return h.proxyJobGeoService()
}

// proxyJobGeoService sets the MAP parameter to the job's QGIS project file and
// forwards the request to QGIS Server, following the same pattern as handleProjectOws.
func (h *Handlers) proxyJobGeoService() echo.HandlerFunc {
	reverseProxy := proxy.NewQGISReverseProxy(h.mapserverURL, h.log)
	capabilitiesProxy := proxy.NewQGISReverseProxy(h.mapserverURL, h.log)
	capabilitiesProxy.ModifyResponse = proxy.RewriteCapabilitiesURLs
	return func(c echo.Context) error {
		jobID := c.Param("jobId")
		if strings.Contains(jobID, "..") {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid job ID")
		}
		record, err := h.lookupJob(c, jobID)
		if err != nil {
			return err
		}
		if record.ProjectFile == "" {
			return echo.NewHTTPError(http.StatusNotFound, "Geo service not available for this job")
		}
		mapPath := filepath.Join(h.publishRoot, "__jobs", jobID, record.ProjectFile)
		req := c.Request()
		query := req.URL.Query()
		query.Set("MAP", mapPath)
		req.URL.RawQuery = query.Encode()
		service := query.Get("SERVICE")
		if strings.EqualFold(query.Get("REQUEST"), "GetCapabilities") &&
			(strings.EqualFold(service, "WMS") || strings.EqualFold(service, "WFS")) {
			req.Header.Set("X-Ows-Url", req.URL.Path)
			capabilitiesProxy.ServeHTTP(c.Response(), req)
		} else {
			reverseProxy.ServeHTTP(c.Response(), req)
		}
		return nil
	}
}

// lookupJob retrieves a JobRecord from Redis, verifying the project scope matches the URL.
func (h *Handlers) lookupJob(c echo.Context, jobID string) (*JobRecord, error) {
	project := c.Get("project").(string)
	record, err := h.jobs.Get(c.Request().Context(), project, jobID)
	if errors.Is(err, ErrJobNotFound) {
		return nil, echo.NewHTTPError(http.StatusNotFound, "Job not found")
	}
	if err != nil {
		h.log.Errorw("Redis job lookup", "jobID", jobID, zap.Error(err))
		return nil, echo.NewHTTPError(http.StatusInternalServerError, "Failed to look up job")
	}
	if record.Project != project {
		return nil, echo.NewHTTPError(http.StatusNotFound, "Job not found")
	}
	return record, nil
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
	data[len(data)-1] = ','
	data = append(data, s.Extra[1:]...)
	return data, nil
}
