package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gisquick/gisquick-server/internal/domain"
	"github.com/gisquick/gisquick-server/internal/processing"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

func (s *Server) AddRoutes(e *echo.Echo) {

	LoginRequired := s.middlewares.LoginRequired
	SuperuserRequired := s.middlewares.SuperuserRequired
	ProjectAdminAccess := s.middlewares.ProjectAdminAccess
	ProjectSuperuserAccess := s.middlewares.ProjectSuperuserAccess
	ProjectAccess := s.middlewares.ProjectAccess
	ProjectAccessOWS := s.middlewares.ProjectAccessOWS

	e.POST("/api/auth/login", s.handleLogin())
	e.POST("/api/auth/logout", s.handleLogout)
	e.GET("/api/auth/logout", s.handleLogout) // Just for compatibility!!!

	e.GET("/api/users", s.handleGetUsers, LoginRequired)

	e.GET("/api/admin/config", s.handleAdminConfig, SuperuserRequired)
	e.GET("/api/admin/users", s.handleGetAllUsers, SuperuserRequired)
	e.GET("/api/admin/users/:user", s.handleGetUser, SuperuserRequired)
	e.PUT("/api/admin/users/:user", s.handleUpdateUser(), SuperuserRequired)
	e.PUT("/api/admin/users/profile/:user", s.handleAdminUpdateUserProfile, SuperuserRequired)
	e.DELETE("/api/admin/users/:user", s.handleDeleteUser, SuperuserRequired)
	e.POST("/api/admin/user", s.handleCreateUser(), SuperuserRequired)
	e.POST("/api/admin/email_preview", s.handleGetEmailPreview(), SuperuserRequired)
	e.POST("/api/admin/email", s.handleSendEmail(), SuperuserRequired)
	e.POST("/api/admin/send_activation_email", s.handleSendActivationEmail(), SuperuserRequired)
	e.GET("/api/admin/notifications", s.handleGetNotifications, SuperuserRequired)
	e.POST("/api/admin/notification", s.handleSaveNotification, SuperuserRequired)
	e.DELETE("/api/admin/notification/:id", s.handleDeleteNotification, SuperuserRequired)

	if s.Config.SignupAPI {
		e.POST("/api/accounts/signup", s.handleSignUp())
		e.POST("/api/accounts/invite", s.handleInvitation(), SuperuserRequired)
		e.POST("/api/accounts/activate", s.handleActivateAccount())
	}
	e.GET("/api/accounts/check", s.handleCheckAvailability())
	e.POST("/api/accounts/password_reset", s.handlePasswordReset())
	e.POST("/api/accounts/new_password", s.handleNewPassword())
	e.POST("/api/accounts/change_password", s.handleChangePassword(), LoginRequired)
	e.GET("/api/account", s.handleGetAccountInfo(), LoginRequired)
	e.PUT("/api/account/profile", s.handleUpdateAccountProfile, LoginRequired)

	e.GET("/api/auth/user", s.handleGetSessionUser)
	e.GET("/api/auth/is_authenticated", s.handleGetSessionUser, LoginRequired)
	e.GET("/api/auth/is_superuser", s.handleGetSessionUser, SuperuserRequired)

	e.GET("/api/app", s.handleAppInit())

	// e.POST("/api/map/project/*", s.handleUpdateProject)

	e.POST("/api/project/:user/:name", s.handleCreateProject(), LoginRequired)
	e.DELETE("/api/project/:user/:name", s.handleDeleteProject, ProjectSuperuserAccess)
	e.GET("/api/projects", s.handleGetProjects())
	e.GET("/api/projects/:user", s.handleGetUserProjects, SuperuserRequired)
	e.POST("/api/project/upload/:user/:name", s.handleUpload(), ProjectAdminAccess)

	e.GET("/api/project/ows/:user/:name", s.handleProjectOws(), ProjectAdminAccess)
	e.POST("/api/project/ows/:user/:name", s.handleProjectOws(), ProjectAdminAccess)
	e.GET("/api/project/files/:user/:name", s.handleGetProjectFiles(), ProjectAdminAccess)
	e.DELETE("/api/project/files/:user/:name", s.handleDeleteProjectFiles(), ProjectAdminAccess)
	e.GET("/api/project/info/:user/:name", s.handleGetProjectInfo, ProjectAdminAccess)
	e.GET("/api/project/full-info/:user/:name", s.handleGetProjectFullInfo(), ProjectAdminAccess)

	e.GET("/api/project/media/:user/:name/*", s.mediaFileHandler("/tmp/thumbnails"), ProjectAccess)
	e.GET("/api/project/media/:user/:name/web/app/*", s.appMediaFileHandler)
	e.POST("/api/project/media/:user/:name/*", s.handleUploadMediaFile, ProjectAccess)
	e.DELETE("/api/project/media/:user/:name/*", s.handleDeleteMediaFile, ProjectAccess)
	e.POST("/api/project/script/:user/:name", s.handleScriptUpload(), ProjectAdminAccess)
	e.DELETE("/api/project/script/:user/:name", s.handleDeleteScript(), ProjectAdminAccess)

	e.GET("/api/project/file/:user/:name/*", s.handleProjectFile, ProjectAdminAccess)
	e.GET("/api/project/download/:user/:name", s.handleDownloadProjectFiles, ProjectAdminAccess)
	e.GET("/api/project/download/:user/:name/*", s.handleDownloadProjectFiles, ProjectAdminAccess)
	e.GET("/api/project/inline/:user/:name/*", s.handleInlineProjectFile, ProjectAdminAccess)

	e.POST("/api/project/meta/:user/:name", s.handleUpdateProjectMeta(), ProjectAdminAccess)

	e.POST("/api/project/settings/:user/:name", s.handleSaveProjectSettings, ProjectAdminAccess)
	e.POST("/api/project/thumbnail/:user/:name", s.handleUploadThumbnail, ProjectAdminAccess)
	e.GET("/api/project/thumbnail/:user/:name", s.handleGetThumbnail)
	e.GET("/api/map/project/:user/:name", s.handleGetProject(), MiddlewareErrorHandler(ProjectAccess, func(e error, c echo.Context) error {
		if he, ok := e.(*echo.HTTPError); ok {
			if he.Code == 401 {
				projectName := c.Get("project").(string)
				pInfo, err := s.projects.GetProjectInfo(projectName)
				if err != nil {
					if errors.Is(err, domain.ErrProjectNotExists) {
						return echo.NewHTTPError(http.StatusBadRequest, "Project does not exists")
					}
					s.log.Errorw("reading project info", zap.Error(err))
				}
				type app struct {
					App    json.RawMessage `json:"app"`
					Status int             `json:"status"`
					Name   string          `json:"name"`
					Title  string          `json:"title"`
				}
				data := app{
					Name:   projectName,
					Title:  pInfo.Title,
					Status: he.Code,
				}
				if s.Config.ProjectCustomization {
					cfg, err := s.projects.GetProjectCustomizations(projectName)
					if err != nil {
						s.log.Errorw("reading project customization config", zap.Error(err))
					} else if cfg != nil {
						data.App = cfg
					}
				}
				return c.JSON(he.Code, data)
			}
		}
		return e
	}))

	owsHandler := s.handleMapOws()
	e.GET("/api/map/ows/:user/:name", owsHandler, ProjectAccessOWS)
	e.POST("/api/map/ows/:user/:name", owsHandler, ProjectAccessOWS)
	e.GET("/api/map/capabilities/:user/:name", s.handleGetLayerCapabilities(), ProjectAccess)
	e.GET("/api/map/search/:user/:name/*", s.handleSearch(), ProjectAccess)

	e.POST("/api/project/reload/:user/:name", s.handleProjectReload, ProjectAdminAccess)

	e.GET("/ws/app", s.handleWebAppWS, LoginRequired)
	e.GET("/ws/plugin", s.handlePluginWS, LoginRequired)

	if s.Config.PluginsURL != "" {
		// e.GET("/plugins/", s.pythonPluginRepoHandler("/qgis-plugins-repo"))
		e.GET("/plugins/platform/:platform", s.platformPluginRepoHandler("/qgis-plugins-repo"))
		e.GET("/plugins/download/*", s.handleDownloadPlugin("/qgis-plugins-repo"))
	}

	// Processing module
	procHandlers := processing.NewHandlers(s.projects, s.log, s.Config.MapserverURL, s.Config.ProcessingProxySecret)
	e.POST("/api/project/processing/:user/:name", procHandlers.HandleAddProcessingService(), ProjectAdminAccess)
	e.PUT("/api/project/processing/:user/:name/:id", procHandlers.HandleUpdateProcessingService(), ProjectAdminAccess)
	e.DELETE("/api/project/processing/:user/:name/:id", procHandlers.HandleDeleteProcessingService(), ProjectAdminAccess)
	e.GET("/api/project/processing/:user/:name", procHandlers.HandleGetProcessingConfig(), ProjectAdminAccess)

	// OGC API - Processes proxy endpoints
	e.GET("/api/map/ogc-processes/:user/:name", procHandlers.HandleLandingPage(), ProjectAccess)
	e.GET("/api/map/ogc-processes/:user/:name/conformance", procHandlers.HandleConformance(), ProjectAccess)
	e.GET("/api/map/ogc-processes/:user/:name/processes", procHandlers.HandleProcessList(), ProjectAccess)
	e.GET("/api/map/ogc-processes/:user/:name/processes/:processId", procHandlers.HandleProcessDescription(), ProjectAccess)
	e.POST("/api/map/ogc-processes/:user/:name/processes/:processId/execution", procHandlers.HandleExecute(), ProjectAccess)
	e.GET("/api/map/ogc-processes/:user/:name/jobs/:jobId", procHandlers.HandleJobStatus(), ProjectAccess)
	e.GET("/api/map/ogc-processes/:user/:name/jobs/:jobId/results", procHandlers.HandleJobResults(), ProjectAccess)
	e.GET("/api/map/ogc-processes/:user/:name/jobs/:jobId/artifacts/:filename", procHandlers.HandleArtifactDownload(), ProjectAccess)
	e.Any("/api/map/ogc-processes/:user/:name/jobs/:jobId/wms", procHandlers.HandleWMSProxy(), ProjectAccess)
	e.Any("/api/map/ogc-processes/:user/:name/jobs/:jobId/wfs", procHandlers.HandleWFSProxy(), ProjectAccess)

	// owsHandler := s.owsHandler()
	// e.GET("/api/map/ows", owsHandler)
	// e.POST("/api/map/ows", owsHandler)

	// // Mapcache
	// e.GET("/api/map/tile/:project_hash/tile/:layers_hash/:z/:x/:y", s.handleMapcacheTile())
	// e.GET("/api/map/tile/:project_hash/legend/:layers_hash/:filename", s.handleMapcacheLegend())
}
