package server

import (
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/disintegration/imaging"
	"github.com/gisquick/gisquick-server/internal/application"
	"github.com/gisquick/gisquick-server/internal/domain"
	"github.com/gisquick/gisquick-server/internal/infrastructure/proxy"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
	_ "golang.org/x/image/webp"
	"golang.org/x/sync/singleflight"
)

const MB int64 = 1024 * 1024

var MaxJSONSize int64 = 2 * MB
var MaxScriptSize int64 = 5 * MB

func (s *Server) handleGetProjectFiles() func(echo.Context) error {
	type ProjectFiles struct {
		Files          []domain.ProjectFile `json:"files"`
		TemporaryFiles []domain.ProjectFile `json:"temporary"`
	}
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		files, tmpFiles, err := s.projects.ListProjectFiles(projectName, true)
		if err != nil {
			if errors.Is(err, domain.ErrProjectNotExists) {
				return echo.NewHTTPError(http.StatusBadRequest, "Project does not exists")
			}
			return fmt.Errorf("handleGetProjectFiles: %w", err)
		}
		return c.JSON(http.StatusOK, ProjectFiles{files, tmpFiles})
	}
}

type UserDashboard struct {
	Projects []string `json:"projects"`
}

func (s *Server) handleGetProjects() func(echo.Context) error {
	type QueryParams struct {
		Projects string `query:"projects"`
		Filter   string `query:"filter"`
	}
	return func(c echo.Context) error {
		var user domain.User
		var projectsNames []string
		queryParams := new(QueryParams)
		if err := (&echo.DefaultBinder{}).BindQueryParams(c, queryParams); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid query parameters")
		}
		if queryParams.Projects != "" {
			projectsNames = strings.Split(queryParams.Projects, ",")
		} else {
			var err error
			user, err = s.auth.GetUser(c)
			if err != nil {
				return err
			}
			if !user.IsAuthenticated {
				return echo.ErrUnauthorized
			}
			dashboardPath := filepath.Join(s.Config.ProjectsRoot, user.Username, "dashboard.json")
			content, err := os.ReadFile(dashboardPath)
			if err == nil {
				var data UserDashboard
				if err = json.Unmarshal(content, &data); err != nil {
					s.log.Warnw("reading user dashboard file", "user", user.Username, zap.Error(err))
				} else {
					projectsNames = data.Projects
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				s.log.Warnw("reading user dashboard file", "user", user.Username, zap.Error(err))
			}
		}
		if len(projectsNames) > 0 {
			data := make([]domain.ProjectInfo, 0, len(projectsNames))
			for _, name := range projectsNames {
				p, err := s.projects.GetProjectInfo(strings.TrimSpace(name))
				if err == nil {
					data = append(data, p)
				}
			}
			return c.JSON(http.StatusOK, data)
		}
		if strings.EqualFold(queryParams.Filter, "accessible") {
			data, err := s.projects.AccessibleProjects(user.Username, true)
			if err != nil {
				return fmt.Errorf("getting list of user accessible projects: %w", err)
			}
			return c.JSON(http.StatusOK, data)
		}
		data, err := s.projects.GetUserProjects(user.Username)
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, data)
	}
}

func (s *Server) handleGetUserProjects(c echo.Context) error {
	username := c.Param("user")
	data, err := s.projects.GetUserProjects(username)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, data)
}

func (s *Server) handleDeleteProject(c echo.Context) error {
	projectName := c.Get("project").(string)
	if err := s.projects.Delete(projectName); err != nil {
		if errors.Is(err, domain.ErrProjectNotExists) {
			return echo.NewHTTPError(http.StatusBadRequest, "Project does not exists")
		}
		return err
	}
	return c.NoContent(http.StatusOK)
}

// ProgressReader export
type ProgressReader struct {
	Reader   io.ReadCloser
	Callback func(int, int)
	Step     int
	Progress int
	lastVal  int
}

func (r *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.Progress += n
	delta := r.Progress - r.lastVal
	if delta >= r.Step || err == io.EOF {
		r.Callback(r.Progress, delta)
		r.lastVal = r.Progress
	}
	return
}

func (r *ProgressReader) Close() error {
	return r.Reader.Close()
}

func percProgress(size, total int) int {
	if total == 0 {
		return 100
	}
	return int(100 * (float64(size) / float64(total)))
}

func (s *Server) handleUpload() func(echo.Context) error {
	type fileUploadProgress struct {
		Files         map[string]int `json:"files"`
		TotalProgress int            `json:"total"`
	}
	type uploadInfo struct {
		Files []domain.ProjectFile `json:"files"`
	}

	return func(c echo.Context) error {
		req := c.Request()
		ctype, params, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
		if err != nil || ctype != "multipart/form-data" {
			return echo.NewHTTPError(http.StatusBadRequest, http.ErrNotMultipart.Error())
		}
		boundary, ok := params["boundary"]
		if !ok {
			return echo.NewHTTPError(http.StatusBadRequest, http.ErrMissingBoundary.Error())
		}
		user, err := s.auth.GetUser(c)
		if err != nil {
			return err
		}
		if s.Config.MaxProjectSize > 0 {
			req.Body = http.MaxBytesReader(c.Response(), req.Body, s.Config.MaxProjectSize)
		}
		reader := multipart.NewReader(req.Body, boundary)
		projectName := c.Get("project").(string)

		// first part should contain upload info
		var info uploadInfo
		part, err := reader.NextPart()
		if err != nil {
			s.log.Errorw("uploading files", "project", projectName, zap.Error(err))
			return err
		}
		err = json.NewDecoder(part).Decode(&info)
		if err != nil {
			s.log.Errorw("decoding upload metadata", "project", projectName, zap.Error(err))
			return err
		}

		totalSize := int64(0)
		uploadSizeMap := make(map[string]int, len(info.Files))
		for _, f := range info.Files {
			uploadSizeMap[f.Path] = int(f.Size)
			totalSize += f.Size
		}
		// Ver. 1
		uploadedSize := 0
		uploadProgress := make(map[string]int)
		lastNotification := time.Now()
		nextFile := func() (string, io.ReadCloser, error) { // or ReadCloser?
			part, err := reader.NextPart()
			if err != nil {
				return "", nil, err
			}
			var partReader io.ReadCloser = part
			if strings.HasSuffix(part.FileName(), ".gz") && !strings.HasSuffix(part.FormName(), ".gz") {
				partReader, _ = gzip.NewReader(part)
			}
			pr := &ProgressReader{Reader: partReader, Step: 32 * 1024, Callback: func(uploaded, last int) {
				uploadProgress[part.FormName()] = percProgress(uploaded, uploadSizeMap[part.FormName()])
				uploadedSize += last
				now := time.Now()
				if now.Sub(lastNotification).Seconds() > 0.5 {

					totalProgress := percProgress(uploadedSize, int(totalSize))
					s.log.Infow("upload progress", "file", part.FormName(), "uploaded", uploaded, "delta", last, "totalUploaded", uploadedSize, "totalSize", totalSize, "totalProgress", totalProgress)
					s.sws.AppChannel().Send(user.Username, "UploadProgress", fileUploadProgress{uploadProgress, totalProgress})

					lastNotification = now
					uploadProgress = make(map[string]int)
				}
			}}
			return part.FormName(), pr, nil
		}
		changes := domain.FilesChanges{Updates: info.Files}
		if _, err := s.projects.UpdateFiles(projectName, changes, nextFile); err != nil {
			// better check in future release https://github.com/golang/go/issues/30715
			if errors.Is(err, application.ErrAccountStorageLimit) {
				return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "Reached account storage limit")
			}
			if errors.Is(err, application.ErrProjectSizeLimit) || err.Error() == "http: request body too large" {
				// s.log.Warn("uploading files: max limit reached")
				return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "Reached project size limit.")
			}
			return err
		}
		// finish reading from stream
		if _, err := reader.NextPart(); err != io.EOF {
			s.log.Warnf("expected end of stream", "project", projectName)
		}
		s.sws.AppChannel().Send(user.Username, "UploadProgress", fileUploadProgress{uploadProgress, 100})

		// Ver. 2
		/*
			uploadProgress := make(map[string]int)
			lastNotification := time.Now()
			for {
				part, err := reader.NextPart()
				if err == io.EOF {
					s.sws.AppChannel().Send(user.Username, "UploadProgress", uploadProgress)
					// if appWs :=  appsWs.Get(username); appWs != nil {
					// 	s.sendJSONMessage(appWs, "UploadProgress", uploadProgress)
					// }
					break
				}
				var partReader io.ReadCloser = part
				if strings.HasSuffix(part.FileName(), ".gz") && !strings.HasSuffix(part.FormName(), ".gz") {
					partReader, _ = gzip.NewReader(part)
				}
				pr := &ProgressReader{Reader: partReader, Step: 32 * 1024, Callback: func(p int) {
					uploadProgress[part.FormName()] = p
					now := time.Now()
					if now.Sub(lastNotification).Seconds() > 0.5 {
						// if appWs := s.appsWs.Get(username); appWs != nil {
						// 	s.sendJSONMessage(appWs, "UploadProgress", uploadProgress)
						// }
						s.sws.AppChannel().Send(user.Username, "UploadProgress", uploadProgress)
						lastNotification = now
						uploadProgress = make(map[string]int)
					}
				}}
				s.projects.SaveFile(projectName, part.FormName(), pr)
				partReader.Close()
				if err != nil {
					return err
				}
			}
		*/
		return c.NoContent(http.StatusOK)
	}
}

func (s *Server) handleDeleteProjectFiles() func(echo.Context) error {
	type FilesInfo struct {
		Files []string `json:"files"`
	}

	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		var data FilesInfo
		if err := (&echo.DefaultBinder{}).BindBody(c, &data); err != nil {
			return err
		}
		if len(data.Files) < 1 {
			return echo.NewHTTPError(http.StatusBadRequest, "No files specified")
		}
		changes := domain.FilesChanges{Removes: data.Files}
		// nextFile := func() (string, io.ReadCloser, error) {
		// 	return "", nil, io.EOF
		// }
		files, err := s.projects.UpdateFiles(projectName, changes, nil)
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, files)
	}
}

func (s *Server) handleProjectOws() func(echo.Context) error {
	reverseProxy := proxy.NewQGISReverseProxy(s.Config.MapserverURL, s.log)
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)

		p, err := s.projects.GetProjectInfo(projectName)
		if err != nil {
			if errors.Is(err, domain.ErrProjectNotExists) {
				return echo.NewHTTPError(http.StatusBadRequest, "Project does not exists")
			}
			return err
		}
		// TODO: hardcoded /publish/ directory!
		owsProject := filepath.Join("/publish/", projectName, p.QgisFile)
		s.log.Infow("GetMap", "ows_project", owsProject)
		query := c.Request().URL.Query()
		query.Set("MAP", owsProject)
		c.Request().URL.RawQuery = query.Encode()

		reverseProxy.ServeHTTP(c.Response(), c.Request())
		return nil
	}
}

func (s *Server) handleCreateProject() func(echo.Context) error {
	return func(c echo.Context) error {
		// TODO: check project folder/index file doesn't exists
		req := c.Request()
		req.Body = http.MaxBytesReader(c.Response(), req.Body, MaxJSONSize)
		defer req.Body.Close()

		var data json.RawMessage
		d := json.NewDecoder(req.Body)
		if err := d.Decode(&data); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data:", err.Error())
		}
		username := c.Param("user")
		name := c.Param("name")
		projName := filepath.Join(username, name)
		info, err := s.projects.Create(projName, data)
		if err != nil {
			if errors.Is(err, domain.ErrProjectAlreadyExists) {
				return echo.NewHTTPError(http.StatusConflict, "Project already exists")
			}
			if errors.Is(err, application.ErrAccountProjectsLimit) {
				return echo.NewHTTPError(http.StatusConflict, "Projects limit was reached")
			}
			return err
		}
		s.log.Infow("Created project", "info", info)
		return c.JSON(http.StatusOK, info)
	}
}

func (s *Server) handleGetProjectFullInfo() func(echo.Context) error {
	type FullInfo struct {
		Auth       string          `json:"authentication"`
		Name       string          `json:"name"`
		Title      string          `json:"title"`
		Created    time.Time       `json:"created"`
		LastUpdate time.Time       `json:"last_update"`
		State      string          `json:"state"`
		Size       int64           `json:"size"`
		Thumbnail  bool            `json:"thumbnail"`
		Meta       domain.QgisMeta `json:"meta"`
		// Meta     json.RawMessage         `json:"meta"`
		Settings *domain.ProjectSettings `json:"settings"`
		Scripts  domain.Scripts          `json:"scripts"`
	}
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		info, err := s.projects.GetProjectInfo(projectName)
		if err != nil {
			if errors.Is(err, domain.ErrProjectNotExists) {
				return echo.NewHTTPError(http.StatusBadRequest, "Project does not exists")
			}
			return fmt.Errorf("[handleGetProjectInfo] loading project info: %w", err)
		}
		// var meta json.RawMessage
		var meta domain.QgisMeta
		if err := s.projects.GetQgisMetadata(projectName, &meta); err != nil {
			return fmt.Errorf("[handleGetProjectInfo] loading qgis meta: %w", err)
		}
		// meta, err := s.projects.GetQgisMetadata(projectName)
		// if err != nil {
		// 	return fmt.Errorf("[handleGetProjectInfo] loading qgis meta: %w", err)
		// }
		data := FullInfo{
			Auth:       info.Authentication,
			Name:       projectName,
			Title:      info.Title,
			Created:    info.Created,
			LastUpdate: info.LastUpdate,
			State:      info.State,
			Size:       info.Size,
			Thumbnail:  info.Thumbnail,
			Meta:       meta,
		}
		if info.State != "empty" {
			settings, err := s.projects.GetSettings(projectName)
			if err == nil {
				data.Settings = &settings
			} else {
				s.log.Warnw("[handleGetProjectInfo] settings not found", "project", projectName, zap.Error(err))
			}
		}
		scripts, err := s.projects.GetScripts(projectName)
		if err != nil {
			s.log.Errorw("[handleGetProjectInfo] loading scripts", "project", projectName)
		} else {
			data.Scripts = scripts
		}
		return c.JSON(http.StatusOK, data)
	}
}

func (s *Server) handleGetProjectInfo(c echo.Context) error {
	projectName := c.Get("project").(string)
	info, err := s.projects.GetProjectInfo(projectName)
	if err != nil {
		if errors.Is(err, domain.ErrProjectNotExists) {
			return echo.NewHTTPError(http.StatusBadRequest, "Project does not exists")
		}
		return fmt.Errorf("handleGetProjectInfo: %w", err)
	}
	return c.JSON(http.StatusOK, info)
}

func (s *Server) handleUpdateProjectMeta() func(echo.Context) error {
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		req := c.Request()
		req.Body = http.MaxBytesReader(c.Response(), req.Body, MaxJSONSize)
		defer req.Body.Close()

		var data json.RawMessage
		d := json.NewDecoder(req.Body)
		if err := d.Decode(&data); err != nil {
			s.log.Errorw("decoding project metadata", "project", projectName, zap.Error(err))
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data:", err.Error())
		}

		err := s.projects.UpdateMeta(projectName, data)
		if err != nil {
			if errors.Is(err, domain.ErrProjectNotExists) {
				return echo.NewHTTPError(http.StatusConflict, "Project does not exists")
			}
			return err
		}
		return c.NoContent(http.StatusOK)
	}
}

/* Settings Handlers */

func (s *Server) handleSaveProjectSettings(c echo.Context) error {
	projectName := c.Get("project").(string)
	req := c.Request()
	req.Body = http.MaxBytesReader(c.Response(), req.Body, MaxJSONSize)
	defer req.Body.Close()

	var data json.RawMessage
	d := json.NewDecoder(req.Body)
	if err := d.Decode(&data); err != nil {
		s.log.Errorw("decoding project settings", "project", projectName, zap.Error(err))
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data:", err.Error())
	}
	return s.projects.UpdateSettings(projectName, data)
}

func (s *Server) handleUploadThumbnail(c echo.Context) error {
	if err := c.Request().ParseForm(); err != nil {
		return err
	}
	f, h, err := c.Request().FormFile("image")
	if err != nil {
		return err
	}
	defer f.Close()
	projectName := c.Get("project").(string)
	s.log.Infow("thumbnail", "project", projectName, "image", h.Filename)
	if err := s.projects.SaveThumbnail(projectName, f); err != nil {
		return err
	}
	return c.NoContent(http.StatusOK)
}

func (s *Server) handleGetThumbnail(c echo.Context) error {
	username := c.Param("user")
	name := c.Param("name")
	projectName := filepath.Join(username, name)
	return c.File(s.projects.GetThumbnailPath(projectName))
}

func (s *Server) handleScriptUpload() func(echo.Context) error {
	type Data struct {
		domain.ScriptModule
		Module string `json:"module"`
	}
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)

		req := c.Request()
		req.Body = http.MaxBytesReader(c.Response(), req.Body, MaxJSONSize)
		if err := req.ParseMultipartForm(2 * MB); err != nil {
			return err
		}
		var info Data
		jsonInfo := c.FormValue("info")
		if err := json.Unmarshal([]byte(jsonInfo), &info); err != nil {
			s.log.Errorw("[handleScriptUpload] parsing metadata", zap.Error(err))
			return echo.ErrBadRequest
		}
		if info.Module == "" || len(info.Components) < 1 { // TODO: better name validation with regex
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid script module data")
		}
		var filesMeta []domain.ProjectFile = []domain.ProjectFile{}
		var files []*multipart.FileHeader = []*multipart.FileHeader{}

		form, err := c.MultipartForm()
		if err != nil {
			return err
		}
		for n, f := range form.File {
			s.log.Infow("[handleScriptUpload]", "file", n, "len", len(f))
			for _, fh := range f {
				path := filepath.Join("web", "components", fh.Filename)
				filesMeta = append(filesMeta, domain.ProjectFile{Path: path, Size: fh.Size})
				files = append(files, fh)
			}
		}
		changes := domain.FilesChanges{Updates: filesMeta}
		s.log.Infow("[handleScriptUpload]", "info", info, "changes", changes)

		findex := 0
		nextFile := func() (string, io.ReadCloser, error) { // or ReadCloser?
			if findex >= len(files) || findex >= len(filesMeta) {
				return "", nil, io.EOF
			}
			f := files[findex]
			path := filesMeta[findex].Path
			file, err := f.Open()
			if err != nil {
				return "", nil, err
			}
			findex += 1
			return path, file, nil
		}
		if _, err := s.projects.UpdateFiles(projectName, changes, nextFile); err != nil {
			if errors.Is(err, application.ErrProjectSizeLimit) {
				return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "Reached project size limit.")
			}
			return fmt.Errorf("[handleScriptUpload] saving script file: %w", err)
		}
		scripts, err := s.projects.GetScripts(projectName)
		if err != nil {
			return fmt.Errorf("[handleScriptUpload] getting scripts metadata: %w", err)
		}
		if scripts == nil {
			scripts = make(domain.Scripts, 1)
		}
		info.Path = filepath.Join("web", "components", info.Path)
		scripts[info.Module] = info.ScriptModule
		// s.log.Infow("[handleScriptUpload]", "scripts", scripts)

		if err := s.projects.UpdateScripts(projectName, scripts); err != nil {
			return fmt.Errorf("[handleScriptUpload] updating scripts metadata: %w", err)
		}
		return c.JSON(http.StatusOK, scripts)
	}
}

func (s *Server) handleDeleteScript() func(echo.Context) error {
	type Params struct {
		Modules []string `json:"modules"`
	}
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		// var params Params
		var modules []string
		if err := (&echo.DefaultBinder{}).BindBody(c, &modules); err != nil {
			return err
		}
		scripts, err := s.projects.RemoveScripts(projectName, modules...)
		if err != nil {
			return fmt.Errorf("[handleDeleteScript] removing scripts: %w", err)
		}
		return c.JSON(http.StatusOK, scripts)
	}
}

func (s *Server) handleProjectFile(c echo.Context) error {
	projectName := c.Get("project").(string)
	filePath := c.Param("*")
	return c.File(filepath.Join(s.Config.ProjectsRoot, projectName, filePath))
}

func CopyFile(dest io.Writer, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(dest, file)
	return err
}

func (s *Server) handleDownloadProjectFiles(c echo.Context) error {
	projectName := c.Get("project").(string)
	filePath := c.Param("*")
	fullPath := filepath.Join(s.Config.ProjectsRoot, projectName, filePath)

	name := filepath.Base(fullPath)

	info, err := os.Lstat(fullPath)
	if err != nil {
		return fmt.Errorf("getting file info: %w", err)
	}
	if info.IsDir() {
		c.Response().Header().Set("Content-Type", "application/octet-stream")
		c.Response().Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.zip", name))
		writer := zip.NewWriter(c.Response())
		defer writer.Close()
		rootPath := filepath.Dir(fullPath)
		err := filepath.WalkDir(fullPath, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				// relPath2 := path[len(rootPath)+1:]
				relPath, _ := filepath.Rel(rootPath, path)
				part, err := writer.Create(relPath)
				if err != nil {
					return err
				}
				return CopyFile(part, path)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("downloading project directory: %w", err)
		}
		return nil
	}
	return c.Attachment(fullPath, name)
}

func (s *Server) handleInlineProjectFile(c echo.Context) error {
	projectName := c.Get("project").(string)
	filePath := c.Param("*")
	name := filepath.Base(filePath)
	return c.Inline(filepath.Join(s.Config.ProjectsRoot, projectName, filePath), name)
}

func (s *Server) handleProjectReload(c echo.Context) error {
	client := &http.Client{}
	projectName := c.Get("project").(string)
	p, err := s.projects.GetProjectInfo(projectName)
	if err != nil {
		if errors.Is(err, domain.ErrProjectNotExists) {
			return echo.NewHTTPError(http.StatusBadRequest, "Project does not exists")
		}
		return err
	}
	// TODO: hardcoded /publish/ directory!
	owsProject := filepath.Join("/publish/", projectName, p.QgisFile)
	params := url.Values{"MAP": {owsProject}}

	req, err := http.NewRequest(http.MethodPost, s.Config.MapserverURL, nil)
	if err != nil {
		return fmt.Errorf("[handleProjectReload] building request: %w", err)
	}
	req.URL.Path = filepath.Join(req.URL.Path, "/reload")
	req.URL.RawQuery = params.Encode()
	// s.log.Infow("[handleProjectReload]", "project", projectName, "url", req.URL.String())

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("mapserver request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		msg, _ := ioutil.ReadAll(resp.Body)
		s.log.Errorw("[handleProjectReload]", "project", projectName, "status", resp.StatusCode, "msg", string(msg))
		return fmt.Errorf("reloading project on qgis server: %s", string(msg))
	}
	return c.NoContent(http.StatusOK)
}

/*
func (s *Server) handleMediaFileUpload(c echo.Context) error {
	projectName := c.Get("project").(string)
	directory := c.Param("*")
	// directory := c.QueryParam("directory")

	if err := c.Request().ParseMultipartForm(2 * MB); err != nil {
		return err
	}
	var filesMeta []domain.ProjectFile = []domain.ProjectFile{}
	var files []*multipart.FileHeader = []*multipart.FileHeader{}

	form, err := c.MultipartForm()
	if err != nil {
		return fmt.Errorf("parsing MultipartForm data: %w", err)
	}
	for n, f := range form.File {
		s.log.Infow("[handleMediaFileUpload]", "file", n, "len", len(f))
		for _, fh := range f {
			path := filepath.Join(directory, fh.Filename)
			filesMeta = append(filesMeta, domain.ProjectFile{Path: path, Size: fh.Size})
			files = append(files, fh)
		}
	}
	changes := domain.FilesChanges{Updates: filesMeta}
	s.log.Infow("[handleMediaFileUpload]", "directory", directory, "changes", changes)
	if true {
		return nil
	}

	findex := 0
	nextFile := func() (string, io.ReadCloser, error) { // or ReadCloser?
		if findex >= len(files) || findex >= len(filesMeta) {
			return "", nil, io.EOF
		}
		f := files[findex]
		path := filesMeta[findex].Path
		file, err := f.Open()
		if err != nil {
			return "", nil, err
		}
		findex += 1
		return path, file, nil
	}
	if _, err := s.projects.UpdateFiles(projectName, changes, nextFile); err != nil {
		return fmt.Errorf("[handleMediaFileUpload] saving script file: %w", err)
	}
	return nil
}
*/

func (s *Server) mediaFileHandler(cacheDir string) func(echo.Context) error {
	var lock singleflight.Group
	return func(c echo.Context) error {
		projectName := c.Get("project").(string)
		filePath := c.Param("*")
		folder := filepath.Dir(filePath)
		if folder != "web" && !strings.HasPrefix(folder, "web/") {
			return echo.ErrNotFound
		}

		absPath := filepath.Join(s.Config.ProjectsRoot, projectName, filePath)
		if cacheDir != "" && strings.EqualFold(c.Request().URL.Query().Get("thumbnail"), "true") {
			key := filepath.Join(projectName, filePath)
			val, err, _ := lock.Do(key, func() (interface{}, error) {
				srcFinfo, err := os.Stat(absPath)
				if err != nil {
					return "", err
				}
				thumbAbsPath := filepath.Join(cacheDir, key)
				finfo, err := os.Stat(thumbAbsPath)
				if err == nil {
					if finfo.ModTime().Unix() > srcFinfo.ModTime().Unix() {
						// valid thumbnail image
						return thumbAbsPath, nil
					}
				}

				err = os.MkdirAll(filepath.Dir(thumbAbsPath), 0777)
				if err != nil {
					return "", err
				}

				srcImage, err := imaging.Open(absPath, imaging.AutoOrientation(true))
				if err != nil {
					return "", fmt.Errorf("reading media image file: %w", err)
				}

				dstImageFit := imaging.Fit(srcImage, 500, 500, imaging.Lanczos)
				format, err := imaging.FormatFromFilename(absPath)
				if err != nil {
					format = imaging.JPEG
				}

				f, err := os.Create(thumbAbsPath)
				if err != nil {
					return "", err
				}
				defer f.Close()
				err = imaging.Encode(f, dstImageFit, format, imaging.JPEGQuality(75))
				return thumbAbsPath, err
			})
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return echo.NewHTTPError(http.StatusNotFound, "Image not found")
				}
				return err
			}
			absPath = val.(string)
		}
		// maybe when media folders permissions will be implemented
		// c.Response().Header().Set("Cache-Control", "private, must-revalidate")
		return c.File(absPath)
	}
}

func (s *Server) appMediaFileHandler(c echo.Context) error {
	username := c.Param("user")
	name := c.Param("name")
	projectName := filepath.Join(username, name)
	filePath := c.Param("*")
	absPath := filepath.Join(s.Config.ProjectsRoot, projectName, "web", "app", filePath)
	return c.File(absPath)
}

type MediaFile struct {
	domain.ProjectFile
	Filename string `json:"filename"`
}

func (s *Server) handleUploadMediaFile(c echo.Context) error {
	projectName := c.Get("project").(string)
	directory := c.Param("*")
	file, err := c.FormFile("file")
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}
	if !strings.HasPrefix(directory, "web/") {
		return echo.ErrForbidden
	}

	// user, err := s.auth.GetUser(c)
	// if err != nil {
	// 	return err
	// }

	// TODO: check directory access
	// s.projects.GetDirectoryPerms(user)
	src, err := file.Open()
	if err != nil {
		return fmt.Errorf("reading upload file: %w", err)
	}

	finfo, err := s.projects.SaveFile(projectName, directory, file.Filename, src, file.Size)
	if err != nil {
		if errors.Is(err, application.ErrProjectSizeLimit) {
			return echo.NewHTTPError(http.StatusRequestEntityTooLarge, "Reached project size limit.")
		}
		return err
	}
	return c.JSON(http.StatusOK, MediaFile{finfo, filepath.Base(finfo.Path)})
}

func (s *Server) handleDeleteMediaFile(c echo.Context) error {
	projectName := c.Get("project").(string)
	path := c.Param("*")
	if !strings.HasPrefix(path, "web/") {
		return echo.ErrForbidden
	}
	return s.projects.DeleteFile(projectName, path)
}
