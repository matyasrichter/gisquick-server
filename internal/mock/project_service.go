package mock

import (
	"encoding/json"
	"io"

	"github.com/gisquick/gisquick-server/internal/application"
	"github.com/gisquick/gisquick-server/internal/domain"
)

// compile-time check
var _ application.ProjectService = (*ProjectService)(nil)

// ProjectService is a configurable test double for application.ProjectService.
// Only the two processing config methods have override hooks; all others are no-op stubs.
type ProjectService struct {
	GetProcessingConfigFunc    func(string) (domain.ProcessingConfig, error)
	UpdateProcessingConfigFunc func(string, domain.ProcessingConfig) error
}

func (m *ProjectService) GetProcessingConfig(n string) (domain.ProcessingConfig, error) {
	if m.GetProcessingConfigFunc != nil {
		return m.GetProcessingConfigFunc(n)
	}
	return domain.ProcessingConfig{}, nil
}

func (m *ProjectService) UpdateProcessingConfig(n string, cfg domain.ProcessingConfig) error {
	if m.UpdateProcessingConfigFunc != nil {
		return m.UpdateProcessingConfigFunc(n, cfg)
	}
	return nil
}

func (m *ProjectService) Create(projectName string, meta json.RawMessage) (*domain.ProjectInfo, error) {
	return nil, nil
}

func (m *ProjectService) Delete(projectName string) error { return nil }

func (m *ProjectService) GetProjectInfo(projectName string) (domain.ProjectInfo, error) {
	return domain.ProjectInfo{}, nil
}

func (m *ProjectService) GetUserProjects(username string) ([]domain.ProjectInfo, error) {
	return nil, nil
}

func (m *ProjectService) AccessibleProjects(username string, skipErrors bool) ([]domain.ProjectInfo, error) {
	return nil, nil
}

func (m *ProjectService) ProjectsNames(skipErrors bool) ([]string, error) { return nil, nil }

func (m *ProjectService) SaveFile(projectName, dir, pattern string, r io.Reader, size int64) (domain.ProjectFile, error) {
	return domain.ProjectFile{}, nil
}

func (m *ProjectService) DeleteFile(projectName, path string) error { return nil }

func (m *ProjectService) ListProjectFiles(projectName string, checksum bool) ([]domain.ProjectFile, []domain.ProjectFile, error) {
	return nil, nil, nil
}

func (m *ProjectService) GetQgisMetadata(projectName string, data interface{}) error { return nil }

func (m *ProjectService) UpdateMeta(projectName string, meta json.RawMessage) error { return nil }

func (m *ProjectService) GetSettings(projectName string) (domain.ProjectSettings, error) {
	return domain.ProjectSettings{}, nil
}

func (m *ProjectService) UpdateSettings(projectName string, data json.RawMessage) error { return nil }

func (m *ProjectService) GetThumbnailPath(projectName string) string { return "" }

func (m *ProjectService) SaveThumbnail(projectName string, r io.Reader) error { return nil }

func (m *ProjectService) UpdateFiles(projectName string, info domain.FilesChanges, next func() (string, io.ReadCloser, error)) ([]domain.ProjectFile, error) {
	return nil, nil
}

func (m *ProjectService) GetLayersData(projectName string) (application.LayersData, error) {
	return application.LayersData{}, nil
}

func (m *ProjectService) GetMapConfig(projectName string, user domain.User) (map[string]interface{}, error) {
	return nil, nil
}

func (m *ProjectService) GetScripts(projectName string) (domain.Scripts, error) {
	return nil, nil
}

func (m *ProjectService) UpdateScripts(projectName string, scripts domain.Scripts) error { return nil }

func (m *ProjectService) RemoveScripts(projectName string, modules ...string) (domain.Scripts, error) {
	return nil, nil
}

func (m *ProjectService) GetProjectCustomizations(projectName string) (json.RawMessage, error) {
	return nil, nil
}

func (m *ProjectService) Close() {}
