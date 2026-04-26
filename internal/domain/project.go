package domain

import (
	"encoding/json"
	"errors"
	"io"
)

var (
	ErrProjectNotExists     = errors.New("project does not exists")
	ErrFileNotExists        = errors.New("project file does not exists")
	ErrProjectAlreadyExists = errors.New("project already exists")
)

// Old code, currently used in mapcache package
type ProjectFileInfo struct {
	User     string
	Folder   string
	Name     string
	FullName string
	Map      string
}

// TODO: remove
type Project struct {
	Info     ProjectFileInfo
	Meta     map[string]interface{}
	Settings ProjectSettings
}

func (p *Project) ProjectionCode() string {
	projection, ok := p.Meta["projection"].(map[string]interface{})
	if !ok {
		return ""
	}
	return projection["code"].(string)
}

type Projection struct {
	Proj4       string `json:"proj4"`
	IsGeografic bool   `json:"is_geographic"`
}

type LayerPermission struct {
	View         bool `json:"view"`
	Insert       bool `json:"insert"`
	Update       bool `json:"update"`
	Delete       bool `json:"delete"`
	EditGeometry bool `json:"edit_geom"`
}

type UserRolesPermissions struct {
	roles      []ProjectRole // user roles
	layers     map[string]Flags
	attributes map[string]map[string]Flags
	topics     []string
}

func NewUserRolesPermissions(user User, auth Authentication) *UserRolesPermissions {
	if auth.Roles == nil || len(auth.Roles) == 0 {
		return nil
	}
	roles := FilterUserRoles(user, auth.Roles)
	layersFlags := make(map[string]Flags)
	attributesFlags := make(map[string]map[string]Flags)
	return &UserRolesPermissions{roles: roles, layers: layersFlags, attributes: attributesFlags}
}

func (p *UserRolesPermissions) LayerFlags(layerId string) Flags {
	flags, exists := p.layers[layerId]
	if !exists {
		if len(p.roles) == 0 {
			flags = Flags{}
		} else {
			flags = p.roles[0].Permissions.Layers[layerId]
			for _, f := range p.roles[1:] {
				flags = flags.Union(f.Permissions.Layers[layerId])
			}
		}
		p.layers[layerId] = flags
	}
	return flags
}

func (p *UserRolesPermissions) AttributesFlags(layerId string) map[string]Flags {
	flagsMap, exists := p.attributes[layerId]
	if !exists {
		flagsMap = make(map[string]Flags, 0)
		for _, f := range p.roles {
			for attrName, flags := range f.Permissions.Attributes[layerId] {
				flagsMap[attrName] = flags.Union(flagsMap[attrName])
			}
		}
		p.attributes[layerId] = flagsMap
	}
	return flagsMap
}

func (p *UserRolesPermissions) UserTopics() []string {
	if p.topics == nil {
		topics := Flags{}
		for _, r := range p.roles {
			topics = topics.Union(r.Permissions.Topics)
		}
		p.topics = topics
	}
	return p.topics
}

func (s ProjectSettings) UserLayerPermissionsFlags(u User, layerId string) Flags {
	lset, ok := s.Layers[layerId]
	if !ok || lset.Flags.Has("excluded") {
		return nil
	}
	roles := FilterUserRoles(u, s.Auth.Roles)
	if len(roles) == 0 {
		return nil
	}
	flags := roles[0].Permissions.Layers[layerId]
	for _, role := range roles[1:] {
		flags = flags.Union(role.Permissions.Layers[layerId])
	}
	return flags
}

func (s ProjectSettings) UserLayerAttrinutesFlags(u User, layerId string) map[string]Flags {
	roles := FilterUserRoles(u, s.Auth.Roles)
	finalFlags := make(map[string]Flags)
	for _, role := range roles {
		for attrName, flags := range role.Permissions.Attributes[layerId] {
			f, exists := finalFlags[attrName]
			if !exists {
				finalFlags[attrName] = flags
			} else {
				finalFlags[attrName] = f.Union(flags)
			}
		}
	}
	return finalFlags
}

type FileInfo struct {
	Hash  string `json:"hash,omitempty"`
	Size  int64  `json:"size"`
	Mtime int64  `json:"mtime"`
}

type ProjectFile struct {
	Path  string `json:"path"`
	Hash  string `json:"hash,omitempty"`
	Size  int64  `json:"size"`
	Mtime int64  `json:"mtime"`
}

func checkUserRole(u User, role ProjectRole) bool {
	if role.Auth == "all" {
		return true
	}
	if role.Auth == "authenticated" {
		return u.IsAuthenticated
	}
	if role.Auth == "anonymous" {
		return !u.IsAuthenticated
	}
	if role.Auth == "users" {
		for _, username := range role.Users {
			if u.Username == username {
				return true
			}
		}
	}
	return false
}

func FilterUserRoles(u User, roles []ProjectRole) []ProjectRole {
	var userRoles []ProjectRole
	for _, r := range roles {
		if r.Auth != "other" && checkUserRole(u, r) {
			userRoles = append(userRoles, r)
		}
	}
	if len(userRoles) == 0 {
		for _, r := range roles {
			if r.Auth == "other" {
				userRoles = append(userRoles, r)
			}
		}
	}
	return userRoles
}

type FilesChanges struct {
	Removes []string
	Updates []ProjectFile
}

type ScriptModule struct {
	Path       string   `json:"path"`
	Components []string `json:"components"`
}

type Scripts map[string]ScriptModule

type FilesReader func() (string, io.ReadCloser, error)

// ProcessingServiceType represents the type of processing API.
type ProcessingServiceType string

const (
	ProcessingServiceTypeWPS          ProcessingServiceType = "wps"
	ProcessingServiceTypeOGCProcesses ProcessingServiceType = "ogcapi-processes"
)

// RemoteConfig describes the remote processing service endpoint.
type RemoteConfig struct {
	Type       string            `json:"type"`                  // "wps" or "ogcapi-processes"
	ExecuteURL string            `json:"execute_url,omitempty"` // override; defaults to {service_url}/processes/{id}/execution
	Method     string            `json:"method,omitempty"`      // default "POST"
	StatusURL  string            `json:"status_url,omitempty"`
	ResultURL  string            `json:"result_url,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
}

// ProcessConfig holds the stored description for a specific process.
type ProcessConfig struct {
	Title       string          `json:"title,omitempty"`
	Description json.RawMessage `json:"description,omitempty"` // full OGC API process description, stored at registration time
}

// ProcessingService represents a configured processing backend.
type ProcessingService struct {
	ID        string                   `json:"id"`
	URL       string                   `json:"url"`
	Type      ProcessingServiceType    `json:"type"`
	Name      string                   `json:"name,omitempty"`
	Headers   map[string]string        `json:"headers,omitempty"`
	Processes map[string]ProcessConfig `json:"processes,omitempty"`
}

// ProcessingConfig holds the processing configuration for a project.
type ProcessingConfig struct {
	Services []ProcessingService `json:"services"`
}

type ProjectsRepository interface {
	CheckProjectExists(name string) bool
	Create(name string, qmeta json.RawMessage) (*ProjectInfo, error)
	AllProjects(skipErrors bool) ([]string, error)
	UserProjects(user string) ([]string, error) // or should it require User object?
	GetProjectInfo(name string) (ProjectInfo, error)
	Delete(name string) error
	// SaveFile(projectName, filename string, r io.Reader) error
	CreateFile(projectName, directory, pattern string, r io.Reader) (ProjectFile, error)
	SaveFile(project string, finfo ProjectFile, path string) error

	GetFileInfo(project, path string) (FileInfo, error)
	GetFilesInfo(project string, paths ...string) (map[string]FileInfo, error)
	ListProjectFiles(project string, checksum bool) ([]ProjectFile, []ProjectFile, error)

	ParseQgisMetadata(projectName string, data interface{}) error
	UpdateMeta(projectName string, meta json.RawMessage) error

	GetSettings(projectName string) (ProjectSettings, error)
	UpdateSettings(projectName string, data json.RawMessage) error

	GetThumbnailPath(projectName string) string
	SaveThumbnail(projectName string, r io.Reader) error

	UpdateFiles(projectName string, info FilesChanges, next FilesReader) ([]ProjectFile, error)
	GetScripts(projectName string) (Scripts, error)
	UpdateScripts(projectName string, scripts Scripts) error
	GetProcessingConfig(projectName string) (ProcessingConfig, error)
	UpdateProcessingConfig(projectName string, cfg ProcessingConfig) error
	GetProjectCustomizations(projectName string) (json.RawMessage, error)
	Close()
}
