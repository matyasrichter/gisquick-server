package project

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gisquick/gisquick-server/internal/domain"
	"github.com/gisquick/gisquick-server/internal/infrastructure/cache"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
)

type FilesIndex struct {
	sync.RWMutex
	Index map[string]domain.FileInfo
}

func (fi *FilesIndex) Get(path string) (domain.FileInfo, bool) {
	fi.RLock()
	defer fi.RUnlock()
	val, exists := fi.Index[path]
	return val, exists
}

func (fi *FilesIndex) GetFiles(paths ...string) map[string]domain.FileInfo {
	fi.RLock()
	defer fi.RUnlock()
	data := make(map[string]domain.FileInfo, len(paths))
	for _, path := range paths {
		val, exists := fi.Index[path]
		if exists {
			data[path] = val
		}
	}
	return data
}

func (fi *FilesIndex) Set(path string, info domain.FileInfo) {
	fi.Lock()
	defer fi.Unlock()
	fi.Index[path] = info
}

func (fi *FilesIndex) Delete(path string) {
	fi.Lock()
	defer fi.Unlock()
	delete(fi.Index, path)
}

func (fi *FilesIndex) DeleteDir(dirPath string) {
	fi.Lock()
	defer fi.Unlock()

	dirPrefix := strings.TrimSuffix(dirPath, string(filepath.Separator)) + string(filepath.Separator)
	for p := range fi.Index {
		if strings.HasPrefix(p, dirPrefix) {
			delete(fi.Index, p)
		}
	}
}

func (fi *FilesIndex) TotalSize() int64 {
	fi.RLock()
	defer fi.RUnlock()
	size := int64(0)
	for _, info := range fi.Index {
		size += info.Size
	}
	return size
}

type DiskStorage struct {
	ProjectsRoot      string
	log               *zap.SugaredLogger
	indexCache        *ttlcache.Cache[string, *FilesIndex]
	configCache       *cache.DataCache[string, json.RawMessage]
	projectInfoReader JsonFilesReader[domain.ProjectInfo]
	settingsReader    JsonFilesReader[domain.ProjectSettings]
}

type Info struct {
	Title       string `json:"title"`
	File        string `json:"file"`
	ProjectHash string `json:"project_hash"`
	Projection  string `json:"projection"`
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !errors.Is(err, os.ErrNotExist)
}

// func (s *DiskStorage) GetProject(name string) (*domain.Project, error) {
// 	loadFn := func(key string) (interface{}, time.Duration, error) {
// 		proj, err := s.loadProjectData(key)
// 		return proj, 0, err
// 	}
// 	p, err := s.cache.GetByLoader(project, loadFn)
// 	return p.(*domain.Project), err
// }

func DBHash(path string) (string, error) {
	cmdOut, err := exec.Command("dbhash", path).Output()
	if err != nil {
		return "", fmt.Errorf("executing dbhash command: %w", err)
	}
	hash := strings.Split(string(cmdOut), " ")[0]
	return hash, nil
}

// Checksum computes SHA-1 hash of file
func Sha1(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	h := sha1.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func Checksum(path string) (string, error) {
	if strings.ToLower(filepath.Ext(path)) == ".gpkg" {
		dbhash, err := DBHash(path)
		return "dbhash:" + dbhash, err
	}
	return Sha1(path)
}

type JsonFilesReader[T any] interface {
	Get(filename string) (T, error)
	Close()
}

var excludeExtRegex = regexp.MustCompile(`(?i).*\.(gpkg-wal|gpkg-shm)$`)

func NewDiskStorage(log *zap.SugaredLogger, projectsRoot string) *DiskStorage {
	cfgCache := cache.NewDataCache(func(filename string) (json.RawMessage, error) {
		var config json.RawMessage
		content, err := ioutil.ReadFile(filename)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, nil
			}
			return nil, fmt.Errorf("reading project file: %w", err)
		}
		err = json.Unmarshal(content, &config)
		if err != nil {
			return nil, fmt.Errorf("reading customization file: %w", err)
		}
		return config, nil
	})
	ds := &DiskStorage{
		ProjectsRoot: projectsRoot,
		log:          log,
		configCache:  cfgCache,
	}
	loader := ttlcache.LoaderFunc[string, *FilesIndex](
		func(c *ttlcache.Cache[string, *FilesIndex], project string) *ttlcache.Item[string, *FilesIndex] {
			log.Debugf("ttlcache.LoaderFunc: %s", project)

			indexData, err := ds.loadFilesIndex(project)
			if err != nil {
				log.Errorw("reading files index file", "project", project, zap.Error(err))
				files, _, err := ds.createFilesMap(project)
				if err != nil {
					log.Errorw("listing project files", "project", project, zap.Error(err))
					// TODO: return nil or empty index?
					// var emptyIndex map[string]*domain.FileInfo
					// emptyIndex := &FilesIndex{Index: emptyIndex}
					return nil
				}
				for path, info := range files {
					absPath := filepath.Join(projectsRoot, project, path)
					hash, err := Checksum(absPath)
					if err != nil {
						log.Errorw("listing project files", "project", project, zap.Error(err))
						return nil
					}
					// info := files[path]
					info.Hash = hash
					files[path] = info
				}
				indexData = files
			}
			index := &FilesIndex{Index: indexData}
			item := c.Set(project, index, ttlcache.DefaultTTL)
			return item
		},
	)
	indexCache := ttlcache.New(
		ttlcache.WithTTL[string, *FilesIndex](12*time.Hour),
		// ttlcache.WithTTL[string, *FilesIndex](1*time.Minute),
		ttlcache.WithLoader[string, *FilesIndex](loader),
		ttlcache.WithDisableTouchOnHit[string, *FilesIndex](),
	)
	ds.indexCache = indexCache
	indexCache.OnEviction(func(ctx context.Context, er ttlcache.EvictionReason, i *ttlcache.Item[string, *FilesIndex]) {
		project := i.Key()
		index := i.Value()
		log.Infow("ttlcache.OnEviction.indexCache", "project", project)
		if err := saveJsonFile(filepath.Join(projectsRoot, project, ".gisquick", "filesmap.json"), index.Index); err != nil {
			log.Errorw("saving files index", "project", project, zap.Error(err))
		}
	})
	go indexCache.Start()
	ds.settingsReader = cache.NewJSONFileReader[domain.ProjectSettings](time.Hour)
	ds.projectInfoReader = cache.NewJSONFileReader[domain.ProjectInfo](time.Hour)
	return ds
}

func saveJsonFile(path string, data interface{}) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	if err := encoder.Encode(data); err != nil {
		return err
	}
	return nil
}

func (s *DiskStorage) saveConfigFile(projectName, filename string, data interface{}) error {
	indexFilePath := filepath.Join(s.ProjectsRoot, projectName, ".gisquick", filename)
	if err := saveJsonFile(indexFilePath, data); err != nil {
		return fmt.Errorf("creating project file: %w", err)
	}
	return nil
}

func (s *DiskStorage) Create(fullName string, meta json.RawMessage) (*domain.ProjectInfo, error) {
	projDir := filepath.Join(s.ProjectsRoot, fullName)
	internalDir := filepath.Join(projDir, ".gisquick")
	if s.CheckProjectExists(fullName) {
		return nil, domain.ErrProjectAlreadyExists
	}
	if err := os.MkdirAll(internalDir, 0775); err != nil {
		return nil, err
	}

	var i Info
	if err := json.Unmarshal(meta, &i); err != nil {
		s.log.Errorw("parsing qgis meta", zap.Error(err))
		return nil, domain.ErrInvalidQgisMeta
	}

	if err := s.saveConfigFile(fullName, "qgis.json", meta); err != nil {
		return nil, fmt.Errorf("creating qgis meta file: %w", err)
	}

	info := domain.ProjectInfo{
		QgisFile:   i.File,
		Projection: i.Projection,
		Title:      i.Title,
		State:      "empty",
		Created:    time.Now().UTC(),
	}
	return &info, s.saveConfigFile(fullName, "project.json", info)
}

func (s *DiskStorage) UserProjects(username string) ([]string, error) {
	projectsNames := make([]string, 0)
	userDir := filepath.Join(s.ProjectsRoot, username)
	entries, err := os.ReadDir(userDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return projectsNames, nil
		}
		return projectsNames, fmt.Errorf("listing projects: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			projectName := filepath.Join(username, entry.Name())
			projPath := filepath.Join(userDir, entry.Name(), ".gisquick", "project.json")
			if fileExists(projPath) {
				projectsNames = append(projectsNames, projectName)
			}
		}
	}
	return projectsNames, nil
}

func (s *DiskStorage) AllProjects(skipErrors bool) ([]string, error) {
	projectsNames := make([]string, 0)
	entries, err := os.ReadDir(s.ProjectsRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return projectsNames, nil
		}
		return projectsNames, fmt.Errorf("listing projects: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			username := entry.Name()
			userProjects, err := s.UserProjects(username)
			if err != nil {
				s.log.Errorw("listing projects", "user", username, zap.Error(err))
				if !skipErrors {
					return userProjects, fmt.Errorf("listing projects: %v", err)
				}
			} else {
				projectsNames = append(projectsNames, userProjects...)
			}
		}
	}
	return projectsNames, nil
}

func (s *DiskStorage) CheckProjectExists(name string) bool {
	projPath := filepath.Join(s.ProjectsRoot, name, ".gisquick", "project.json")
	return fileExists(projPath)
}

func (s *DiskStorage) GetProjectInfo(name string) (domain.ProjectInfo, error) {
	projPath := filepath.Join(s.ProjectsRoot, name, ".gisquick", "project.json")
	pInfo, err := s.projectInfoReader.Get(projPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.ProjectInfo{}, domain.ErrProjectNotExists
		}
		return domain.ProjectInfo{}, err
	}
	pInfo.Name = name
	return pInfo, nil
}

// func (s *DiskStorage) saveFileIndex(project string, index *FilesIndex) {
// 	if err := saveJsonFile(filepath.Join(s.ProjectsRoot, project, ".gisquick", "filesmap.json"), index); err != nil {
// 		return nil, fmt.Errorf("saving files index: %w", err)
// 	}
// }

func (s *DiskStorage) createFilesMap(project string) (map[string]domain.FileInfo, map[string]domain.FileInfo, error) {
	files := make(map[string]domain.FileInfo)
	excludedFiles := make(map[string]domain.FileInfo)
	root, err := filepath.Abs(filepath.Join(s.ProjectsRoot, project))
	if err != nil {
		return nil, nil, err
	}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// skip files which already not exists
				return nil
			}
			return err
		}
		if !entry.IsDir() {
			relPath := path[len(root)+1:]
			if !strings.HasPrefix(relPath, ".gisquick/") && !strings.HasSuffix(relPath, "~") {
				fInfo, err := entry.Info()
				if err != nil {
					return fmt.Errorf("getting file info: %w", err)
				}
				entry := domain.FileInfo{Size: fInfo.Size(), Mtime: fInfo.ModTime().Unix()}
				if excludeExtRegex.Match([]byte(relPath)) {
					excludedFiles[relPath] = entry
				} else {
					files[relPath] = entry
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("listing project files: %w", err)
	}
	return files, excludedFiles, nil
}

// TODO: update files index when not up to date
/*
func (s *DiskStorage) ListProjectFiles1(project string, checksum bool) ([]domain.ProjectFile, error) {
	if !s.CheckProjectExists(project) {
		return nil, domain.ErrProjectNotExists
	}
	index, err := s.filesIndex(project)
	if err != nil {
		// log error and continue without index
		s.log.Errorw("getting files index", zap.Error(err))
	}

	root, err := filepath.Abs(filepath.Join(s.ProjectsRoot, project))
	if err != nil {
		return nil, err
	}
	var files []domain.ProjectFile = []domain.ProjectFile{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			relPath := path[len(root)+1:]
			if !strings.HasPrefix(relPath, ".gisquick/") && !strings.HasSuffix(relPath, "~") && !excludeExtRegex.Match([]byte(relPath)) {
				fInfo, err := entry.Info()
				if err != nil {
					return fmt.Errorf("getting file info: %w", err)
				}
				finfo := domain.ProjectFile{Path: relPath, Size: fInfo.Size(), Mtime: fInfo.ModTime()}
				if checksum {
					cachedInfo, hasCachedInfo := index.Get(relPath)
					if hasCachedInfo && cachedInfo.Mtime == fInfo.ModTime().Unix() {
						finfo.Hash = cachedInfo.Hash
					} else {
						hash, err := Checksum(path)
						if err != nil {
							return fmt.Errorf("computing checksum: %w", err)
						}
						finfo.Hash = hash
						index.Set(relPath, domain.FileInfo{Hash: finfo.Hash, Size: finfo.Size, Mtime: finfo.Mtime.Unix()})
					}
				}
				files = append(files, finfo)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing project files: %w", err)
	}
	return files, nil
}
*/

func (s *DiskStorage) ListProjectFiles(project string, checksum bool) ([]domain.ProjectFile, []domain.ProjectFile, error) {
	if !s.CheckProjectExists(project) {
		return nil, nil, domain.ErrProjectNotExists
	}
	filesMap, temporaryFiles, err := s.createFilesMap(project)
	if err != nil {
		return nil, nil, fmt.Errorf("listing project files: %w", err)
	}
	index, err := s.filesIndex(project)
	if err != nil {
		s.log.Errorw("reading project files index", "project", project, zap.Error(err))
		return nil, nil, fmt.Errorf("reading project files index: %w", err)
	}
	indexUpdated := false
	files := make([]domain.ProjectFile, len(filesMap))
	i := 0
	for path, info := range filesMap {
		f := domain.ProjectFile{
			Path:  path,
			Size:  info.Size,
			Mtime: info.Mtime,
		}
		if checksum {
			cachedInfo, hasCachedInfo := index.Get(path)
			if hasCachedInfo && cachedInfo.Mtime == info.Mtime {
				f.Hash = cachedInfo.Hash
			} else {
				absPath := filepath.Join(s.ProjectsRoot, project, path)
				hash, err := Checksum(absPath)
				if err != nil {
					return nil, nil, fmt.Errorf("computing checksum: %w", err)
				}
				f.Hash = hash
				// update file info in the index
				index.Set(path, domain.FileInfo{Hash: hash, Size: info.Size, Mtime: info.Mtime})
				indexUpdated = true
				s.log.Debugw("updating files index", "path", path)
			}
		}
		files[i] = f
		i += 1
	}
	// index.RLock()
	// defer index.RUnlock()
	for path := range index.Index {
		if _, exists := filesMap[path]; !exists {
			index.Delete(path)
			indexUpdated = true
			s.log.Debugw("cleaning files index", "path", path)
		}
	}
	if indexUpdated {
		projectInfo, err := s.GetProjectInfo(project)
		if err != nil {
			s.log.Errorw("updating project size", "project", project, zap.Error(err))
		}
		projectInfo.Size = index.TotalSize()
		if err := s.saveConfigFile(project, "project.json", projectInfo); err != nil {
			s.log.Errorw("updating project size", "project", project, zap.Error(err))
		}
	}
	tempFiles := make([]domain.ProjectFile, len(temporaryFiles))
	i = 0
	for path, info := range temporaryFiles {
		// without checksums
		tempFiles[i] = domain.ProjectFile{
			Path:  path,
			Size:  info.Size,
			Mtime: info.Mtime,
		}
		i += 1
	}
	return files, tempFiles, nil
}

// func (s *DiskStorage) GetFileInfo(project, path string, checksum bool) (domain.FileInfo, error) {
// 	absPath, err := filepath.Abs(filepath.Join(s.ProjectsRoot, project, path))
// 	if err != nil {
// 		return domain.FileInfo{}, err
// 	}
// 	fi, err := os.Stat(absPath)
// 	if err != nil {
// 		if errors.Is(err, os.ErrNotExist) {
// 			return domain.FileInfo{}, domain.ErrFileNotExists
// 		}
// 		return domain.FileInfo{}, fmt.Errorf("getting file info: %w", err)
// 	}
// 	pfi := domain.FileInfo{Size: fi.Size(), Mtime: fi.ModTime().Unix()}
// 	if checksum {
// 		hash, err := Checksum(absPath)
// 		if err != nil {
// 			return pfi, fmt.Errorf("getting file checksum: %w", err)
// 		}
// 		pfi.Hash = hash
// 	}
// 	return pfi, nil
// }

func (s *DiskStorage) GetFileInfo(project, path string) (domain.FileInfo, error) {
	index, err := s.filesIndex(project)
	if err != nil {
		s.log.Errorw("reading files index", "project", project, zap.Error(err))
		return domain.FileInfo{}, fmt.Errorf("reading files index [%s]: %w", project, err)
	}
	fi, exists := index.Get(path)
	if !exists {
		return domain.FileInfo{}, domain.ErrFileNotExists
	}
	return fi, nil
}

func (s *DiskStorage) GetFilesInfo(project string, paths ...string) (map[string]domain.FileInfo, error) {
	index, err := s.filesIndex(project)
	if err != nil {
		s.log.Errorw("reading files index", "project", project, zap.Error(err))
		// return nil, fmt.Errorf("loading files index: %w", err)
		return nil, fmt.Errorf("reading files index [%s]: %w", project, err)
	}
	data := index.GetFiles(paths...)
	return data, nil
}

func (s *DiskStorage) Delete(name string) error {
	if !s.CheckProjectExists(name) {
		return domain.ErrProjectNotExists
	}
	dest := filepath.Join(s.ProjectsRoot, name)
	if err := os.RemoveAll(dest); err != nil {
		return err
	}
	return nil
}

func saveToFile(src io.Reader, filename string) (err error) {
	err = os.MkdirAll(filepath.Dir(filename), 0775)
	if err != nil {
		return err
	}
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	// more verbose but with better errors propagation
	defer func() {
		if cerr := file.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	if _, err := io.Copy(file, src); err != nil {
		return err
	}
	return nil
}

func saveToFile2(src io.Reader, filename string) (h string, err error) {
	err = os.MkdirAll(filepath.Dir(filename), 0775)
	if err != nil {
		return
	}
	file, err := os.Create(filename)
	if err != nil {
		return
	}
	defer func() {
		// Clean up in case we are returning with an error
		if err != nil {
			file.Close()
			os.Remove(file.Name())
		}
	}()

	sha := sha1.New()
	dest := io.MultiWriter(file, sha)

	if _, err := io.Copy(dest, src); err != nil {
		return "", err
	}
	if err = file.Close(); err != nil {
		return
	}
	hash := fmt.Sprintf("%x", sha.Sum(nil))
	return hash, nil
}

func (s *DiskStorage) CreateFile(projectName, directory, pattern string, r io.Reader) (finfo domain.ProjectFile, err error) {
	finfo = domain.ProjectFile{}
	if !s.CheckProjectExists(projectName) {
		err = domain.ErrProjectNotExists
		return
	}
	destDir := filepath.Join(s.ProjectsRoot, projectName, directory)
	err = os.MkdirAll(destDir, 0775)
	if err != nil {
		err = fmt.Errorf("creating directory: %w", err)
		return
	}
	var f *os.File
	// pre-formatting: timestamp, random
	// post-formatting: hash
	if strings.Contains(pattern, "<timestamp>") {
		pattern = strings.Replace(pattern, "<timestamp>", fmt.Sprint(time.Now().Unix()), 1)
	}
	if strings.Contains(pattern, "<random>") {
		pattern = strings.Replace(pattern, "<random>", "*", 1)
		f, err = os.CreateTemp(destDir, pattern)
		if err != nil {
			err = fmt.Errorf("creating temp file: %w", err)
			return
		}
		pattern = filepath.Base(f.Name())
	}
	if f == nil {
		f, err = os.Create(filepath.Join(destDir, pattern))
		if err != nil {
			err = fmt.Errorf("creating new file: %w", err)
			return
		}
	}
	defer func() {
		// Clean up in case we are returning with an error
		if f != nil && err != nil {
			f.Close()
			os.Remove(f.Name())
		}
	}()
	sha := sha1.New()
	dest := io.MultiWriter(f, sha)
	if _, err = io.Copy(dest, r); err != nil {
		return
	}
	if err = f.Close(); err != nil {
		return
	}
	fStat, err := os.Stat(f.Name())
	if err != nil {
		return
	}
	finfo.Size = fStat.Size()
	finfo.Mtime = fStat.ModTime().Unix()
	finfo.Path = f.Name()
	finfo.Hash = fmt.Sprintf("%x", sha.Sum(nil))

	if strings.Contains(pattern, "<hash>") {
		pattern = strings.Replace(pattern, "<hash>", finfo.Hash[:10], 1)
		if err = os.Rename(f.Name(), filepath.Join(destDir, pattern)); err != nil {
			return
		}
	}
	finfo.Path = filepath.Join(directory, pattern)
	f = nil
	index, err := s.filesIndex(projectName)
	if err != nil {
		s.log.Errorw("reading files index", "project", projectName, zap.Error(err))
		return
	}
	index.Set(finfo.Path, domain.FileInfo{Hash: finfo.Hash, Size: finfo.Size, Mtime: finfo.Mtime})
	pInfo, err := s.GetProjectInfo(projectName)
	if err != nil {
		s.log.Errorw("getting project info", zap.Error(err))
	}
	pInfo.Size += finfo.Size
	if err := s.saveConfigFile(projectName, "project.json", pInfo); err != nil {
		s.log.Errorw("updating project file", zap.Error(err))
	}
	return
}

func (s *DiskStorage) SaveFile(project string, finfo domain.ProjectFile, path string) error {
	absPath := filepath.Join(s.ProjectsRoot, project, path)
	if err := os.MkdirAll(filepath.Dir(absPath), 0775); err != nil {
		return err
	}
	if err := os.Rename(finfo.Path, absPath); err != nil {
		return fmt.Errorf("saving project file: %w", err)
	}
	index, err := s.filesIndex(project)
	if err != nil {
		s.log.Errorw("reading files index", "project", project, zap.Error(err))
		return nil
	}
	index.Set(path, domain.FileInfo{Hash: finfo.Hash, Size: finfo.Size, Mtime: finfo.Mtime})
	pInfo, err := s.GetProjectInfo(project)
	if err != nil {
		s.log.Errorw("getting project info", zap.Error(err))
	}
	pInfo.Size += finfo.Size
	if err := s.saveConfigFile(project, "project.json", pInfo); err != nil {
		s.log.Errorw("updating project file", zap.Error(err))
	}
	return nil
}

func (s *DiskStorage) GetQgisMetaPath(projectName string) string {
	return filepath.Join(s.ProjectsRoot, projectName, ".gisquick", "qgis.json")
}

func (s *DiskStorage) GetSettingsPath(projectName string) string {
	return filepath.Join(s.ProjectsRoot, projectName, ".gisquick", "settings.json")
}

func (s *DiskStorage) GetThumbnailPath(projectName string) string {
	return filepath.Join(s.ProjectsRoot, projectName, ".gisquick", "thumbnail")
}

func (s *DiskStorage) SaveThumbnail(projectName string, r io.Reader) error {
	project, err := s.GetProjectInfo(projectName)
	if err != nil {
		return err
	}
	if err := saveToFile(r, s.GetThumbnailPath(projectName)); err != nil {
		return fmt.Errorf("saving thumbnail file: %w", err)
	}
	project.Thumbnail = true
	project.LastUpdate = time.Now().UTC()
	if err := s.saveConfigFile(projectName, "project.json", project); err != nil {
		return fmt.Errorf("updating project file: %w", err)
	}
	return nil
}

// func (s *DiskStorage) filesIndex1(projectName string) ([]domain.ProjectFile, error) {
// 	var files []domain.ProjectFile
// 	if !s.CheckProjectExists(projectName) {
// 		return files, domain.ErrProjectNotExists
// 	}
// 	indexPath := filepath.Join(s.ProjectsRoot, projectName, ".gisquick", "files.json")
// 	f, err := os.Open(indexPath)
// 	if err != nil {
// 		if errors.Is(err, os.ErrNotExist) {
// 			return files, nil
// 		}
// 		return files, fmt.Errorf("reading index file: %w", err)
// 	}
// 	defer f.Close()
// 	decoder := json.NewDecoder(f)
// 	if err := decoder.Decode(&files); err != nil {
// 		// s.log.Errorw("parsing project files index", zap.Error(err))
// 		return files, fmt.Errorf("parsing index file: %w", err)
// 	}
// 	return files, nil
// }

func (s *DiskStorage) loadFilesIndex(projectName string) (map[string]domain.FileInfo, error) {
	s.log.Infow("loading filesIndex", "project", projectName)
	var index map[string]domain.FileInfo
	indexPath := filepath.Join(s.ProjectsRoot, projectName, ".gisquick", "filesmap.json")
	f, err := os.Open(indexPath)
	if err != nil {
		index = make(map[string]domain.FileInfo)
		if errors.Is(err, os.ErrNotExist) {
			return index, nil
		}
		return index, fmt.Errorf("reading index file: %w", err)
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&index); err != nil {
		// s.log.Errorw("parsing project files index", zap.Error(err))
		return make(map[string]domain.FileInfo), fmt.Errorf("parsing index file: %w", err)
	}
	return index, nil
}

func (s *DiskStorage) filesIndex(projectName string) (*FilesIndex, error) {
	var index *FilesIndex
	if !s.CheckProjectExists(projectName) {
		return index, domain.ErrProjectNotExists
		// return make(map[string]domain.FileInfo), domain.ErrProjectNotExists
	}
	fi := s.indexCache.Get(projectName)
	if fi == nil {
		return index, fmt.Errorf("loading project files index: %s", projectName)
	}
	return fi.Value(), nil
}

// func createFilesIndex(files []domain.ProjectFile) map[string]domain.FileInfo {
// 	index := make(map[string]domain.FileInfo, len(files))
// 	for _, f := range files {
// 		index[f.Path] = domain.FileInfo{Hash: f.Hash, Size: f.Size, Mtime: time.Now().Unix()}
// 	}
// 	return index
// }

func indexProjectFilesList(index *FilesIndex) []domain.ProjectFile {
	index.RLock()
	defer index.RUnlock()
	listIndex := make([]domain.ProjectFile, len(index.Index))
	i := 0
	for path, info := range index.Index {
		listIndex[i] = domain.ProjectFile{Path: path, Hash: info.Hash, Size: info.Size, Mtime: info.Mtime}
		i += 1
	}
	return listIndex
}

func (s *DiskStorage) UpdateFiles(projectName string, info domain.FilesChanges, next domain.FilesReader) ([]domain.ProjectFile, error) {
	project, err := s.GetProjectInfo(projectName)
	if err != nil {
		return nil, err
	}
	index, err := s.filesIndex(projectName)
	if err != nil {
		return nil, err
	}
	updateFiles := info.Updates

	// i := 0
	// for {
	// 	path, reader, err := next()
	// 	if err != nil {
	// 		if err == io.EOF {
	// 			break
	// 		}
	// 		return nil, err
	// 	}
	// 	if i >= len(files) {
	// 		return nil, fmt.Errorf("missing file change metadata: %s", path)
	// 	}
	// 	i += 1
	// }
	if len(updateFiles) > 0 && next == nil {
		return nil, fmt.Errorf("required function for reading files")
	}
	for i := 0; i < len(updateFiles); i++ {
		path, reader, err := next()
		if err != nil {
			return nil, fmt.Errorf("reading upload files stream: %w", err)
		}
		declaredInfo := updateFiles[i]
		if declaredInfo.Path != path {
			return nil, err // TODO: more graceful error handling
		}
		absPath := filepath.Join(s.ProjectsRoot, projectName, path)
		// if err := saveToFile(reader, absPath); err != nil {
		// 	return err
		// }
		calcHash, err := saveToFile2(reader, absPath)
		if err != nil {
			reader.Close() // or move to saveToFile?
			return nil, err
		}
		// lmtime := declaredInfo.Mtime
		lmtime := time.Unix(declaredInfo.Mtime, 0)
		if err := os.Chtimes(absPath, lmtime, lmtime); err != nil {
			s.log.Errorw("updating file's modification time", zap.Error(err))
		}
		reader.Close()

		fStat, err := os.Stat(absPath)
		if err != nil {
			s.log.Errorw("getting file's stat info", zap.Error(err))
		} else if declaredInfo.Size != fStat.Size() {
			return nil, fmt.Errorf("declared file info doesn't match: %s", path)
		}
		finfo := domain.FileInfo{Hash: calcHash, Size: declaredInfo.Size, Mtime: declaredInfo.Mtime}
		if declaredInfo.Hash != "" {
			if strings.HasPrefix(declaredInfo.Hash, "dbhash:") {
				finfo.Hash = declaredInfo.Hash
			} else if declaredInfo.Hash != calcHash {
				return nil, fmt.Errorf("calculated file hash doesn't match: %s", path)
			}
		}
		// s.log.Infow("saving file", "path", absPath, "hash", calcHash, "hashMatch", declaredInfo.Hash == calcHash, "cmtime", declaredInfo.Mtime.Local(), "smtime", fStat.ModTime())
		index.Set(path, finfo)
	}
	for _, path := range info.Removes {
		absPath := filepath.Join(s.ProjectsRoot, projectName, path)
		info, err := os.Lstat(absPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				index.Delete(path)
				continue
			}
			return nil, fmt.Errorf("removing file/directory %s: %w", path, err)
		}
		if info.IsDir() {
			if err := os.RemoveAll(absPath); err != nil {
				return nil, fmt.Errorf("removing project directory %s: %w", path, err) // TODO: or allow this kind of error?
			}
			index.DeleteDir(path)
		} else {
			if err := os.Remove(absPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("removing project file %s: %w", path, err) // TODO: or allow this kind of error?
			}
			index.Delete(path)
		}
	}
	if err := saveJsonFile(filepath.Join(s.ProjectsRoot, projectName, ".gisquick", "filesmap.json"), index); err != nil {
		return nil, fmt.Errorf("saving files index: %w", err)
	}
	size := index.TotalSize()
	project.Size = size
	if project.State == "empty" && size > 0 {
		project.State = "staged"
		project.LastUpdate = time.Now().UTC()
	}
	if err := s.saveConfigFile(projectName, "project.json", project); err != nil {
		return nil, fmt.Errorf("updating project file: %w", err)
	}
	return indexProjectFilesList(index), nil
}

type SettingsInfo struct {
	Title string `json:"title"`
	Auth  struct {
		Type string `json:"type"`
	} `json:"auth"`
}

func (s *DiskStorage) UpdateSettings(projectName string, data json.RawMessage) error {
	project, err := s.GetProjectInfo(projectName)
	if err != nil {
		return err
	}
	var sInfo SettingsInfo
	if err := json.Unmarshal(data, &sInfo); err != nil {
		return fmt.Errorf("extracting authentication settings: %w", err)
	}
	if err := s.saveConfigFile(projectName, "settings.json", data); err != nil {
		return fmt.Errorf("saving settings file: %w", err)
	}
	project.State = "published"
	project.LastUpdate = time.Now().UTC()
	project.Authentication = sInfo.Auth.Type
	project.Title = sInfo.Title
	if err := s.saveConfigFile(projectName, "project.json", project); err != nil {
		return fmt.Errorf("updating project file: %w", err)
	}
	return nil
}

func (s *DiskStorage) GetSettings(projectName string) (domain.ProjectSettings, error) {
	var settings domain.ProjectSettings
	data, err := s.settingsReader.Get(s.GetSettingsPath(projectName))
	if err != nil {
		return settings, err
	}
	return data, nil
}

func (s *DiskStorage) ParseQgisMetadata(projectName string, data interface{}) error {
	content, err := os.ReadFile(s.GetQgisMetaPath(projectName))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(content, &data); err != nil {
		return err
	}
	return nil
}

func (s *DiskStorage) UpdateMeta(projectName string, meta json.RawMessage) error {
	pInfo, err := s.GetProjectInfo(projectName)
	if err != nil {
		return err
	}
	var i Info
	if err := json.Unmarshal(meta, &i); err != nil {
		s.log.Errorw("parsing qgis meta", zap.Error(err))
		return domain.ErrInvalidQgisMeta
	}

	if err := s.saveConfigFile(projectName, "qgis.json", meta); err != nil {
		return fmt.Errorf("creating qgis meta file: %w", err)
	}

	pInfo.QgisFile = i.File
	pInfo.Projection = i.Projection
	pInfo.Title = i.Title
	pInfo.LastUpdate = time.Now().UTC()
	return s.saveConfigFile(projectName, "project.json", pInfo)
}

func (s *DiskStorage) GetScripts(projectName string) (domain.Scripts, error) {
	file := filepath.Join(s.ProjectsRoot, projectName, ".gisquick", "scripts.json")
	content, err := os.ReadFile(file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var data domain.Scripts
	err = json.Unmarshal(content, &data)
	return data, nil
}

func (s *DiskStorage) UpdateScripts(projectName string, scripts domain.Scripts) error {
	return s.saveConfigFile(projectName, "scripts.json", scripts)
}

func (s *DiskStorage) GetProcessingConfig(projectName string) (domain.ProcessingConfig, error) {
	var cfg domain.ProcessingConfig
	file := filepath.Join(s.ProjectsRoot, projectName, ".gisquick", "processing.json")
	content, err := os.ReadFile(file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.ProcessingConfig{Services: []domain.ProcessingService{}}, nil
		}
		return cfg, err
	}
	if err := json.Unmarshal(content, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Services == nil {
		cfg.Services = []domain.ProcessingService{}
	}
	return cfg, nil
}

func (s *DiskStorage) UpdateProcessingConfig(projectName string, cfg domain.ProcessingConfig) error {
	return s.saveConfigFile(projectName, "processing.json", cfg)
}

func (s *DiskStorage) Close() {
	s.settingsReader.Close()
	s.projectInfoReader.Close()
	s.indexCache.Stop()
	s.indexCache.DeleteAll()
}

func (s *DiskStorage) GetProjectCustomizations(projectName string) (json.RawMessage, error) {
	filename := filepath.Join(s.ProjectsRoot, projectName, "web", "app", "config.json")
	fStat, err := os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.configCache.Remove(filename)
			return nil, nil
		}
		return nil, err
	}
	updated := fStat.ModTime()
	timestamp := updated.Unix()

	config, err := s.configCache.Get(filename, timestamp)
	if err != nil {
		return nil, err
	}
	return config, nil
}
