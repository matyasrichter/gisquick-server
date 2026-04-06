package processing

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// SaveResults writes execution outputs to jobDir on disk.
// By-value outputs are written directly; by-reference outputs are downloaded.
// Returns []Artifact with metadata and relative paths (relative to jobDir).
func SaveResults(ctx context.Context, httpClient *http.Client, jobDir string, results []OutputResult, authHeaders map[string]string) ([]Artifact, error) {
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating job directory: %w", err)
	}

	var artifacts []Artifact
	for _, out := range results {
		var artifact Artifact
		var err error

		if out.Value != nil {
			artifact, err = saveByValue(jobDir, out)
		} else if out.Reference != "" {
			artifact, err = saveByReference(ctx, httpClient, jobDir, out, authHeaders)
		} else {
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("saving output %q: %w", out.OutputID, err)
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, nil
}

// saveByValue writes inline output bytes to disk.
func saveByValue(jobDir string, out OutputResult) (Artifact, error) {
	filename := outputFilename(out.OutputID, out.Filename, out.ContentType, "")
	path := filepath.Join(jobDir, filename)

	if err := os.WriteFile(path, out.Value, 0o644); err != nil {
		return Artifact{}, err
	}

	return Artifact{
		OutputID:    out.OutputID,
		Path:        filename,
		ContentType: out.ContentType,
		SizeBytes:   int64(len(out.Value)),
		MediaKind:   mediaKind(out.ContentType),
	}, nil
}

// saveByReference downloads an output from a URL and streams it to disk.
func saveByReference(ctx context.Context, httpClient *http.Client, jobDir string, out OutputResult, authHeaders map[string]string) (Artifact, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, out.Reference, nil)
	if err != nil {
		return Artifact{}, err
	}
	for k, v := range authHeaders {
		req.Header.Set(k, v)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return Artifact{}, fmt.Errorf("downloading reference: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Artifact{}, fmt.Errorf("reference download returned status %d", resp.StatusCode)
	}

	// Determine content type from response if not provided.
	contentType := out.ContentType
	if contentType == "" {
		contentType = resp.Header.Get("Content-Type")
	}

	// Determine filename: explicit hint → Content-Disposition → URL path → outputID+ext.
	filenameHint := out.Filename
	if filenameHint == "" {
		filenameHint = filenameFromContentDisposition(resp.Header.Get("Content-Disposition"))
	}
	filename := outputFilename(out.OutputID, filenameHint, contentType, out.Reference)
	path := filepath.Join(jobDir, filename)

	f, err := os.Create(path)
	if err != nil {
		return Artifact{}, err
	}
	defer f.Close()

	n, err := io.Copy(f, resp.Body)
	if err != nil {
		return Artifact{}, fmt.Errorf("writing download to disk: %w", err)
	}

	return Artifact{
		OutputID:    out.OutputID,
		Path:        filename,
		ContentType: contentType,
		SizeBytes:   n,
		MediaKind:   mediaKind(contentType),
	}, nil
}

// outputFilename derives a safe filename for an output.
// Priority: explicit hint > URL path > outputID + extension from content type.
func outputFilename(outputID, hint, contentType, referenceURL string) string {
	if hint != "" {
		return sanitizeFilename(hint)
	}
	if referenceURL != "" {
		if u, err := url.Parse(referenceURL); err == nil {
			parts := strings.Split(strings.TrimRight(u.Path, "/"), "/")
			if last := parts[len(parts)-1]; last != "" && strings.Contains(last, ".") {
				return sanitizeFilename(last)
			}
		}
	}
	ext := extensionFromMIME(contentType)
	return sanitizeFilename(outputID + ext)
}

// sanitizeFilename removes path separators to prevent directory traversal.
func sanitizeFilename(name string) string {
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, "..", "_")
	return name
}

// extensionFromMIME returns a file extension for a MIME type, e.g. ".json", ".tif".
func extensionFromMIME(contentType string) string {
	if contentType == "" {
		return ""
	}
	// Strip parameters like charset.
	mimeType, _, _ := mime.ParseMediaType(contentType)
	switch mimeType {
	case "application/json", "application/geo+json":
		return ".json"
	case "application/geopackage+sqlite3":
		return ".gpkg"
	case "image/tiff", "image/geotiff":
		return ".tif"
	case "image/png":
		return ".png"
	case "application/zip":
		return ".zip"
	case "text/plain":
		return ".txt"
	case "text/csv":
		return ".csv"
	}
	exts, _ := mime.ExtensionsByType(mimeType)
	if len(exts) > 0 {
		return exts[0]
	}
	return ""
}

// filenameFromContentDisposition extracts a filename from a Content-Disposition header.
func filenameFromContentDisposition(header string) string {
	if header == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(header)
	if err != nil {
		return ""
	}
	return params["filename"]
}

// mediaKind classifies a MIME type into broad categories for client use.
func mediaKind(contentType string) string {
	mimeType, _, _ := mime.ParseMediaType(contentType)
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		return "raster"
	case mimeType == "application/geopackage+sqlite3",
		mimeType == "application/geo+json",
		mimeType == "application/vnd.geo+json":
		return "vector"
	case mimeType == "application/json":
		return "json"
	default:
		return ""
	}
}
