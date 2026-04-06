package proxy

import (
	"bytes"
	"fmt"
	"html"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

// NewQGISReverseProxy builds a reverse proxy that forwards requests to mapserverURL,
// rewriting the request URL while preserving the original Host header so that QGIS
// Server generates OnlineResource links pointing back to the proxy, not to itself.
func NewQGISReverseProxy(mapserverURL string, log *zap.SugaredLogger) *httputil.ReverseProxy {
	target, _ := url.Parse(mapserverURL)
	rp := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.URL.Path = target.Path
			if _, ok := req.Header["User-Agent"]; !ok {
				req.Header.Set("User-Agent", "")
			}
			req.Header.Del("Cookie")
		},
	}
	rp.ErrorHandler = func(rw http.ResponseWriter, r *http.Request, e error) {
		log.Errorw("mapserver proxy error", zap.Error(e))
	}
	return rp
}

// RewriteCapabilitiesURLs returns a ModifyResponse function that rewrites QGIS Server's
// internal OnlineResource URLs in GetCapabilities responses. It reads the target path from
// the X-Ows-Url request header and replaces all xlink:href="http...MAP=..." patterns,
// stripping the MAP parameter and substituting the API proxy path.
func RewriteCapabilitiesURLs(resp *http.Response) error {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err = resp.Body.Close(); err != nil {
		return err
	}
	reg := regexp.MustCompile(`xlink:href="http[s]?://[^"]+MAP=[^"]+"`)
	owsPath := resp.Request.Header.Get("X-Ows-Url")
	doc := string(body)
	replaced := make(map[string]string, 2)
	for _, match := range reg.FindAllString(doc, -1) {
		if _, done := replaced[match]; done {
			continue
		}
		u := strings.TrimPrefix(match, `xlink:href="`)
		u = strings.TrimSuffix(u, `"`)
		parsed, _ := url.Parse(html.UnescapeString(u))
		params := parsed.Query()
		params.Del("MAP")
		parsed.Path = owsPath
		parsed.RawQuery = params.Encode()
		replaced[match] = fmt.Sprintf(`xlink:href="%s"`, html.EscapeString(parsed.String()))
		doc = strings.ReplaceAll(doc, match, replaced[match])
	}
	newBody := []byte(doc)
	resp.Body = ioutil.NopCloser(bytes.NewReader(newBody))
	resp.ContentLength = int64(len(newBody))
	resp.Header.Set("Content-Length", strconv.Itoa(len(newBody)))
	return nil
}
