package server

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gisquick/gisquick-server/internal/domain"
	"github.com/gisquick/gisquick-server/internal/infrastructure/proxy"
	"github.com/labstack/echo/v4"
)

type GetFeature struct {
	XMLName xml.Name `xml:"GetFeature"`
	Query   []Query  `xml:"Query"`
}

type Query struct {
	XMLName    xml.Name       `xml:"Query"`
	TypeName   string         `xml:"typeName,attr"`
	Properties []PropertyName `xml:"ogc:PropertyName"`
	Contents   []AnyTag       `xml:",any"`
}

type PropertyName struct {
	XMLName xml.Name `xml:"ogc:PropertyName"`
	Name    string   `xml:",chardata"`
}

type AnyTag struct {
	XMLName xml.Name
	Content string `xml:",innerxml"`
}

type Transaction struct {
	XMLName xml.Name `xml:"Transaction"`
	Updates []Update `xml:"Update"`
	Inserts []Insert `xml:"Insert"`
	Deletes []Delete `xml:"Delete"`
}

type Update struct {
	XMLName    xml.Name   `xml:"Update"`
	TypeName   string     `xml:"typeName,attr"`
	Properties []Property `xml:"Property"`
}

type InsertObject struct {
	XMLName    xml.Name
	Properties []InsertProperty `xml:",any"`
}
type InsertProperty struct {
	XMLName xml.Name
	// Content string `xml:",innerxml"`
}

type Insert struct {
	XMLName xml.Name       `xml:"Insert"`
	Objects []InsertObject `xml:",any"`
}

type Delete struct {
	XMLName  xml.Name `xml:"Delete"`
	TypeName string   `xml:"typeName,attr"`
}

type Property struct {
	XMLName xml.Name `xml:"Property"`
	Name    string   `xml:"Name"`
	Value   string   `xml:"Value"`
}

type OwsRequestParams struct {
	Map     string `query:"map"`
	Service string `query:"service"`
	Request string `query:"request"`
	Layers  string `query:"layers"`
}

type OwsGetFeatureRequestParams struct {
	TypeName     string `query:"TYPENAME"`
	PropertyName string `query:"PROPERTYNAME"`
	FeatureID    string `query:"FEATUREID"`
}

func parseTypeName(typeName string) (string, error) {
	parts := strings.Split(typeName, ":")
	if len(parts) != 2 {
		return "", fmt.Errorf("Invalid typeName: %s", typeName)
	}
	return parts[1], nil
}

func replaceQueryParam(query url.Values, name, value string) {
	for param := range query {
		if strings.EqualFold(param, name) {
			query.Del(param)
		}
	}
	query.Set(name, value)
}

func (s *Server) handleMapOws() func(c echo.Context) error {
	/*
		director := func(req *http.Request) {
			target, _ := url.Parse(s.Config.MapserverURL)
			query := req.URL.Query()
			mapParam := req.URL.Query().Get("MAP")
			query.Set("MAP", filepath.Join("/publish", mapParam))
			req.URL.RawQuery = query.Encode()
			req.URL.Path = target.Path
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host

			if _, ok := req.Header["User-Agent"]; !ok {
				// explicitly disable User-Agent so it's not set to default value
				req.Header.Set("User-Agent", "")
			}
		}
	*/
	director := func(req *http.Request) {
		target, _ := url.Parse(s.Config.MapserverURL)
		// s.log.Debugw("Map proxy", "query", req.URL.RawQuery)
		req.URL.Path = target.Path
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host

		if _, ok := req.Header["User-Agent"]; !ok {
			// explicitly disable User-Agent so it's not set to default value
			req.Header.Set("User-Agent", "")
		}
		req.Header.Del("Cookie")
	}
	reverseProxy := &httputil.ReverseProxy{Director: director}
	capabilitiesProxy := &httputil.ReverseProxy{Director: director}
	capabilitiesProxy.ModifyResponse = proxy.RewriteCapabilitiesURLs

	return func(c echo.Context) error {
		params := new(OwsRequestParams)
		if err := (&echo.DefaultBinder{}).BindQueryParams(c, params); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid query parameters")
		}

		projectName := getProjectName(c)
		pInfo, err := s.projects.GetProjectInfo(projectName)
		if err != nil {
			if errors.Is(err, domain.ErrProjectNotExists) {
				return echo.ErrNotFound
			}
			return fmt.Errorf("reading project info: %w", err)
		}

		req := c.Request()
		// Set MAP parameter
		owsProject := filepath.Join("/publish", projectName, pInfo.QgisFile)
		query := req.URL.Query()
		query.Set("MAP", owsProject)

		if params.Service == "WMS" && strings.EqualFold(params.Request, "GetCapabilities") {
			req.Header.Set("X-Ows-Url", req.URL.Path)
			req.URL.RawQuery = query.Encode()
			capabilitiesProxy.ServeHTTP(c.Response(), req)
			return nil
		}

		settings, err := s.projects.GetSettings(projectName)
		if err != nil {
			return fmt.Errorf("getting project settings: %w", err)
		}
		if len(settings.Auth.Roles) > 0 {
			user, err := s.auth.GetUser(c)
			layersPermFlags := make(map[string]domain.Flags)
			layersData, err := s.projects.GetLayersData(projectName)
			if err != nil {
				return fmt.Errorf("getting layer data: %w", err)
			}
			getLayerId := func(typeName string) string {
				parts := strings.Split(typeName, ":")
				lname := parts[len(parts)-1]
				id, _ := layersData.LayerNameToID[lname]
				return id
			}
			getLayerPermissions := func(typeName string) domain.Flags {
				id := getLayerId(typeName)
				flags, ok := layersPermFlags[id]
				if !ok {
					flags = settings.UserLayerPermissionsFlags(user, id)
					layersPermFlags[id] = flags
				}
				return flags
			}
			if params.Service == "WMS" && strings.EqualFold(params.Request, "GetMap") && params.Layers != "" {
				for _, lname := range strings.Split(params.Layers, ",") {
					if !getLayerPermissions(lname).Has("view") {
						return echo.ErrForbidden
					}
				}
			}
			if params.Service == "WFS" {
				layersAttrsFlags := make(map[string]map[string]domain.Flags)
				getLayerAttributesFlags := func(typeName string) map[string]domain.Flags {
					id := getLayerId(typeName)
					attrsFlags, ok := layersAttrsFlags[id]
					if !ok {
						attrsFlags = settings.UserLayerAttrinutesFlags(user, id)
						geomAttrs, ok := attrsFlags["geometry"]
						if ok {
							attrsFlags["geometry"] = geomAttrs.Union([]string{"view"})
						} else {
							// for backward compatibility
							attrsFlags["geometry"] = []string{"view", "edit"}
						}
					}
					return attrsFlags
				}

				if params.Request == "" && req.Method == "POST" { // GetFeature Insert/Update/Delete
					var wfsTransaction Transaction
					// read all bytes from content body and create new stream using it.
					bodyBytes, _ := ioutil.ReadAll(req.Body)
					req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
					if err := xml.Unmarshal(bodyBytes, &wfsTransaction); err != nil {
						return err
					}
					for _, u := range wfsTransaction.Updates {
						if !getLayerPermissions(u.TypeName).Has("update") {
							return echo.ErrForbidden
						}
						attrsFlags := getLayerAttributesFlags(u.TypeName)
						for _, p := range u.Properties {
							if !attrsFlags[p.Name].Has("edit") {
								return echo.ErrForbidden
							}
						}
					}
					for _, i := range wfsTransaction.Inserts {
						for _, o := range i.Objects {
							if !getLayerPermissions(o.XMLName.Local).Has("insert") {
								return echo.ErrForbidden
							}
							attrsFlags := getLayerAttributesFlags(o.XMLName.Local)
							for _, p := range o.Properties {
								if !attrsFlags[p.XMLName.Local].Has("edit") {
									return echo.ErrForbidden
								}
							}
						}
					}
					for _, d := range wfsTransaction.Deletes {
						if !getLayerPermissions(d.TypeName).Has("delete") {
							return echo.ErrForbidden
						}
					}
				} else if strings.EqualFold(params.Request, "GetFeature") {
					if req.Method == "POST" {
						bodyBytes, _ := ioutil.ReadAll(req.Body)
						var getFeature GetFeature
						if err := xml.Unmarshal(bodyBytes, &getFeature); err != nil {
							return err
						}
						bodyModified := false
						for i, q := range getFeature.Query {
							if !getLayerPermissions(q.TypeName).Has("query") {
								return echo.ErrForbidden
							}
							attrsFlags := getLayerAttributesFlags(q.TypeName)
							// Note: at least one valid non-geometry field must be specified, otherwise qgis server will return all fields
							if len(q.Properties) > 0 {
								for _, p := range q.Properties {
									aFlags, exist := attrsFlags[p.Name]
									if !exist || !aFlags.Has("view") {
										return echo.ErrForbidden
									}
								}
								if len(q.Properties) == 1 && q.Properties[0].Name == "geometry" {
									return echo.ErrForbidden
								}
							} else {
								var properties []PropertyName
								for name, flags := range attrsFlags {
									if flags.Has("view") {
										properties = append(properties, PropertyName{Name: name})
									}
								}
								if len(properties) == 0 || (len(properties) == 1 && properties[0].Name == "geometry") {
									return echo.ErrForbidden
								}
								getFeature.Query[i].Properties = properties
								bodyModified = true
							}
						}
						if bodyModified {
							newData, err := xml.Marshal(getFeature)
							if err != nil {
								return fmt.Errorf("transforming GetFeature request: %w", err)
							}
							req.Body = ioutil.NopCloser(bytes.NewBuffer(newData))
							newSize := len(newData)
							req.Header.Set("Content-Length", strconv.Itoa(newSize))
							req.ContentLength = int64(newSize)
						} else {
							req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
						}
					} else {
						getFeatureParams := new(OwsGetFeatureRequestParams)
						if err := (&echo.DefaultBinder{}).BindQueryParams(c, getFeatureParams); err != nil {
							return echo.NewHTTPError(http.StatusBadRequest, "Invalid GetFeature query parameters")
						}
						// note: no support for multiple layers
						layername := getFeatureParams.TypeName
						if layername == "" {
							layername = strings.SplitN(getFeatureParams.FeatureID, ".", 2)[0]
						}
						if layername == "" {
							return echo.ErrBadRequest
						}
						if !getLayerPermissions(layername).Has("query") {
							return echo.ErrForbidden
						}
						attrsFlags := getLayerAttributesFlags(layername)
						if getFeatureParams.PropertyName != "" {
							properties := strings.Split(getFeatureParams.PropertyName, ",")
							for _, pName := range properties {
								aFlags, exist := attrsFlags[pName]
								if !exist || !aFlags.Has("view") {
									return echo.ErrForbidden
								}
							}
							if len(properties) == 1 && properties[0] == "geometry" {
								return echo.ErrForbidden
							}
						} else {
							var properties []string
							for name, flags := range attrsFlags {
								if flags.Has("view") {
									properties = append(properties, name)
								}
							}
							if len(properties) == 0 || (len(properties) == 1 && properties[0] == "geometry") {
								return echo.ErrForbidden
							}
							replaceQueryParam(query, "PROPERTYNAME", strings.Join(properties, ","))
						}
					}
				}
			}
		}
		req.URL.RawQuery = query.Encode()
		reverseProxy.ServeHTTP(c.Response(), req)
		return nil
	}
}
