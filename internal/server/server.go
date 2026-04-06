package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gisquick/gisquick-server/internal/application"
	"github.com/gisquick/gisquick-server/internal/infrastructure/project"
	"github.com/gisquick/gisquick-server/internal/infrastructure/ws"
	"github.com/gisquick/gisquick-server/internal/processing"
	"github.com/gisquick/gisquick-server/internal/server/auth"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
	"github.com/labstack/echo-contrib/prometheus"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"
)

type Config struct {
	Debug                bool
	Language             string
	LandingProject       string
	MapserverURL         string
	MapCacheRoot         string
	ProjectsRoot         string
	SiteURL              string
	SecretKey            string
	SessionExpiration    time.Duration
	SignupAPI            bool
	PluginsURL           string
	MaxProjectSize        int64
	ProjectCustomization bool
	PublishRoot          string
	QGISPluginSecret     string
}

var extensions = make(map[string]func(s *Server) error, 0)

type Middlewares struct {
	LoginRequired          echo.MiddlewareFunc
	SuperuserRequired      echo.MiddlewareFunc
	ProjectAdminAccess     echo.MiddlewareFunc
	ProjectSuperuserAccess echo.MiddlewareFunc
	ProjectAccess          echo.MiddlewareFunc
	ProjectAccessOWS       echo.MiddlewareFunc
}

type Server struct {
	Config Config
	echo   *echo.Echo
	log    *zap.SugaredLogger
	// Logger          echo.Logger
	auth              *auth.AuthService
	accountsService   *application.AccountsService
	projects          application.ProjectService
	notifications     *project.RedisNotificationStore
	middlewares       Middlewares
	sws               *ws.SettingsWS
	limiter           application.AccountsLimiter
	shutdownCallbacks []func()
	db                *sqlx.DB
	jobStore          processing.JobStore
}

type JSONSerializer struct{}

// Serialize converts an interface into a json and writes it to the response.
// You can optionally use the indent parameter to produce pretty JSONs.
func (d JSONSerializer) Serialize(c echo.Context, i interface{}, indent string) error {
	enc := jsoniter.NewEncoder(c.Response())
	if indent != "" {
		enc.SetIndent("", indent)
	}
	return enc.Encode(i)
}

// func (d JSONSerializer) Serialize(c echo.Context, i interface{}, indent string) error {
// 	return oj.Write(c.Response(), i, len(indent))
// }

// Deserialize reads a JSON from a request body and converts it into an interface.
func (d JSONSerializer) Deserialize(c echo.Context, i interface{}) error {
	err := jsoniter.NewDecoder(c.Request().Body).Decode(i)
	if ute, ok := err.(*json.UnmarshalTypeError); ok {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Unmarshal type error: expected=%v, got=%v, field=%v, offset=%v", ute.Type, ute.Value, ute.Field, ute.Offset)).SetInternal(err)
	} else if se, ok := err.(*json.SyntaxError); ok {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Syntax error: offset=%v, error=%v", se.Offset, se.Error())).SetInternal(err)
	}
	return err
}

func NewServer(log *zap.SugaredLogger, cfg Config, db *sqlx.DB,
	as *auth.AuthService, signUpService *application.AccountsService, projects application.ProjectService,
	sws *ws.SettingsWS, limiter application.AccountsLimiter, notifications *project.RedisNotificationStore,
	jobStore processing.JobStore) *Server {
	e := echo.New()
	e.HideBanner = true

	p := prometheus.NewPrometheus("api", nil)
	p.Use(e)

	// e.JSONSerializer = &JSONSerializer{}
	e.HTTPErrorHandler = func(err error, c echo.Context) {
		e.DefaultHTTPErrorHandler(err, c)
		code := http.StatusInternalServerError
		if he, ok := err.(*echo.HTTPError); ok {
			code = he.Code
		}
		if code == http.StatusInternalServerError {
			log.Error(err)
		}
	}
	middlewares := Middlewares{
		LoginRequired:          LoginRequiredMiddlewareWithConfig(as),
		SuperuserRequired:      SuperuserAccessMiddleware(as),
		ProjectAdminAccess:     ProjectAdminAccessMiddleware(as, projects),
		ProjectSuperuserAccess: ProjectSuperuserAccessMiddleware(as, projects),
		ProjectAccess:          ProjectAccessMiddleware(as, projects, ""),
		ProjectAccessOWS:       ProjectAccessMiddleware(as, projects, "basic realm=Restricted"),
	}

	e.Pre(middleware.RemoveTrailingSlash())
	e.Use(
		middleware.Recover(),
		// middleware.Logger(),
		middleware.CSRFWithConfig(middleware.CSRFConfig{
			TokenLookup: "header:X-CSRF-Token",
			CookieName:  "csrftoken",
			Skipper: func(c echo.Context) bool {
				// for n, v := range c.Request().Header {
				// 	fmt.Println(n, v)
				// }
				// client := c.Request().Header.Get("X-Requested-By")
				return true
				// return c.Request().Header.Get("Cookie") == ""
				// return client == "gisquick-client" && c.Request().Header.Get("Cookie") == ""
			},
		}),
		// SessionMiddlewareWithConfig(as.rdb),
	)
	s := &Server{
		Config:          cfg,
		log:             log,
		echo:            e,
		auth:            as,
		db:              db,
		accountsService: signUpService,
		projects:        projects,
		middlewares:     middlewares,
		sws:             sws,
		limiter:         limiter,
		notifications:   notifications,
		jobStore:        jobStore,
	}

	// e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	s.AddRoutes(e)
	AddAliasAPI(s)
	return s
}

func (s *Server) ListenAndServe(addr string) error {
	return s.echo.Start(addr)
}

func (s *Server) OnShutdown(fn func()) {
	s.shutdownCallbacks = append(s.shutdownCallbacks, fn)
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.projects.Close()
	for _, fn := range s.shutdownCallbacks {
		fn()
	}
	return s.echo.Shutdown(ctx)
}

func (s *Server) AddExtension(name string) error {
	extension, registred := extensions[name]
	if !registred {
		return fmt.Errorf("unknown server extension: %s", name)
	}
	return extension(s)
}
