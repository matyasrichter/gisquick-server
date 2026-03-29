package commands

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ardanlabs/conf/v2"
	"github.com/gisquick/gisquick-server/internal/application"
	"github.com/gisquick/gisquick-server/internal/domain"
	"github.com/gisquick/gisquick-server/internal/infrastructure/email"
	"github.com/gisquick/gisquick-server/internal/infrastructure/postgres"
	"github.com/gisquick/gisquick-server/internal/infrastructure/project"
	"github.com/gisquick/gisquick-server/internal/infrastructure/security"
	"github.com/gisquick/gisquick-server/internal/infrastructure/ws"
	"github.com/gisquick/gisquick-server/internal/processing"
	"github.com/gisquick/gisquick-server/internal/server"
	"github.com/gisquick/gisquick-server/internal/server/auth"
	"github.com/go-redis/redis/v8"
	mail "github.com/xhit/go-simple-mail/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func parseByteSize(value string) (int64, error) {
	value = strings.TrimSpace(value)
	factor := 1
	if strings.HasSuffix(value, "M") {
		factor = 1024 * 1024
	} else if strings.HasSuffix(value, "G") {
		factor = 1024 * 1024 * 1024
	}
	num, err := strconv.Atoi(strings.TrimRight(value, "MGB"))
	if err != nil {
		return -1, fmt.Errorf("Invalid byte size: %s", value)
	}
	return int64(num * factor), nil
}

type ByteSize int64

// Satisfy the flag package Value interface.
func (b *ByteSize) Set(s string) error {
	bs, err := parseByteSize(s)
	if err != nil {
		return err
	}
	*b = ByteSize(bs)
	return nil
}

// Satisfy the encoding.TextUnmarshaler interface.
func (b *ByteSize) UnmarshalText(text []byte) error {
	return b.Set(string(text))
}

func Serve() error {
	cfg := struct {
		Gisquick struct {
			Debug                bool   `conf:"default:false"`
			Language             string `conf:"default:en-us"`
			ProjectsRoot         string `conf:"default:/publish"`
			MapCacheRoot         string
			MapserverURL         string
			PluginsURL           string
			SignupAPI            bool
			ProjectSizeLimit     ByteSize `conf:"default:-1"`
			AccountStorageLimit  ByteSize `conf:"default:-1"`
			AccountProjectsLimit int      `conf:"default:-1"`
			AccountLimiterConfig string
			LandingProject       string
			ProjectCustomization bool
			Extensions           string
		}
		Auth struct {
			SessionExpiration    time.Duration `conf:"default:24h"`
			EmailTokenExpiration time.Duration `conf:"default:72h"`
			SecretKey            string        `conf:"default:secret-key,mask"`
		}
		Web struct {
			ReadTimeout     time.Duration `conf:"default:5s"`
			WriteTimeout    time.Duration `conf:"default:10s"`
			IdleTimeout     time.Duration `conf:"default:120s"`
			ShutdownTimeout time.Duration `conf:"default:20s"`
			SiteURL         string        `conf:"default:http://localhost"`
			APIHost         string        `conf:"default:0.0.0.0:3000"`
		}
		Postgres struct {
			User               string `conf:"default:postgres"`
			Password           string `conf:"default:postgres,mask"`
			Host               string `conf:"default:postgres"`
			Name               string `conf:"default:postgres,env:POSTGRES_DB"`
			Port               int    `conf:"default:5432"`
			MaxIdleConns       int    `conf:"default:3"`
			MaxOpenConns       int    `conf:"default:3"`
			SSLMode            string `conf:"default:disable"`
			StatementCacheMode string `conf:"default:prepare"`
		}
		Redis struct {
			Addr     string `conf:"default:redis:6379"` // "/var/run/redis/redis.sock"
			Network  string // "unix"
			Password string `conf:"mask"`
			DB       int    `conf:"default:0"`
		}
		Email struct {
			Host                 string
			Port                 int    `conf:"default:465"`
			Encryption           string `conf:"default:SSL,help: Options [None|SSL|TLS|SSLTLS|STARTTLS]"`
			Username             string
			Password             string `conf:"mask"`
			Sender               string
			ActivationSubject    string `conf:"default:Gisquick Registration"`
			PasswordResetSubject string `conf:"default:Gisquick Password Reset"`
		}
		ProcessingProxy struct {
			SharedSecret string `conf:"env:PROCESSING_PROXY_SHARED_SECRET,mask"`
		}
	}{}

	// const prefix = "GISQUICK"
	const prefix = ""
	help, err := conf.Parse(prefix, &cfg)
	if err != nil {
		if errors.Is(err, conf.ErrHelpWanted) {
			fmt.Println(help)
			return nil
		}
		return fmt.Errorf("parsing config: %w", err)
	}
	logLevel := zap.InfoLevel
	if cfg.Gisquick.Debug {
		logLevel = zap.DebugLevel
	}
	log, err := createLogger(logLevel)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return fmt.Errorf("generating config for output: %w", err)
	}
	// fmt.Println(out)
	log.Infow("startup", "config", out)

	// Database
	dbConn, err := server.OpenDB(server.DBConfig{
		User:               cfg.Postgres.User,
		Password:           cfg.Postgres.Password,
		Host:               cfg.Postgres.Host,
		Name:               cfg.Postgres.Name,
		Port:               cfg.Postgres.Port,
		MaxIdleConns:       cfg.Postgres.MaxIdleConns,
		MaxOpenConns:       cfg.Postgres.MaxOpenConns,
		SSLMode:            cfg.Postgres.SSLMode,
		StatementCacheMode: cfg.Postgres.StatementCacheMode,
	})
	if err != nil {
		return fmt.Errorf("connecting to db: %w", err)
	}
	defer func() {
		// log.Infow("shutdown", "status", "stopping database support", "host", cfg.Postgres.Host)
		dbConn.Close()
	}()

	// for unix socket, use Network: "unix" and Addr: "/var/run/redis/redis.sock"
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Network:  cfg.Redis.Network,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer rdb.Close()

	var es email.EmailService
	encryptionMap := map[string]mail.Encryption{
		"None":     mail.EncryptionNone,
		"SSL":      mail.EncryptionSSL,
		"TLS":      mail.EncryptionTLS,
		"SSLTLS":   mail.EncryptionSSLTLS,
		"STARTTLS": mail.EncryptionSTARTTLS,
	}
	encryption, ok := encryptionMap[cfg.Email.Encryption]
	if !ok {
		encryption = mail.EncryptionNone
	}
	if cfg.Email.Host != "" {
		es = &email.SmtpEmailService{
			Host:       cfg.Email.Host,
			Port:       cfg.Email.Port,
			Encryption: encryption,
			Username:   cfg.Email.Username,
			Password:   cfg.Email.Password,
		}
	}

	notifications := project.NewRedisNotificationStore(log, rdb)
	jobStore := processing.NewRedisJobStore(rdb)

	conf := server.Config{
		Language:             cfg.Gisquick.Language,
		LandingProject:       cfg.Gisquick.LandingProject,
		MapserverURL:         cfg.Gisquick.MapserverURL,
		MapCacheRoot:         cfg.Gisquick.MapCacheRoot,
		ProjectsRoot:         cfg.Gisquick.ProjectsRoot,
		PluginsURL:           cfg.Gisquick.PluginsURL,
		SignupAPI:            cfg.Gisquick.SignupAPI,
		SiteURL:              cfg.Web.SiteURL,
		MaxProjectSize:       int64(cfg.Gisquick.ProjectSizeLimit),
		ProjectCustomization:  cfg.Gisquick.ProjectCustomization,
		ProcessingProxySecret: cfg.ProcessingProxy.SharedSecret,
	}

	// Services
	accountsRepo := postgres.NewAccountsRepository(dbConn)
	tokenGenerator := security.NewTokenGenerator(cfg.Auth.SecretKey, "signup", cfg.Auth.EmailTokenExpiration)
	emailSender := email.NewAccountsEmailSender(
		es,
		cfg.Email.Sender,
		cfg.Web.SiteURL,
		cfg.Email.ActivationSubject,
		cfg.Email.PasswordResetSubject,
	)
	accountsService := application.NewAccountsService(emailSender, accountsRepo, tokenGenerator)

	sessionStore := auth.NewRedisStore(rdb)
	authServ := auth.NewAuthService(log, cfg.Auth.SessionExpiration, accountsRepo, sessionStore)

	projectsRepo := project.NewDiskStorage(log, cfg.Gisquick.ProjectsRoot)
	defaultAccountConfig := domain.AccountConfig{
		ProjectsCountLimit: cfg.Gisquick.AccountProjectsLimit,
		ProjectSizeLimit:   domain.ByteSize(cfg.Gisquick.ProjectSizeLimit),
		StorageLimit:       domain.ByteSize(cfg.Gisquick.AccountStorageLimit),
	}
	var limiter application.AccountsLimiter
	if cfg.Gisquick.AccountLimiterConfig != "" {
		limiter = project.NewConfigurableProjectsLimiter(log, cfg.Gisquick.AccountLimiterConfig, defaultAccountConfig)
	} else {
		limiter = project.NewSimpleProjectsLimiter(defaultAccountConfig)
	}
	projectsServ := application.NewProjectsService(log, projectsRepo, limiter)

	sws := ws.NewSettingsWS(log)
	s := server.NewServer(log, conf, dbConn, authServ, accountsService, projectsServ, sws, limiter, notifications, jobStore)

	if cfg.Gisquick.Extensions != "" {
		extensionsList := strings.Split(cfg.Gisquick.Extensions, ",")
		for _, e := range extensionsList {
			if err := s.AddExtension(e); err != nil {
				log.Errorw("adding server extension", "name", e, zap.Error(err))
			}
		}
	}

	// Start server
	go func() {
		if err := s.ListenAndServe(cfg.Web.APIHost); err != nil && err != http.ErrServerClosed {
			log.Fatalf("shutting down the server: %v", err)
		}
	}()
	// Wait for interrupt signal to gracefully shutdown the server with a timeout of 10 seconds.
	// Use a buffered channel to avoid missing signals as recommended for signal.Notify
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Infof("Received shutdown signal")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
	log.Sync()
	return nil
}

func createLogger(level zapcore.Level) (*zap.SugaredLogger, error) {
	config := zap.NewProductionConfig()
	// config := zap.NewDevelopmentConfig()

	// config.OutputPaths = []string{"stdout"}
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.DisableStacktrace = true
	config.Level.SetLevel(level)

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}
	defer logger.Sync()
	log := logger.Sugar()
	return log, nil
}
