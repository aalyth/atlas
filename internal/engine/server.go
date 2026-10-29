package engine

import (
	"atlas/internal/data"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

type AtlasServerConfig struct {
	Engine AtlasConfig
	Port   int
}

type AtlasServer struct {
	engine *Atlas
	router *chi.Mux
	config AtlasServerConfig
	log    *zap.Logger
}

const maxRequestBodyBytes = int64(data.MaxEntrySize)

func NewAtlasServer(log *zap.Logger, config AtlasServerConfig) (*AtlasServer, error) {
	engineLog := log.With(zap.String("component", "engine"))
	engine, err := NewAtlas(engineLog, config.Engine)
	if err != nil {
		log.Error("initialize engine failed", zap.Error(err))
		return nil, err
	}

	server := &AtlasServer{
		engine: engine,
		config: config,
		log:    log,
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(server.requestLogger)

	r.Get("/healthz", server.handleHealth)
	r.Route("/v1/atlas/{key}", func(r chi.Router) {
		r.Get("/", server.handleGet)
		r.Put("/", server.handlePut)
		r.Delete("/", server.handleDelete)
	})

	server.router = r
	return server, nil
}

func (server *AtlasServer) Start() {
	addr := fmt.Sprintf(":%d", server.config.Port)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		server.log.Info("starting Atlas server", zap.String("addr", addr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			server.log.Fatal("server failed", zap.Error(err))
		}
	}()

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, os.Interrupt, syscall.SIGTERM)
	<-termChan

	server.log.Info("shutting down Atlas server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		server.log.Error("server shutdown failed", zap.Error(err))
	}
}

func (server *AtlasServer) handleHealth(response http.ResponseWriter, _ *http.Request) {
	response.WriteHeader(http.StatusOK)
	_, _ = response.Write([]byte("ok"))
}

func (server *AtlasServer) handleGet(response http.ResponseWriter, request *http.Request) {
	key := chi.URLParam(request, "key")

	result, exists, err := server.engine.Get(key)
	if err != nil {
		server.log.Error("get failed", zap.String("key", key), zap.Error(err))
		http.Error(response, "Internal server error", http.StatusInternalServerError)
		return
	}

	if !exists {
		response.WriteHeader(http.StatusNoContent)
		return
	}

	value, isAlive := result.Value()
	if !isAlive {
		response.WriteHeader(http.StatusNoContent)
		return
	}

	if _, err := response.Write([]byte(value)); err != nil {
		server.log.Error("write response failed", zap.String("key", key), zap.Error(err))
	}
}

func (server *AtlasServer) handlePut(response http.ResponseWriter, request *http.Request) {
	key := chi.URLParam(request, "key")

	body := http.MaxBytesReader(response, request.Body, maxRequestBodyBytes)
	defer body.Close()

	value, err := io.ReadAll(body)
	if err != nil {
		server.log.Warn("read body failed", zap.String("key", key), zap.Error(err))
		http.Error(response, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := server.engine.Insert(key, string(value)); err != nil {
		if isValidationError(err) {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		server.log.Error("insert failed", zap.String("key", key), zap.Error(err))
		http.Error(response, "Internal server error", http.StatusInternalServerError)
		return
	}

	response.WriteHeader(http.StatusCreated)
}

func (server *AtlasServer) handleDelete(response http.ResponseWriter, request *http.Request) {
	key := chi.URLParam(request, "key")

	if err := server.engine.Delete(key); err != nil {
		if isValidationError(err) {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		server.log.Error("delete failed", zap.String("key", key), zap.Error(err))
		http.Error(response, "Internal server error", http.StatusInternalServerError)
		return
	}

	response.WriteHeader(http.StatusOK)
}

func (server *AtlasServer) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		server.log.Info("http request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", ww.Status()),
			zap.Int("bytes", ww.BytesWritten()),
			zap.Duration("latency", time.Since(start)),
			zap.String("request_id", middleware.GetReqID(r.Context())),
		)
	})
}

func isValidationError(err error) bool {
	return errors.Is(err, data.ErrEmptyKey) ||
		errors.Is(err, data.ErrEntryTooLarge) ||
		errors.Is(err, data.ErrInvalidKey) ||
		errors.Is(err, data.ErrInvalidValue)
}
