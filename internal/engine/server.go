package engine

import (
	"atlas/pkg/logger"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

const (
	getEntryEndpoint    = "GET /v1/atlas"
	putEntryEndpoint    = "PUT /v1/atlas"
	deleteEntryEndpoint = "DELETE /v1/atlas"
)

type AtlasServerConfig struct {
	Engine AtlasConfig
	Port   int
}

type AtlasServer struct {
	engine *Atlas
	mux    *http.ServeMux
	config AtlasServerConfig
}

func CreateAtlasServer(config AtlasServerConfig) (*AtlasServer, error) {
	engine, err := NewAtlas(config.Engine)
	if err != nil {
		logger.Error("Failed initializing Atlas server engine: %v", err)
		return nil, err
	}

	server := &AtlasServer{engine: engine, config: config}

	server.mux = http.NewServeMux()
	server.mux.HandleFunc("GET /v1/atlas", server.handleGet)
	server.mux.HandleFunc("PUT /v1/atlas", server.handlePut)
	server.mux.HandleFunc("DELETE /v1/atlas", server.handleDelete)

	return server, nil
}

func (server *AtlasServer) Start() {
	go func() {
		port := fmt.Sprintf(":%d", server.config.Port)
		logger.Info("Starting Atlas server on %s", port)
		if err := http.ListenAndServe(port, server.mux); err != nil {
			logger.Fatal(1, "Failed starting server: %v", err)
		}
	}()

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, os.Interrupt, syscall.SIGTERM)
	<-termChan

	logger.Info("Shutting down Atlas server...")
}

func (server *AtlasServer) handleGet(response http.ResponseWriter, request *http.Request) {
	key, exists := getQueryParameter("key", getEntryEndpoint, response, request)
	if !exists {
		return
	}

	result, exists, err := server.engine.Get(key)
	if err != nil {
		logger.Error("Failed `%s`: %v", getEntryEndpoint, err)
		msg := "Internal server error"
		http.Error(response, msg, http.StatusInternalServerError)
		return
	}

	value, isAlive := result.Value()
	if !exists || !isAlive {
		response.WriteHeader(http.StatusNoContent)
		return
	}

	_, err = response.Write([]byte(value))
	if err != nil {
		logger.Error("Failed writing response in `%s`: %v", getEntryEndpoint, err)
	}
}

func (server *AtlasServer) handlePut(response http.ResponseWriter, request *http.Request) {
	key, exists := getQueryParameter("key", putEntryEndpoint, response, request)
	if !exists {
		return
	}

	value, exists := getQueryParameter("value", putEntryEndpoint, response, request)
	if !exists {
		return
	}

	err := server.engine.Insert(key, value)
	if err != nil {
		logger.Error("Failed `%s`: %v", putEntryEndpoint, err)
	}

	response.WriteHeader(http.StatusCreated)
}

func (server *AtlasServer) handleDelete(response http.ResponseWriter, request *http.Request) {
	key, exists := getQueryParameter("key", deleteEntryEndpoint, response, request)
	if !exists {
		return
	}

	err := server.engine.Delete(key)
	if err != nil {
		logger.Error("Failed `%s`: %v", deleteEntryEndpoint, err)
	}

	response.WriteHeader(http.StatusOK)
}

func getQueryParameter(
	param, url string,
	response http.ResponseWriter,
	request *http.Request,
) (value string, exists bool) {
	value = request.URL.Query().Get("key")
	if value == "" {
		logger.Warn("Malformed `%s` request - missing `%s` parameter", url, param)
		msg := fmt.Sprintf("Missing query parameter `%s`", param)
		http.Error(response, msg, http.StatusBadRequest)
		return "", false
	}
	return value, true
}
