/*
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"syscall"

	"github.com/aalyth/atlas/pkg/log"
)

type CreateContainerBody struct {
	Command string   `json:"command"`
	Args    []string `json:"args"`
}

func main() {
	if !isRoot() {
		log.Fatal("atlasd must be ran as root")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /create_container", postCreateContainer)

	log.Info("running server on %s", httpAddr)
	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Fatal("could not start http server on %s: %v", httpAddr, err)
	}
}

func postCreateContainer(w http.ResponseWriter, r *http.Request) {
	if _, err := exec.LookPath("compass"); err != nil {
		log.Error("could not find the executable `compass` in $PATH")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	bodyRaw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "could not parse body", http.StatusBadRequest)
		return
	}

	var body CreateContainerBody
	if err := json.Unmarshal(bodyRaw, &body); err != nil {
		log.Warn("invalid request body json: %v", err)
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	compassArgs := append([]string{"run", body.Command}, body.Args...)
	log.Info("compass args: %v", compassArgs)

	cmd := exec.Command("compass", compassArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Error("failed to run container (`%s`) with compass: %v")
		http.Error(w,
			fmt.Sprintf("failed to run container: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
}

func isRoot() bool {
	return syscall.Geteuid() == 0
}

func must(err error, msg string) {
	if err != nil {
		log.Warn("error %s: %v", msg, err)
	}
}
*/

package main

import (
	"context"
	"log"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
)

const (
	atlasRoot = "/var/lib/atlas"
	httpAddr  = ":3000"
)

func main() {
	if err := redisExample(); err != nil {
		log.Fatal(err)
	}
}

func redisExample() error {
	client, err := containerd.New("/run/containerd/containerd.sock")
	if err != nil {
		return err
	}
	defer client.Close()

	_ = namespaces.WithNamespace(context.Background(), "atlas")

	return nil
}
