package main

import (
	"fmt"

	"atlas/pkg/log"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:  "compctl",
	Long: "Compass - the CLI for interacting with the Atlas orchestrator.",
	Run: func(cmd *cobra.Command, args []string) {
		// log.Info("running `compctl` with: %+v and args %v", cmd, args)
		log.Info("bueno")
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal("running rootCmd failed: %v", err)
	}

	fmt.Printf("hello world\n")
}
