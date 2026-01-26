package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/ugent-library/catbird"
	"github.com/ugent-library/catbird/dashboard"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

func main() {
	dashboardCmd.Flags().Int("port", 8080, "port to listen on")

	rootCmd.AddCommand(dashboardCmd)

	cobra.CheckErr(rootCmd.Execute())
}

var rootCmd = &cobra.Command{
	Use:   "cb",
	Short: "Catbird CLI client",
}

var dashboardCmd = &cobra.Command{
	Use:   "dashboard",
	Short: "Starts the dashboard server",
	RunE: func(cmd *cobra.Command, args []string) error {
		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			return err
		}

		pool, err := pgxpool.New(cmd.Context(), os.Getenv("CB_CONN"))
		if err != nil {
			return err
		}

		h := dashboard.New(dashboard.Config{
			Client: catbird.New(pool),
			Log:    logger,
		}).Handler()

		logger.Info("starting dashboard", "port", port)

		return http.ListenAndServe(fmt.Sprintf(":%d", port), h)
	},
}
