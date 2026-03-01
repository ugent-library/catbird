package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/ugent-library/catbird"
	"github.com/ugent-library/catbird/dashboard"
	"github.com/ugent-library/catbird/tui"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

func main() {
	rootCmd.PersistentFlags().BoolP("interactive", "i", false, "start interactive terminal UI")
	uiCmd.Flags().String("conn", "", "PostgreSQL connection string (defaults to CB_CONN)")
	dashboardCmd.Flags().Int("port", 8080, "port to listen on")
	dashboardCmd.Flags().String("conn", "", "PostgreSQL connection string (defaults to CB_CONN)")

	rootCmd.AddCommand(uiCmd)
	rootCmd.AddCommand(dashboardCmd)

	cobra.CheckErr(rootCmd.Execute())
}

var rootCmd = &cobra.Command{
	Use:   "cb",
	Short: "Catbird CLI client",
	RunE: func(cmd *cobra.Command, args []string) error {
		interactive, err := cmd.Flags().GetBool("interactive")
		if err != nil {
			return err
		}
		if interactive {
			return runUI(cmd)
		}
		return cmd.Help()
	},
}

var uiCmd = &cobra.Command{
	Use:   "ui",
	Short: "Starts the terminal UI",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runUI(cmd)
	},
}

var dashboardCmd = &cobra.Command{
	Use:   "dashboard",
	Short: "Starts the dashboard server",
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := resolveConn(cmd)
		if err != nil {
			return err
		}

		port, err := cmd.Flags().GetInt("port")
		if err != nil {
			return err
		}

		pool, err := pgxpool.New(cmd.Context(), conn)
		if err != nil {
			return err
		}
		defer pool.Close()

		h := dashboard.New(dashboard.Config{
			Client: catbird.New(pool),
			Log:    logger,
		}).Handler()

		logger.Info("starting dashboard", "port", port)

		return http.ListenAndServe(fmt.Sprintf(":%d", port), h)
	},
}

func runUI(cmd *cobra.Command) error {
	conn, err := resolveConn(cmd)
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(cmd.Context(), conn)
	if err != nil {
		return err
	}
	defer pool.Close()

	return tui.Run(cmd.Context(), catbird.New(pool))
}

func resolveConn(cmd *cobra.Command) (string, error) {
	conn, err := cmd.Flags().GetString("conn")
	if err != nil {
		return "", err
	}
	if conn == "" {
		conn = os.Getenv("CB_CONN")
	}
	if conn == "" {
		return "", errors.New("connection string required: set --conn or CB_CONN")
	}
	return conn, nil
}
