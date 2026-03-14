package cli

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"charm.land/log/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/ugent-library/catbird"
	"github.com/ugent-library/catbird/dashboard"
)

func newDashboardCmd(cfg *Config) *cobra.Command {
	var host string
	var port int

	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Start the dashboard server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.Conn == "" {
				return errors.New("connection string required (--conn or $CB_CONN)")
			}
			pool, err := pgxpool.New(cmd.Context(), cfg.Conn)
			if err != nil {
				return err
			}
			defer pool.Close()

			logger := slog.New(log.New(cmd.OutOrStdout()))

			h := dashboard.New(dashboard.Config{
				Client: catbird.New(pool),
				Logger: logger,
			}).Handler()

			addr := fmt.Sprintf("%s:%d", host, port)

			logger.Info("starting dashboard", "addr", addr)

			return http.ListenAndServe(addr, h)
		},
	}

	cmd.Flags().StringVar(&host, "host", "localhost", "host to listen on")
	cmd.Flags().IntVar(&port, "port", 8080, "port to listen on")

	return cmd
}
