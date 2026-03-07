package cli

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/ugent-library/catbird"
	"github.com/ugent-library/catbird/dashboard"
)

func newDashboardCmd(cfg *Config) *cobra.Command {
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

			h := dashboard.New(dashboard.Config{
				Client: catbird.New(pool),
				Logger: logger,
			}).Handler()

			logger.Info("starting dashboard", "port", port)

			return http.ListenAndServe(fmt.Sprintf(":%d", port), h)
		},
	}

	cmd.Flags().IntVar(&port, "port", 8080, "port to listen on")

	return cmd
}
