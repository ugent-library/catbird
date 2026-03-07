package cli

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
	"github.com/ugent-library/catbird"
	"github.com/ugent-library/catbird/tui"
)

func newUICmd(cfg *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "ui",
		Short: "Start the terminal UI",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.Conn == "" {
				return errors.New("connection string required (--conn or $CB_CONN)")
			}
			pool, err := pgxpool.New(cmd.Context(), cfg.Conn)
			if err != nil {
				return err
			}
			defer pool.Close()
			return tui.Run(cmd.Context(), catbird.New(pool))
		},
	}
}
