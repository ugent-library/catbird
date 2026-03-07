package cli

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

func NewRootCmd() *cobra.Command {
	cfg := defaultConfig()

	root := &cobra.Command{
		Use:           "cb",
		Short:         "Catbird CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	root.PersistentFlags().StringVar(&cfg.Conn, "conn", cfg.Conn, "PostgreSQL connection string [$CB_CONN]")

	root.AddCommand(newUICmd(&cfg))
	root.AddCommand(newDashboardCmd(&cfg))

	return root
}
