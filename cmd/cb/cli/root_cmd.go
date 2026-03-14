package cli

import (
	"github.com/spf13/cobra"
)

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
