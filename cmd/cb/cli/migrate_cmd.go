package cli

import (
	"database/sql"
	"errors"
	"fmt"
	"text/tabwriter"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"
	"github.com/spf13/cobra"
	"github.com/ugent-library/catbird"
)

func newMigrateCmd(cfg *Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Manage the Catbird database schema",
	}
	cmd.AddCommand(newMigrateUpCmd(cfg), newMigrateDownCmd(cfg), newMigrateStatusCmd(cfg))
	return cmd
}

// openDB opens a database/sql handle for goose-based migrations, which use
// *sql.DB (via the pgx stdlib adapter) rather than the pgxpool-based runtime.
func openDB(cfg *Config) (*sql.DB, error) {
	if cfg.Conn == "" {
		return nil, errors.New("connection string required (--conn or $CB_CONN)")
	}
	return sql.Open("pgx", cfg.Conn)
}

func newMigrateUpCmd(cfg *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "up",
		Short: "Apply all pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDB(cfg)
			if err != nil {
				return err
			}
			defer db.Close()
			return catbird.MigrateUpTo(cmd.Context(), db, catbird.SchemaVersion)
		},
	}
}

func newMigrateDownCmd(cfg *Config) *cobra.Command {
	var to int
	cmd := &cobra.Command{
		Use:   "down --to <version>",
		Short: "Roll back migrations down to a target version",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDB(cfg)
			if err != nil {
				return err
			}
			defer db.Close()
			return catbird.MigrateDownTo(cmd.Context(), db, to)
		},
	}
	cmd.Flags().IntVar(&to, "to", 0, "target version to roll back to (required)")
	_ = cmd.MarkFlagRequired("to")
	return cmd
}

func newMigrateStatusCmd(cfg *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show applied and pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDB(cfg)
			if err != nil {
				return err
			}
			defer db.Close()

			infos, err := catbird.MigrationStatus(cmd.Context(), db)
			if err != nil {
				return err
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "VERSION\tSTATUS\tMIGRATION")
			for _, m := range infos {
				status := "pending"
				if m.Applied {
					status = "applied"
				}
				fmt.Fprintf(tw, "%d\t%s\t%s\n", m.Version, status, m.Name)
			}
			return tw.Flush()
		},
	}
}
