package cli

import "os"

// Config holds all runtime configuration for the CLI.
// Populated from flags and environment variables in NewRootCmd;
// commands receive a *Config and do not read flags or env directly.
type Config struct {
	Conn string
}

func defaultConfig() Config {
	return Config{
		Conn: os.Getenv("CB_CONN"),
	}
}
