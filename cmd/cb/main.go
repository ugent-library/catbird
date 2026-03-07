package main

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/ugent-library/catbird/cmd/cb/cli"
)

func main() {
	cobra.CheckErr(cli.NewRootCmd().ExecuteContext(context.Background()))
}
