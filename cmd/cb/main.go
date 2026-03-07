package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ugent-library/catbird/cmd/cb/cli"
)

func main() {
	if err := cli.NewRootCmd().ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
