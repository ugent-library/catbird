package tui

import (
	"context"

	tea "charm.land/bubbletea/v2"
	"github.com/ugent-library/catbird"
)

func Run(ctx context.Context, client *catbird.Client) error {
	p := tea.NewProgram(newModel(ctx, client), tea.WithContext(ctx))
	_, err := p.Run()
	return err
}
