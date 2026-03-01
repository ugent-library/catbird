package tui

import (
	"strings"

	lipgloss "charm.land/lipgloss/v2"
)

type uiStyles struct {
	app          lipgloss.Style
	header       lipgloss.Style
	logo         lipgloss.Style
	title        lipgloss.Style
	tabs         lipgloss.Style
	tabActive    lipgloss.Style
	tabInactive  lipgloss.Style
	helpPanel    lipgloss.Style
	errorPanel   lipgloss.Style
	body         lipgloss.Style
	bodyText     lipgloss.Style
	bodySelected lipgloss.Style
	footer       lipgloss.Style
}

var theme = newUIStyles(uiCanColor())

func newUIStyles(colorEnabled bool) uiStyles {
	if !colorEnabled {
		return uiStyles{
			app:          lipgloss.NewStyle(),
			header:       lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1),
			logo:         lipgloss.NewStyle().Bold(true),
			title:        lipgloss.NewStyle().Bold(true),
			tabs:         lipgloss.NewStyle(),
			tabActive:    lipgloss.NewStyle().Bold(true),
			tabInactive:  lipgloss.NewStyle(),
			helpPanel:    lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1),
			errorPanel:   lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1),
			body:         lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1),
			bodyText:     lipgloss.NewStyle(),
			bodySelected: lipgloss.NewStyle().Bold(true),
			footer:       lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1),
		}
	}

	const (
		bg     = "#1a1a2e"
		panel  = "#252540"
		border = "#3b3b5c"
		text   = "#ececf5"
		muted  = "#c9c9d8"
		title  = "#f0f0f0"
		green  = "#00B05B"
		purple = "#9747FF"
	)

	return uiStyles{
		app: lipgloss.NewStyle().
			Background(lipgloss.Color(bg)).
			Foreground(lipgloss.Color(text)),
		header: lipgloss.NewStyle().
			Background(lipgloss.Color(panel)).
			Foreground(lipgloss.Color(title)).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(border)).
			Padding(0, 1),
		logo: lipgloss.NewStyle().
			Foreground(lipgloss.Color(green)).
			Bold(true),
		title: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color(green)),
		tabs: lipgloss.NewStyle().
			Foreground(lipgloss.Color(muted)),
		tabActive: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color(purple)),
		tabInactive: lipgloss.NewStyle().
			Foreground(lipgloss.Color(muted)),
		helpPanel: lipgloss.NewStyle().
			Background(lipgloss.Color(panel)).
			Foreground(lipgloss.Color(text)).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(border)).
			Padding(0, 1),
		errorPanel: lipgloss.NewStyle().
			Background(lipgloss.Color(panel)).
			Foreground(lipgloss.Color("#ffb3b3")).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#8a2f2f")).
			Padding(0, 1),
		body: lipgloss.NewStyle().
			Background(lipgloss.Color(panel)).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(border)).
			Padding(0, 1),
		bodyText: lipgloss.NewStyle().
			Foreground(lipgloss.Color(text)),
		bodySelected: lipgloss.NewStyle().
			Foreground(lipgloss.Color(green)).
			Bold(true),
		footer: lipgloss.NewStyle().
			Background(lipgloss.Color(panel)).
			Foreground(lipgloss.Color(muted)).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(border)).
			Padding(0, 1),
	}
}

func padToHeight(content string, width, height int) string {
	if height <= 0 {
		return content
	}
	currentLines := strings.Count(content, "\n") + 1
	if currentLines >= height {
		return content
	}

	padLine := theme.app.Width(max(0, width)).Render("")
	padding := strings.Repeat(padLine+"\n", height-currentLines)
	return content + "\n" + strings.TrimRight(padding, "\n")
}

func styleBodyLines(text string, width int) string {
	if strings.TrimSpace(text) == "" {
		return text
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.TrimLeft(line, " "), "> ") {
			selected := strings.Replace(strings.TrimLeft(line, " "), "> ", "▸ ", 1)
			lines[i] = theme.bodySelected.Width(max(0, width)).Render(selected)
			continue
		}
		if trimmed == "" {
			lines[i] = ""
			continue
		}
		lines[i] = theme.bodyText.Width(max(0, width)).Render(line)
	}
	return strings.Join(lines, "\n")
}
