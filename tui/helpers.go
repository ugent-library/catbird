package tui

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"time"
)

const timeRFC3339 = time.RFC3339

func shortTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("2006-01-02 15:04:05")
}

func prettyJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var out bytes.Buffer
	if err := json.Indent(&out, raw, "", "  "); err != nil {
		return string(raw)
	}
	return out.String()
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func truncateWithEllipsis(s string, maxWidth int) string {
	if maxWidth <= 0 {
		return s
	}
	runes := []rune(s)
	if len(runes) <= maxWidth {
		return s
	}
	if maxWidth == 1 {
		return "…"
	}
	return string(runes[:maxWidth-1]) + "…"
}

func visualLineCount(s string) int {
	if s == "" {
		return 0
	}
	return strings.Count(s, "\n") + 1
}

func uiCanColor() bool {
	return !isNoColor() && !isDumbTerm()
}

func uiCanAltScreen() bool {
	return !isDumbTerm()
}

func isNoColor() bool {
	_, ok := os.LookupEnv("NO_COLOR")
	return ok
}

func isDumbTerm() bool {
	term := strings.TrimSpace(strings.ToLower(os.Getenv("TERM")))
	return term == "" || term == "dumb"
}
