package tui

import "strings"

const (
	actionQuit     = "quit"
	actionNextView = "next_view"
	actionPrevView = "prev_view"
	actionRefresh  = "refresh"
	actionMoveUp   = "move_up"
	actionMoveDown = "move_down"
	actionHelp     = "help"
	actionOpen     = "open"
	actionBack     = "back"
)

// KeyBinding describes a keyboard action and its supported key strings.
// It can be reused by future command palette or help overlays.
type KeyBinding struct {
	Action string
	Label  string
	Keys   []string
	Help   string
}

// DefaultKeyBindings is the canonical list of TUI bindings.
var DefaultKeyBindings = []KeyBinding{
	{Action: actionMoveUp, Label: "Move up", Keys: []string{"up", "k"}},
	{Action: actionMoveDown, Label: "Move down", Keys: []string{"down", "j"}},
	{Action: actionNextView, Label: "Next view", Keys: []string{"tab", "l"}},
	{Action: actionPrevView, Label: "Previous view", Keys: []string{"shift+tab", "h"}},
	{Action: actionRefresh, Label: "Refresh", Keys: []string{"r"}},
	{Action: actionOpen, Label: "Open details", Keys: []string{"enter"}},
	{Action: actionBack, Label: "Back", Keys: []string{"esc"}},
	{Action: actionHelp, Label: "Toggle help", Keys: []string{"?"}},
	{Action: actionQuit, Label: "Quit", Keys: []string{"q", "ctrl+c"}},
}

var helpBindingOrder = []string{
	actionMoveUp,
	actionMoveDown,
	actionNextView,
	actionPrevView,
	actionRefresh,
	actionOpen,
	actionBack,
	actionHelp,
	actionQuit,
}

type keymap struct {
	bindings map[string]KeyBinding
}

var keys = newKeymap(DefaultKeyBindings)

func newKeymap(bindings []KeyBinding) keymap {
	byAction := make(map[string]KeyBinding, len(bindings))
	for _, binding := range bindings {
		binding.Help = defaultHelpText(binding)
		byAction[binding.Action] = binding
	}
	return keymap{bindings: byAction}
}

func (k keymap) isQuit(key string) bool {
	return k.matches(actionQuit, key)
}

func (k keymap) isNextView(key string) bool {
	return k.matches(actionNextView, key)
}

func (k keymap) isPrevView(key string) bool {
	return k.matches(actionPrevView, key)
}

func (k keymap) isRefresh(key string) bool {
	return k.matches(actionRefresh, key)
}

func (k keymap) isMoveUp(key string) bool {
	return k.matches(actionMoveUp, key)
}

func (k keymap) isMoveDown(key string) bool {
	return k.matches(actionMoveDown, key)
}

func (k keymap) isHelp(key string) bool {
	return k.matches(actionHelp, key)
}

func (k keymap) isOpen(key string) bool {
	return k.matches(actionOpen, key)
}

func (k keymap) isBack(key string) bool {
	return k.matches(actionBack, key)
}

func (k keymap) helpText() string {
	parts := make([]string, 0, 4)
	for _, action := range []string{actionMoveUp, actionNextView, actionRefresh, actionOpen, actionBack, actionQuit} {
		binding, ok := k.bindings[action]
		if !ok {
			continue
		}
		parts = append(parts, binding.Help)
	}
	if help, ok := k.bindings[actionHelp]; ok {
		parts = append(parts, help.Help)
	}
	return strings.Join(parts, "  ")
}

// BindingsForHelp returns key bindings in stable display order with computed
// Help values. This is intended for future on-screen help views.
func BindingsForHelp() []KeyBinding {
	b := make([]KeyBinding, 0, len(helpBindingOrder))
	for _, action := range helpBindingOrder {
		binding, ok := keys.bindings[action]
		if !ok {
			continue
		}
		b = append(b, binding)
	}
	return b
}

func (k keymap) matches(action, key string) bool {
	binding, ok := k.bindings[action]
	if !ok {
		return false
	}
	return containsKey(binding.Keys, key)
}

func defaultHelpText(binding KeyBinding) string {
	if binding.Help != "" {
		return binding.Help
	}
	if len(binding.Keys) == 0 {
		return strings.ToLower(binding.Label)
	}

	keys := binding.Keys
	if binding.Action == actionMoveUp {
		keys = []string{"j/k"}
	}
	if binding.Action == actionNextView {
		keys = []string{"tab/shift+tab"}
	}

	return strings.Join(keys, "/") + ": " + strings.ToLower(binding.Label)
}

func containsKey(candidates []string, key string) bool {
	for _, candidate := range candidates {
		if candidate == key {
			return true
		}
	}
	return false
}
