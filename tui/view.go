package tui

import (
	"fmt"
	"sort"
	"strings"

	tea "charm.land/bubbletea/v2"
)

func (m model) View() tea.View {
	width := m.contentWidth()
	height := m.contentHeight()

	header := strings.Join([]string{
		theme.title.Render("Catbird"),
		theme.tabs.Render(m.tabsLine()),
	}, "\n")
	headerBlock := theme.header.Width(width).Render(header)

	helpBlock := ""
	if m.showHelp {
		helpBlock = theme.helpPanel.Width(width).Render(m.renderHelpPanel())
	}

	errorBlock := ""
	if m.err != nil {
		errorBlock = theme.errorPanel.Width(width).Render(fmt.Sprintf("Error: %v", m.err))
	}

	bodyText := ""
	if m.loading && !m.hasLoaded {
		bodyText = "Loading..."
	} else if m.detailMode != detailNone {
		bodyText = m.renderDetail()
	} else {
		switch m.activeView {
		case viewOverview:
			bodyText = m.renderOverview()
		case viewQueues:
			bodyText = m.renderQueues()
		case viewTasks:
			bodyText = m.renderTasks()
		case viewFlows:
			bodyText = m.renderFlows()
		case viewWorkers:
			bodyText = m.renderWorkers()
		}
	}
	footerBlock := theme.footer.Width(width).Render(m.statusLine())

	reserved := visualLineCount(headerBlock) + visualLineCount(footerBlock)
	if helpBlock != "" {
		reserved += visualLineCount(helpBlock)
	}
	if errorBlock != "" {
		reserved += visualLineCount(errorBlock)
	}
	gaps := 1
	if helpBlock != "" {
		gaps++
	}
	if errorBlock != "" {
		gaps++
	}
	reserved += gaps
	bodyHeight := max(1, height-reserved)
	bodyBlock := theme.body.Width(width).Height(bodyHeight).Render(styleBodyLines(bodyText, width-2))

	blocks := []string{headerBlock}
	if helpBlock != "" {
		blocks = append(blocks, helpBlock)
	}
	if errorBlock != "" {
		blocks = append(blocks, errorBlock)
	}
	blocks = append(blocks, bodyBlock, footerBlock)

	full := theme.app.Width(width).Height(height).Render(strings.Join(blocks, "\n"))
	v := tea.NewView(full)
	v.AltScreen = uiCanAltScreen()
	return v
}

func (m model) renderDetail() string {
	switch m.detailMode {
	case detailTask:
		return m.renderTaskDetail()
	case detailFlow:
		return m.renderFlowDetail()
	default:
		return ""
	}
}

func (m model) renderTaskDetail() string {
	if m.detailName == "" {
		return "No task selected"
	}

	var selected *taskDetailView
	for _, t := range m.data.tasks {
		if t.Name == m.detailName {
			selected = &taskDetailView{name: t.Name, description: t.Description, unlogged: t.Unlogged, createdAt: t.CreatedAt.Format(timeRFC3339)}
			break
		}
	}
	if selected == nil {
		return fmt.Sprintf("Task %q not found", m.detailName)
	}

	maxWidth := m.contentWidth()
	lines := []string{
		"Task Detail",
		fmt.Sprintf("name: %s", selected.name),
		fmt.Sprintf("description: %s", defaultString(selected.description, "-")),
		fmt.Sprintf("storage: %s", storageLabel(selected.unlogged)),
		fmt.Sprintf("created: %s", selected.createdAt),
		"",
		"Recent runs",
	}
	if len(m.data.taskRuns) == 0 {
		lines = append(lines, "- none")
	} else {
		for _, run := range m.data.taskRuns {
			detail := run.ErrorMessage
			if detail == "" {
				detail = prettyJSON(run.Output)
			}
			detail = truncateWithEllipsis(oneLine(detail), max(12, maxWidth-28))
			lines = append(lines, fmt.Sprintf("- #%d  %s  %s", run.ID, run.Status, detail))
		}
	}
	lines = append(lines, "", "esc: back")

	for i := range lines {
		if lines[i] == "" {
			continue
		}
		lines[i] = truncateWithEllipsis(lines[i], maxWidth)
	}

	return strings.Join(lines, "\n")
}

func (m model) renderFlowDetail() string {
	if m.detailName == "" {
		return "No flow selected"
	}

	var flow *flowDetailView
	for _, f := range m.data.flows {
		if f.Name != m.detailName {
			continue
		}

		steps := make([]flowStepDetailView, len(f.Steps))
		for i, s := range f.Steps {
			deps := make([]string, 0, len(s.DependsOn))
			for _, dep := range s.DependsOn {
				deps = append(deps, dep.Name)
			}
			steps[i] = flowStepDetailView{
				name:        s.Name,
				description: s.Description,
				dependsOn:   deps,
				hasSignal:   s.HasSignal,
				isMapStep:   s.IsMapStep,
				mapSource:   s.MapSource,
			}
		}

		flow = &flowDetailView{
			name:        f.Name,
			description: f.Description,
			unlogged:    f.Unlogged,
			createdAt:   f.CreatedAt.Format(timeRFC3339),
			steps:       steps,
		}
		break
	}
	if flow == nil {
		return fmt.Sprintf("Flow %q not found", m.detailName)
	}

	maxWidth := m.contentWidth()
	lines := []string{
		"Flow Detail",
		fmt.Sprintf("name: %s", flow.name),
		fmt.Sprintf("description: %s", defaultString(flow.description, "-")),
		fmt.Sprintf("storage: %s", storageLabel(flow.unlogged)),
		fmt.Sprintf("created: %s", flow.createdAt),
		fmt.Sprintf("steps: %d", len(flow.steps)),
		"",
		"Shape",
	}
	lines = append(lines, m.renderFlowShape(flow)...)
	lines = append(lines, "", "Step descriptions")
	for _, step := range flow.steps {
		lines = append(lines, fmt.Sprintf("- %s: %s", step.name, defaultString(step.description, "-")))
	}
	lines = append(lines, "", "Recent runs")

	if len(m.data.flowRuns) == 0 {
		lines = append(lines, "- none")
	} else {
		for _, run := range m.data.flowRuns {
			detail := run.ErrorMessage
			if detail == "" {
				detail = prettyJSON(run.Output)
			}
			detail = truncateWithEllipsis(oneLine(detail), max(12, maxWidth-28))
			lines = append(lines, fmt.Sprintf("- #%d  %s  %s", run.ID, run.Status, detail))
		}
	}
	lines = append(lines, "", "esc: back")

	for i := range lines {
		if lines[i] == "" {
			continue
		}
		lines[i] = truncateWithEllipsis(lines[i], maxWidth)
	}

	return strings.Join(lines, "\n")
}

func (m model) renderFlowShape(flow *flowDetailView) []string {
	if len(flow.steps) == 0 {
		return []string{"- (empty flow)"}
	}

	stepsByName := make(map[string]flowStepDetailView, len(flow.steps))
	childrenByStep := make(map[string][]string, len(flow.steps))
	parentsByStep := make(map[string][]string, len(flow.steps))
	indegree := make(map[string]int, len(flow.steps))

	for _, step := range flow.steps {
		stepsByName[step.name] = step
		if _, exists := childrenByStep[step.name]; !exists {
			childrenByStep[step.name] = []string{}
		}
		if _, exists := parentsByStep[step.name]; !exists {
			parentsByStep[step.name] = []string{}
		}
		indegree[step.name] = 0
	}

	for _, step := range flow.steps {
		for _, dependencyName := range step.dependsOn {
			if _, exists := indegree[dependencyName]; !exists {
				indegree[dependencyName] = 0
				childrenByStep[dependencyName] = []string{}
				parentsByStep[dependencyName] = []string{}
			}
			indegree[step.name]++
			childrenByStep[dependencyName] = append(childrenByStep[dependencyName], step.name)
			parentsByStep[step.name] = append(parentsByStep[step.name], dependencyName)
		}
	}

	roots := make([]string, 0)
	for stepName, degree := range indegree {
		if _, exists := stepsByName[stepName]; !exists {
			continue
		}
		if degree == 0 {
			roots = append(roots, stepName)
		}
	}
	sort.Strings(roots)

	lines := make([]string, 0, len(flow.steps)*3)
	lines = append(lines, "(start)")
	if len(roots) == 0 {
		lines = append(lines, "└── [cycle or invalid dependency graph]")
	} else {
		visited := make(map[string]bool, len(flow.steps))
		for index, rootName := range roots {
			isLastRoot := index == len(roots)-1
			lines = append(lines, m.renderFlowOptionCTree(rootName, "", isLastRoot, stepsByName, childrenByStep, indegree, visited)...)
		}
	}

	sharedNodes := make([]string, 0)
	for stepName := range stepsByName {
		if indegree[stepName] > 1 {
			sharedNodes = append(sharedNodes, stepName)
		}
	}
	sort.Strings(sharedNodes)
	if len(sharedNodes) > 0 {
		lines = append(lines, "", "Shared nodes")
		for _, stepName := range sharedNodes {
			step := stepsByName[stepName]
			parents := sortedStrings(parentsByStep[stepName])
			targets := sortedStrings(childrenByStep[stepName])
			targetLabel := formatNodeRefs(targets)
			if len(targets) == 0 {
				targetLabel = "(end)"
			}
			lines = append(lines, fmt.Sprintf("[%s]%s <- %s -> %s", stepName, step.flowStepMeta(), formatNodeRefs(parents), targetLabel))
		}
	}

	endSteps := make([]string, 0)
	for stepName, childNames := range childrenByStep {
		if _, exists := stepsByName[stepName]; !exists {
			continue
		}
		if len(childNames) == 0 {
			endSteps = append(endSteps, stepName)
		}
	}
	sort.Strings(endSteps)
	if len(endSteps) > 0 {
		lines = append(lines, fmt.Sprintf("(end) <- %s", strings.Join(endSteps, ", ")))
	}

	return lines
}

func (m model) renderFlowOptionCTree(
	nodeName string,
	prefix string,
	isLast bool,
	stepsByName map[string]flowStepDetailView,
	childrenByStep map[string][]string,
	indegree map[string]int,
	visited map[string]bool,
) []string {
	step := stepsByName[nodeName]
	branch := "├──"
	nextPrefix := prefix + "│   "
	if isLast {
		branch = "└──"
		nextPrefix = prefix + "    "
	}

	if visited[nodeName] {
		return []string{fmt.Sprintf("%s%s [%s] (ref)", prefix, branch, step.name)}
	}
	visited[nodeName] = true

	allChildren := sortedStrings(childrenByStep[nodeName])
	sharedTargets := make([]string, 0)
	uniqueChildren := make([]string, 0)
	for _, childName := range allChildren {
		if indegree[childName] > 1 {
			sharedTargets = append(sharedTargets, childName)
			continue
		}
		uniqueChildren = append(uniqueChildren, childName)
	}

	line := fmt.Sprintf("%s%s [%s]%s", prefix, branch, step.name, step.flowStepMeta())
	if len(sharedTargets) > 0 {
		line += " -> " + formatNodeRefs(sharedTargets)
	}
	lines := []string{line}
	for index, childName := range uniqueChildren {
		childIsLast := index == len(uniqueChildren)-1
		lines = append(lines, m.renderFlowOptionCTree(childName, nextPrefix, childIsLast, stepsByName, childrenByStep, indegree, visited)...)
	}
	return lines
}

func formatNodeRefs(names []string) string {
	if len(names) == 0 {
		return "-"
	}
	refs := make([]string, len(names))
	for i, name := range names {
		refs[i] = "[" + name + "]"
	}
	return strings.Join(refs, ", ")
}

func sortedStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return copyValues
}

type taskDetailView struct {
	name        string
	description string
	unlogged    bool
	createdAt   string
}

type flowDetailView struct {
	name        string
	description string
	unlogged    bool
	createdAt   string
	steps       []flowStepDetailView
}

func storageLabel(unlogged bool) string {
	if unlogged {
		return "unlogged"
	}
	return "logged"
}

type flowStepDetailView struct {
	name        string
	description string
	dependsOn   []string
	hasSignal   bool
	isMapStep   bool
	mapSource   string
}

func (s flowStepDetailView) flowStepMeta() string {
	flags := make([]string, 0, 2)
	if s.hasSignal {
		flags = append(flags, "signal")
	}
	if s.isMapStep {
		if s.mapSource != "" {
			flags = append(flags, "map:"+s.mapSource)
		} else {
			flags = append(flags, "map")
		}
	}
	if len(flags) == 0 {
		return ""
	}
	return " [" + strings.Join(flags, ",") + "]"
}

func (m model) renderHelpPanel() string {
	bindings := BindingsForHelp()
	if len(bindings) == 0 {
		return "Help\n- no bindings"
	}
	width := m.contentWidth()
	keyCol := 16
	if width < 56 {
		keyCol = 10
	}

	var b strings.Builder
	b.WriteString(truncateWithEllipsis("Help (? to close)", width))
	b.WriteString("\n")
	for _, binding := range bindings {
		line := fmt.Sprintf("- %-*s %s", keyCol, strings.Join(binding.Keys, "/"), binding.Label)
		b.WriteString(truncateWithEllipsis(line, width))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func (m model) tabsLine() string {
	tabs := []string{"Overview", "Queues", "Tasks", "Flows", "Workers"}
	parts := make([]string, len(tabs))
	for i, t := range tabs {
		if view(i) == m.activeView {
			parts[i] = "[" + t + "]"
		} else {
			parts[i] = t
		}
	}
	return truncateWithEllipsis(strings.Join(parts, " | "), m.contentWidth())
}

func (m model) renderOverview() string {
	lines := []string{}
	if m.isCompact() {
		lines = append(lines,
			fmt.Sprintf("Q:%d  T:%d  F:%d  W:%d", len(m.data.queues), len(m.data.tasks), len(m.data.flows), len(m.data.workers)),
			"",
			fmt.Sprintf("Task runs: %d", len(m.data.taskRuns)),
			fmt.Sprintf("Flow runs: %d", len(m.data.flowRuns)),
		)
	} else {
		lines = append(lines,
			fmt.Sprintf("Queues:  %d", len(m.data.queues)),
			fmt.Sprintf("Tasks:   %d", len(m.data.tasks)),
			fmt.Sprintf("Flows:   %d", len(m.data.flows)),
			fmt.Sprintf("Workers: %d", len(m.data.workers)),
			"",
			fmt.Sprintf("Selected task runs: %d", len(m.data.taskRuns)),
			fmt.Sprintf("Selected flow runs: %d", len(m.data.flowRuns)),
		)
	}
	for i := range lines {
		if lines[i] == "" {
			continue
		}
		lines[i] = truncateWithEllipsis(lines[i], m.contentWidth())
	}
	return strings.Join(lines, "\n")
}

func (m model) renderQueues() string {
	if len(m.data.queues) == 0 {
		return "No queues"
	}

	var b strings.Builder
	for i, q := range m.data.queues {
		prefix := "  "
		if i == m.selected[viewQueues] {
			prefix = "> "
		}
		line := ""
		if m.isCompact() {
			line = fmt.Sprintf("%s%s  unlogged=%t  desc=%s", prefix, q.Name, q.Unlogged, defaultString(q.Description, "-"))
		} else {
			expiresAt := "-"
			if !q.ExpiresAt.IsZero() {
				expiresAt = q.ExpiresAt.Format(timeRFC3339)
			}
			line = fmt.Sprintf("%s%s  desc=%s  unlogged=%t  created=%s  expires=%s", prefix, q.Name, defaultString(q.Description, "-"), q.Unlogged, q.CreatedAt.Format(timeRFC3339), expiresAt)
		}
		b.WriteString(truncateWithEllipsis(line, m.contentWidth()))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func (m model) renderTasks() string {
	if len(m.data.tasks) == 0 {
		return "No tasks"
	}

	var b strings.Builder
	b.WriteString(truncateWithEllipsis("Tasks", m.contentWidth()))
	b.WriteString("\n")
	for i, t := range m.data.tasks {
		prefix := "  "
		if i == m.selected[viewTasks] {
			prefix = "> "
		}
		line := ""
		if m.isCompact() {
			line = fmt.Sprintf("%s%s  unlogged=%t  desc=%s", prefix, t.Name, t.Unlogged, defaultString(t.Description, "-"))
		} else {
			line = fmt.Sprintf("%s%s  desc=%s  unlogged=%t  created=%s", prefix, t.Name, defaultString(t.Description, "-"), t.Unlogged, t.CreatedAt.Format(timeRFC3339))
		}
		b.WriteString(truncateWithEllipsis(line, m.contentWidth()))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	runsHeading := "Recent runs for selected task"
	if m.isCompact() {
		runsHeading = "Task runs"
	}
	b.WriteString(truncateWithEllipsis(runsHeading, m.contentWidth()))
	b.WriteString("\n")
	if len(m.data.taskRuns) == 0 {
		b.WriteString("- none")
		return b.String()
	}
	maxDetail := max(16, m.contentWidth()-38)
	if m.isCompact() {
		maxDetail = max(12, m.contentWidth()-16)
	}
	for _, run := range m.data.taskRuns {
		detail := run.ErrorMessage
		if detail == "" {
			detail = prettyJSON(run.Output)
		}
		detail = truncateWithEllipsis(oneLine(detail), maxDetail)
		line := ""
		if m.isCompact() {
			line = fmt.Sprintf("- #%d %s %s", run.ID, run.Status, detail)
		} else {
			line = fmt.Sprintf("- #%d  %s  started=%s  %s", run.ID, run.Status, shortTime(run.StartedAt), detail)
		}
		b.WriteString(truncateWithEllipsis(line, m.contentWidth()))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func (m model) renderFlows() string {
	if len(m.data.flows) == 0 {
		return "No flows"
	}

	var b strings.Builder
	b.WriteString(truncateWithEllipsis("Flows", m.contentWidth()))
	b.WriteString("\n")
	for i, f := range m.data.flows {
		prefix := "  "
		if i == m.selected[viewFlows] {
			prefix = "> "
		}
		line := ""
		if m.isCompact() {
			line = fmt.Sprintf("%s%s  steps=%d  unlogged=%t  desc=%s", prefix, f.Name, len(f.Steps), f.Unlogged, defaultString(f.Description, "-"))
		} else {
			line = fmt.Sprintf("%s%s  desc=%s  steps=%d  unlogged=%t  created=%s", prefix, f.Name, defaultString(f.Description, "-"), len(f.Steps), f.Unlogged, f.CreatedAt.Format(timeRFC3339))
		}
		b.WriteString(truncateWithEllipsis(line, m.contentWidth()))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	runsHeading := "Recent runs for selected flow"
	if m.isCompact() {
		runsHeading = "Flow runs"
	}
	b.WriteString(truncateWithEllipsis(runsHeading, m.contentWidth()))
	b.WriteString("\n")
	if len(m.data.flowRuns) == 0 {
		b.WriteString("- none")
		return b.String()
	}
	maxDetail := max(16, m.contentWidth()-38)
	if m.isCompact() {
		maxDetail = max(12, m.contentWidth()-16)
	}
	for _, run := range m.data.flowRuns {
		detail := run.ErrorMessage
		if detail == "" {
			detail = prettyJSON(run.Output)
		}
		detail = truncateWithEllipsis(oneLine(detail), maxDetail)
		line := ""
		if m.isCompact() {
			line = fmt.Sprintf("- #%d %s %s", run.ID, run.Status, detail)
		} else {
			line = fmt.Sprintf("- #%d  %s  started=%s  %s", run.ID, run.Status, shortTime(run.StartedAt), detail)
		}
		b.WriteString(truncateWithEllipsis(line, m.contentWidth()))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func (m model) renderWorkers() string {
	if len(m.data.workers) == 0 {
		return "No workers"
	}

	var b strings.Builder
	for i, w := range m.data.workers {
		prefix := "  "
		if i == m.selected[viewWorkers] {
			prefix = "> "
		}
		line := ""
		if m.isCompact() {
			line = fmt.Sprintf("%s%s  t=%d s=%d", prefix, w.ID, len(w.TaskHandlers), len(w.StepHandlers))
		} else {
			line = fmt.Sprintf("%s%s  tasks=%d  steps=%d  heartbeat=%s", prefix, w.ID, len(w.TaskHandlers), len(w.StepHandlers), shortTime(w.LastHeartbeatAt))
		}
		b.WriteString(truncateWithEllipsis(line, m.contentWidth()))
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

func (m model) statusLine() string {
	updated := "never"
	if !m.lastUpdate.IsZero() {
		updated = m.lastUpdate.Format("15:04:05")
	}
	if m.detailMode != detailNone {
		line := fmt.Sprintf("esc: back  r: refresh  q/ctrl+c: quit  ?: help  |  updated: %s", updated)
		return truncateWithEllipsis(line, m.contentWidth())
	}
	line := fmt.Sprintf("%s  |  updated: %s", keys.helpText(), updated)
	return truncateWithEllipsis(line, m.contentWidth())
}

func (m model) contentWidth() int {
	if m.width <= 0 {
		return 80
	}
	return m.width
}

func (m model) contentHeight() int {
	if m.height <= 0 {
		return 24
	}
	return m.height
}

func (m model) isCompact() bool {
	return m.contentWidth() < 60
}
