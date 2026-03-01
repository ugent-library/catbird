package tui

import (
	"context"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/ugent-library/catbird"
)

func (m model) Init() tea.Cmd {
	return tea.Batch(m.loadCmd(), tickCmd(), requestWindowSizeCmd())
}

func requestWindowSizeCmd() tea.Cmd {
	return func() tea.Msg {
		return tea.RequestWindowSize()
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyPressMsg:
		key := msg.String()
		switch {
		case keys.isQuit(key):
			return m, tea.Quit
		case keys.isHelp(key):
			m.showHelp = !m.showHelp
			return m, nil
		case keys.isBack(key):
			if m.detailMode != detailNone {
				m.detailMode = detailNone
				m.detailName = ""
			}
			return m, nil
		case keys.isOpen(key):
			if m.detailMode != detailNone {
				return m, nil
			}
			switch m.activeView {
			case viewTasks:
				if idx := m.selected[viewTasks]; idx >= 0 && idx < len(m.data.tasks) {
					m.detailMode = detailTask
					m.detailName = m.data.tasks[idx].Name
				}
			case viewFlows:
				if idx := m.selected[viewFlows]; idx >= 0 && idx < len(m.data.flows) {
					m.detailMode = detailFlow
					m.detailName = m.data.flows[idx].Name
				}
			}
			return m, nil
		case keys.isNextView(key):
			if m.detailMode != detailNone {
				return m, nil
			}
			m.activeView = (m.activeView + 1) % 5
			return m, m.startLoad()
		case keys.isPrevView(key):
			if m.detailMode != detailNone {
				return m, nil
			}
			m.activeView--
			if m.activeView < 0 {
				m.activeView = viewWorkers
			}
			return m, m.startLoad()
		case keys.isRefresh(key):
			return m, m.startLoad()
		case keys.isMoveUp(key):
			if m.detailMode != detailNone {
				return m, nil
			}
			m.move(-1)
			return m, m.startLoad()
		case keys.isMoveDown(key):
			if m.detailMode != detailNone {
				return m, nil
			}
			m.move(1)
			return m, m.startLoad()
		}
	case loadedMsg:
		m.inFlight = false
		m.refreshing = false
		m.loading = false
		m.err = msg.err
		if msg.err == nil {
			m.data = msg.data
			m.hasLoaded = true
			m.lastUpdate = time.Now()
			m.clampSelection()
		}
		if m.pending {
			m.pending = false
			return m, m.startLoad()
		}
		return m, nil
	case refreshTickMsg:
		return m, tea.Batch(m.startLoad(), tickCmd())
	}

	return m, nil
}

func (m *model) startLoad() tea.Cmd {
	if m.inFlight {
		m.pending = true
		return nil
	}
	m.inFlight = true
	if m.hasLoaded {
		m.refreshing = true
		m.loading = false
	} else {
		m.loading = true
		m.refreshing = false
	}
	return m.loadCmd()
}

func (m model) loadCmd() tea.Cmd {
	selectedTask := ""
	selectedFlow := ""
	if idx := m.selected[viewTasks]; idx >= 0 && idx < len(m.data.tasks) {
		selectedTask = m.data.tasks[idx].Name
	}
	if idx := m.selected[viewFlows]; idx >= 0 && idx < len(m.data.flows) {
		selectedFlow = m.data.flows[idx].Name
	}

	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
		defer cancel()

		queues, err := m.client.ListQueues(ctx)
		if err != nil {
			return loadedMsg{err: err}
		}

		tasks, err := m.client.ListTasks(ctx)
		if err != nil {
			return loadedMsg{err: err}
		}

		flows, err := m.client.ListFlows(ctx)
		if err != nil {
			return loadedMsg{err: err}
		}

		workers, err := m.client.ListWorkers(ctx)
		if err != nil {
			return loadedMsg{err: err}
		}

		if selectedTask == "" && len(tasks) > 0 {
			selectedTask = tasks[0].Name
		}
		if selectedFlow == "" && len(flows) > 0 {
			selectedFlow = flows[0].Name
		}

		var taskRuns []*catbird.TaskRunInfo
		if selectedTask != "" {
			taskRuns, err = m.client.ListTaskRuns(ctx, selectedTask)
			if err != nil {
				return loadedMsg{err: err}
			}
		}

		var flowRuns []*catbird.FlowRunInfo
		if selectedFlow != "" {
			flowRuns, err = m.client.ListFlowRuns(ctx, selectedFlow)
			if err != nil {
				return loadedMsg{err: err}
			}
		}

		return loadedMsg{data: snapshot{
			queues:   queues,
			tasks:    tasks,
			flows:    flows,
			workers:  workers,
			taskRuns: taskRuns,
			flowRuns: flowRuns,
		}}
	}
}

func tickCmd() tea.Cmd {
	return tea.Tick(refreshInterval, func(time.Time) tea.Msg {
		return refreshTickMsg{}
	})
}
