package tui

import (
	"context"
	"time"

	"github.com/ugent-library/catbird"
)

type view int

type detailKind int

const (
	viewOverview view = iota
	viewQueues
	viewTasks
	viewFlows
	viewWorkers
)

const (
	detailNone detailKind = iota
	detailTask
	detailFlow
)

const refreshInterval = 2 * time.Second

type snapshot struct {
	queues   []*catbird.QueueInfo
	tasks    []*catbird.TaskInfo
	flows    []*catbird.FlowInfo
	workers  []*catbird.WorkerInfo
	taskRuns []*catbird.TaskRunInfo
	flowRuns []*catbird.FlowRunInfo
}

type loadedMsg struct {
	data snapshot
	err  error
}

type refreshTickMsg struct{}

type model struct {
	ctx        context.Context
	client     *catbird.Client
	width      int
	height     int
	activeView view
	showHelp   bool
	loading    bool
	refreshing bool
	inFlight   bool
	pending    bool
	hasLoaded  bool
	err        error
	data       snapshot
	selected   map[view]int
	detailMode detailKind
	detailName string
	lastUpdate time.Time
}

func newModel(ctx context.Context, client *catbird.Client) model {
	return model{
		ctx:        ctx,
		client:     client,
		activeView: viewOverview,
		loading:    true,
		inFlight:   true,
		selected: map[view]int{
			viewQueues:  0,
			viewTasks:   0,
			viewFlows:   0,
			viewWorkers: 0,
		},
	}
}

func (m *model) move(delta int) {
	cur := m.selected[m.activeView]
	max := m.maxIndex(m.activeView)
	if max < 0 {
		m.selected[m.activeView] = 0
		return
	}
	cur += delta
	if cur < 0 {
		cur = 0
	}
	if cur > max {
		cur = max
	}
	m.selected[m.activeView] = cur
}

func (m *model) maxIndex(v view) int {
	switch v {
	case viewQueues:
		return len(m.data.queues) - 1
	case viewTasks:
		return len(m.data.tasks) - 1
	case viewFlows:
		return len(m.data.flows) - 1
	case viewWorkers:
		return len(m.data.workers) - 1
	default:
		return -1
	}
}

func (m *model) clampSelection() {
	for _, v := range []view{viewQueues, viewTasks, viewFlows, viewWorkers} {
		max := m.maxIndex(v)
		if max < 0 {
			m.selected[v] = 0
			continue
		}
		if m.selected[v] > max {
			m.selected[v] = max
		}
	}
}
