package catbird

type Flow struct {
	name string
}

func NewFlow(name string) *Flow {
	return &Flow{
		name: name,
	}
}

type Step struct {
	name      string
	dependsOn []string
}

type StepOpts struct {
	DependsOn []string
}

func NewStep(name string, opts StepOpts) *Step {
	return &Step{
		name:      name,
		dependsOn: opts.DependsOn,
	}
}
