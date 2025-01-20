package commands

type CommandBase struct {
	verboseName string
	// Any other optional fields...
}

type CommandBaseOption func(*CommandBase)

func WithVerboseName(name string) CommandBaseOption {
	return func(base *CommandBase) {
		base.verboseName = name
	}
}
