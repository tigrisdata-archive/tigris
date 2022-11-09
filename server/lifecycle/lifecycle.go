package lifecycle

import "context"

type Hook interface {
	OnStart(context.Context) error
	OnStop(context.Context) error
}

type Lifecycle struct {
	hooks []Hook
}

func NewLifecycle() *Lifecycle {
	return &Lifecycle{}
}

func (l *Lifecycle) AddHook(h Hook) {
	l.hooks = append(l.hooks, h)
}

func (l *Lifecycle) OnStart(ctx context.Context) {
	for _, h := range l.hooks {
		h.OnStart(ctx)
	}
}

func (l *Lifecycle) OnStop(ctx context.Context) {
	for _, h := range l.hooks {
		h.OnStart(ctx)
	}
}
