package daemon

import (
	"context"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	containertypes "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/v2/daemon/container"
	libcontainerdtypes "github.com/moby/moby/v2/daemon/internal/libcontainerd/types"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

// statusTask is a fake containerd task that only reports a fixed status.
type statusTask struct {
	libcontainerdtypes.Task
	status containerd.Status
}

func (t *statusTask) Pid() uint32 {
	return 1
}

func (t *statusTask) Status(context.Context) (containerd.Status, error) {
	return t.status, nil
}

// newExitTestContainer builds a minimal container with the given restart
// policy, in the state that handleContainerExit expects when a task exits.
func newExitTestContainer(t *testing.T, policy containertypes.RestartPolicy) *container.Container {
	t.Helper()
	ctr := container.NewBaseContainer(t.Name(), t.TempDir())
	ctr.Config = &containertypes.Config{}
	ctr.HostConfig = &containertypes.HostConfig{RestartPolicy: policy}
	ctr.State.StartedAt = time.Now()
	return ctr
}

func TestDecideContainerExitAction(t *testing.T) {
	tests := []struct {
		doc               string
		policy            containertypes.RestartPolicy
		exitCode          int
		daemonShutdown    bool
		manuallyStopped   bool
		manuallyRestarted bool
		expRestart        bool
		expAutoRemove     bool
	}{
		{
			doc:           "no restart policy: stop and auto-remove",
			policy:        containertypes.RestartPolicy{Name: containertypes.RestartPolicyDisabled},
			exitCode:      1,
			expAutoRemove: true,
		},
		{
			doc:        "always: restart, never auto-remove",
			policy:     containertypes.RestartPolicy{Name: containertypes.RestartPolicyAlways},
			exitCode:   0,
			expRestart: true,
		},
		{
			// "always" ignores the manually-stopped / shutdown flag, so the
			// restart-manager still asks for a restart.
			doc:            "always: restart even when daemon shutting down",
			policy:         containertypes.RestartPolicy{Name: containertypes.RestartPolicyAlways},
			exitCode:       1,
			daemonShutdown: true,
			expRestart:     true,
		},
		{
			doc:        "unless-stopped: restart when not manually stopped",
			policy:     containertypes.RestartPolicy{Name: containertypes.RestartPolicyUnlessStopped},
			exitCode:   1,
			expRestart: true,
		},
		{
			doc:             "unless-stopped: stop and auto-remove when manually stopped",
			policy:          containertypes.RestartPolicy{Name: containertypes.RestartPolicyUnlessStopped},
			exitCode:        1,
			manuallyStopped: true,
			expAutoRemove:   true,
		},
		{
			doc:            "unless-stopped: stop and auto-remove on daemon shutdown",
			policy:         containertypes.RestartPolicy{Name: containertypes.RestartPolicyUnlessStopped},
			exitCode:       1,
			daemonShutdown: true,
			expAutoRemove:  true,
		},
		{
			doc:        "on-failure: restart on non-zero exit",
			policy:     containertypes.RestartPolicy{Name: containertypes.RestartPolicyOnFailure},
			exitCode:   2,
			expRestart: true,
		},
		{
			doc:           "on-failure: stop and auto-remove on zero exit",
			policy:        containertypes.RestartPolicy{Name: containertypes.RestartPolicyOnFailure},
			exitCode:      0,
			expAutoRemove: true,
		},
		{
			doc:               "manual restart suppresses auto-remove",
			policy:            containertypes.RestartPolicy{Name: containertypes.RestartPolicyDisabled},
			exitCode:          0,
			manuallyRestarted: true,
			expAutoRemove:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.doc, func(t *testing.T) {
			ctr := newExitTestContainer(t, tc.policy)
			ctr.HasBeenManuallyStopped = tc.manuallyStopped
			ctr.HasBeenManuallyRestarted = tc.manuallyRestarted

			ctr.Lock()
			action := decideContainerExitAction(context.Background(), ctr, container.ExitStatus{ExitCode: tc.exitCode}, tc.daemonShutdown)
			ctr.Unlock()
			// Stop the restart-manager's back-off timer goroutine, if any.
			t.Cleanup(func() { ctr.RestartManager().Cancel() })

			assert.Check(t, is.Equal(action.restart, tc.expRestart), "restart")
			assert.Check(t, is.Equal(action.autoRemove, tc.expAutoRemove), "autoRemove")
			// A restart must always come with a wait channel to block on,
			// and a stop must never leave one behind.
			assert.Check(t, is.Equal(action.wait != nil, tc.expRestart), "wait channel")
			assert.Check(t, action.execDuration >= 0, "execDuration")
		})
	}
}

// A canceled restart-manager (e.g. during daemon shutdown) must result in a
// plain stop, without an error being surfaced or a restart being scheduled.
func TestDecideContainerExitActionRestartCanceled(t *testing.T) {
	ctr := newExitTestContainer(t, containertypes.RestartPolicy{Name: containertypes.RestartPolicyAlways})
	ctr.RestartManager().Cancel()

	ctr.Lock()
	action := decideContainerExitAction(context.Background(), ctr, container.ExitStatus{ExitCode: 1}, false)
	ctr.Unlock()

	assert.Check(t, !action.restart, "restart")
	assert.Check(t, is.Nil(action.wait), "wait channel")
	assert.Check(t, action.autoRemove, "autoRemove")
}

// A restart-manager that is already busy with a restart returns an error;
// the container must then fall back to being stopped instead of restarted.
func TestDecideContainerExitActionRestartManagerError(t *testing.T) {
	ctr := newExitTestContainer(t, containertypes.RestartPolicy{Name: containertypes.RestartPolicyAlways})
	t.Cleanup(func() { ctr.RestartManager().Cancel() })

	// First call activates the restart-manager (a restart is pending).
	ctr.Lock()
	first := decideContainerExitAction(context.Background(), ctr, container.ExitStatus{ExitCode: 1}, false)
	assert.Assert(t, first.restart)

	// Second call, while still active, hits the "invalid call on an active
	// restart manager" error path.
	second := decideContainerExitAction(context.Background(), ctr, container.ExitStatus{ExitCode: 1}, false)
	ctr.Unlock()

	assert.Check(t, !second.restart, "restart")
	assert.Check(t, is.Nil(second.wait), "wait channel")
	assert.Check(t, second.autoRemove, "autoRemove")
}

// A container whose last error is a networking-setup failure must be left
// untouched (kept in "created") so that it is neither restarted nor exited.
func TestHandleContainerExitSkipsNetworkingSetupError(t *testing.T) {
	d := &Daemon{}
	ctr := newExitTestContainer(t, containertypes.RestartPolicy{Name: containertypes.RestartPolicyAlways})
	// The container never started, so it is still in "created" state.
	ctr.State.StartedAt = time.Time{}
	ctr.State.ErrorMsg = "some prefix: " + errSetupNetworking + ": port is already allocated"
	assert.Assert(t, is.Equal(ctr.State.State(), containertypes.StateCreated))

	err := d.handleContainerExit(ctr, &libcontainerdtypes.EventInfo{ContainerID: ctr.ID, ProcessID: ctr.ID, ExitCode: 128})
	assert.NilError(t, err)

	assert.Check(t, is.Equal(ctr.State.State(), containertypes.StateCreated), "state")
	assert.Check(t, is.Equal(ctr.State.ExitCode, 0), "exit code")
	assert.Check(t, is.Equal(ctr.RestartCount, 0), "restart count")
	// The lock must have been released on the early-return path.
	assert.Check(t, ctr.TryLock(), "container lock still held")
	ctr.Unlock()
}

// Duplicate exit events for a container that is already exited / restarting,
// or whose task is still running, must be ignored without touching state.
func TestHandleContainerExitIgnoresDuplicateExitEvent(t *testing.T) {
	tests := []struct {
		doc      string
		setup    func(ctr *container.Container)
		expState containertypes.ContainerState
	}{
		{
			doc: "already exited",
			setup: func(ctr *container.Container) {
				ctr.State.SetStopped(&container.ExitStatus{ExitCode: 3})
			},
			expState: containertypes.StateExited,
		},
		{
			doc: "already restarting",
			setup: func(ctr *container.Container) {
				ctr.State.SetRestarting(&container.ExitStatus{ExitCode: 3})
			},
			expState: containertypes.StateRestarting,
		},
		{
			doc: "task still running",
			setup: func(ctr *container.Container) {
				ctr.State.SetRunning(nil, &statusTask{status: containerd.Status{Status: containerd.Running}}, time.Now())
			},
			expState: containertypes.StateRunning,
		},
		{
			doc: "task stopped with a different exit code",
			setup: func(ctr *container.Container) {
				ctr.State.SetRunning(nil, &statusTask{status: containerd.Status{Status: containerd.Stopped, ExitStatus: 7}}, time.Now())
			},
			expState: containertypes.StateRunning,
		},
	}

	for _, tc := range tests {
		t.Run(tc.doc, func(t *testing.T) {
			d := &Daemon{}
			ctr := newExitTestContainer(t, containertypes.RestartPolicy{Name: containertypes.RestartPolicyAlways})
			ctr.Lock()
			tc.setup(ctr)
			ctr.Unlock()

			err := d.handleContainerExit(ctr, &libcontainerdtypes.EventInfo{ContainerID: ctr.ID, ProcessID: ctr.ID, ExitCode: 1})
			assert.NilError(t, err)

			// Neither the state nor the restart bookkeeping may change.
			assert.Check(t, is.Equal(ctr.State.State(), tc.expState), "state")
			assert.Check(t, is.Equal(ctr.RestartCount, 0), "restart count")
			assert.Check(t, ctr.TryLock(), "container lock still held")
			ctr.Unlock()
		})
	}
}
