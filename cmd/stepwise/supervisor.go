package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// Supervisor runs worker child processes, restarts them when they die, and
// can kill them on request with SIGKILL, which gives a worker no chance to
// clean up. It only ever signals processes it started itself.
type Supervisor struct {
	WorkerArgs   []string
	RestartDelay time.Duration
	Quiet        bool

	mu       sync.Mutex
	slots    []*slot
	stopping bool
	kills    int
	stalls   int
	died     map[string]time.Time // worker id -> when it was killed or exited
	wg       sync.WaitGroup
}

type slot struct {
	index int
	gen   int
	id    string
	cmd   *exec.Cmd
	alive bool
}

type SlotInfo struct {
	Slot  int    `json:"slot"`
	ID    string `json:"id"`
	PID   int    `json:"pid"`
	Alive bool   `json:"alive"`
}

func (s *Supervisor) Start(n int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < n; i++ {
		sl := &slot{index: i}
		s.slots = append(s.slots, sl)
		if err := s.spawnLocked(sl); err != nil {
			return err
		}
	}
	return nil
}

func (s *Supervisor) spawnLocked(sl *slot) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	sl.gen++
	sl.id = fmt.Sprintf("w%d.%d", sl.index+1, sl.gen)
	id := sl.id
	cmd := exec.Command(self, append([]string{"worker", "--id", sl.id}, s.WorkerArgs...)...)
	if !s.Quiet {
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	sl.cmd, sl.alive = cmd, true
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		cmd.Wait()
		s.mu.Lock()
		sl.alive = false
		s.markDeadLocked(id)
		stopping := s.stopping
		s.mu.Unlock()
		if stopping {
			return
		}
		time.Sleep(s.RestartDelay)
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.stopping {
			s.spawnLocked(sl)
		}
	}()
	return nil
}

// Kill sends SIGKILL to the worker in the given slot.
func (s *Supervisor) Kill(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 0 || index >= len(s.slots) || !s.slots[index].alive {
		return false
	}
	s.slots[index].cmd.Process.Signal(syscall.SIGKILL)
	s.markDeadLocked(s.slots[index].id)
	s.kills++
	return true
}

func (s *Supervisor) KillRandom() bool {
	s.mu.Lock()
	var alive []int
	for _, sl := range s.slots {
		if sl.alive {
			alive = append(alive, sl.index)
		}
	}
	s.mu.Unlock()
	if len(alive) == 0 {
		return false
	}
	return s.Kill(alive[rand.IntN(len(alive))])
}

// StallRandom freezes a random worker with SIGSTOP and thaws it with SIGCONT
// after d. To the rest of the system a frozen worker looks dead (no
// heartbeats), but it is not: when it wakes up it still believes it owns its
// workflows. This is the case fencing tokens exist for.
func (s *Supervisor) StallRandom(d time.Duration) bool {
	s.mu.Lock()
	var alive []*slot
	for _, sl := range s.slots {
		if sl.alive {
			alive = append(alive, sl)
		}
	}
	if len(alive) == 0 {
		s.mu.Unlock()
		return false
	}
	proc := alive[rand.IntN(len(alive))].cmd.Process
	proc.Signal(syscall.SIGSTOP)
	s.stalls++
	s.mu.Unlock()
	time.AfterFunc(d, func() { proc.Signal(syscall.SIGCONT) }) // no-op if it was killed meanwhile
	return true
}

func (s *Supervisor) Stalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stalls
}

func (s *Supervisor) markDeadLocked(id string) {
	if s.died == nil {
		s.died = map[string]time.Time{}
	}
	if _, ok := s.died[id]; !ok {
		s.died[id] = time.Now()
	}
}

// DiedAt reports when a worker process was killed or exited. ok is false if
// it is still running.
func (s *Supervisor) DiedAt(id string) (at time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok = s.died[id]
	return
}

func (s *Supervisor) Kills() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kills
}

func (s *Supervisor) Snapshot() []SlotInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SlotInfo, len(s.slots))
	for i, sl := range s.slots {
		out[i] = SlotInfo{Slot: i, ID: sl.id, PID: sl.cmd.Process.Pid, Alive: sl.alive}
	}
	return out
}

// Stop asks every worker to shut down gracefully and waits for them.
func (s *Supervisor) Stop() {
	s.mu.Lock()
	s.stopping = true
	for _, sl := range s.slots {
		if sl.alive {
			sl.cmd.Process.Signal(syscall.SIGCONT) // in case it is frozen
			sl.cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.mu.Lock()
		for _, sl := range s.slots {
			if sl.alive {
				sl.cmd.Process.Kill()
			}
		}
		s.mu.Unlock()
	}
}
