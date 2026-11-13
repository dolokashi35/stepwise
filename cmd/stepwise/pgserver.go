package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pgServer can crash and restart the local PostgreSQL the chaos test is
// talking to. It refuses to touch anything but a cluster whose data directory
// is on this machine and owned by this user.
type pgServer struct {
	pgctl string
	data  string
}

func findPGServer(ctx context.Context, pool *pgxpool.Pool) (*pgServer, error) {
	var data string
	if err := pool.QueryRow(ctx, "SHOW data_directory").Scan(&data); err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(data, "postmaster.pid")); err != nil {
		return nil, fmt.Errorf("the database's data directory %s is not on this machine or not readable; "+
			"--db-crash-every only works against a local PostgreSQL you own (scripts/dev-db.sh)", data)
	}
	pgctl, err := exec.LookPath("pg_ctl")
	if err != nil {
		for _, dir := range []string{"/opt/homebrew/opt/postgresql@17/bin", "/usr/local/opt/postgresql@17/bin"} {
			if _, statErr := os.Stat(filepath.Join(dir, "pg_ctl")); statErr == nil {
				pgctl, err = filepath.Join(dir, "pg_ctl"), nil
				break
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("pg_ctl not found on PATH")
	}
	return &pgServer{pgctl: pgctl, data: data}, nil
}

func (p *pgServer) run(args ...string) error {
	cmd := exec.Command(p.pgctl, append([]string{"-D", p.data}, args...)...)
	cmd.Env = append(os.Environ(), "LC_ALL=en_US.UTF-8")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pg_ctl %v: %w: %s", args, err, out)
	}
	return nil
}

// Crash stops PostgreSQL with an immediate shutdown, which is the closest
// thing to pulling its plug: every backend is killed without a checkpoint and
// the server must run crash recovery from its write-ahead log on the way back
// up. It stays down for the given time, then is started again with the
// options it had before.
func (p *pgServer) Crash(down time.Duration) error {
	if err := p.run("stop", "-m", "immediate", "-w"); err != nil {
		return err
	}
	time.Sleep(down)
	return p.Start()
}

// Start brings the server up if it is down ("restart" reuses the saved
// command-line options, and simply starts the server when it is not running).
func (p *pgServer) Start() error {
	return p.run("restart", "-m", "fast", "-w", "-l", filepath.Join(p.data, "server.log"))
}
