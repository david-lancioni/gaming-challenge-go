package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dlancioni/backend-challenge-go/internal/app"
	"github.com/dlancioni/backend-challenge-go/internal/config"
	"github.com/dlancioni/backend-challenge-go/internal/infra/messaging"
	"github.com/dlancioni/backend-challenge-go/internal/infra/postgres"
	"github.com/dlancioni/backend-challenge-go/migrations"
)

func main() {
	args := os.Args[1:]
	cmd := "serve"
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "serve":
		err = serve()
	case "migrate":
		err = migrate(args)
	case "queues":
		err = queues(args)
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

const usage = `usage:
  wagering [serve]
  wagering migrate up | down [N] | status
  wagering queues init
`

func serve() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	a := app.New(cfg)
	if err := a.Err(); err != nil {
		return err
	}
	a.Run()
	return nil
}

func migrate(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("migrate needs a subcommand: up | down [N] | status")
	}
	cfg, err := config.LoadUnvalidated()
	if err != nil {
		return err
	}
	if cfg.Database.URL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	m, err := connectMigrator(ctx, cfg.Database.URL)
	if err != nil {
		return err
	}
	defer m.Close(context.Background())

	switch args[0] {
	case "up":
		applied, err := m.Up(ctx)
		for _, v := range applied {
			fmt.Printf("applied migration %04d\n", v)
		}
		if err == nil && len(applied) == 0 {
			fmt.Println("database is up to date")
		}
		return err
	case "down":
		steps := 1
		if len(args) > 1 {
			if steps, err = strconv.Atoi(args[1]); err != nil || steps < 1 {
				return fmt.Errorf("invalid number of steps %q", args[1])
			}
		}
		reverted, err := m.Down(ctx, steps)
		for _, v := range reverted {
			fmt.Printf("reverted migration %04d\n", v)
		}
		return err
	case "status":
		list, err := m.Applied(ctx)
		if err != nil {
			return err
		}
		for _, a := range list {
			fmt.Printf("%04d_%s\n", a.Version, a.Name)
		}
		if len(list) == 0 {
			fmt.Println("no migration applied")
		}
		return nil
	}
	return fmt.Errorf("unknown migrate subcommand %q", args[0])
}

func connectMigrator(ctx context.Context, url string) (*postgres.Migrator, error) {
	var lastErr error
	for i := 0; i < 30; i++ {
		m, err := postgres.NewMigrator(ctx, url, migrations.FS)
		if err == nil {
			return m, nil
		}
		lastErr = err
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

func queues(args []string) error {
	if len(args) == 0 || args[0] != "init" {
		return fmt.Errorf("queues needs the subcommand: init")
	}
	cfg, err := config.LoadUnvalidated()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := messaging.NewClient(ctx, cfg.AWS)
	if err != nil {
		return err
	}
	var urls messaging.QueueURLs
	for i := 0; ; i++ {
		urls, err = messaging.EnsureQueues(ctx, client, cfg.AWS.Endpoint, cfg.Queues)
		if err == nil {
			break
		}
		if i >= 30 {
			return err
		}
		fmt.Fprintf(os.Stderr, "waiting for SQS: %v\n", err)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fmt.Printf("input queue:  %s\ndlq:          %s\nevents queue: %s\n", urls.Input, urls.InputDLQ, urls.Events)
	return nil
}
