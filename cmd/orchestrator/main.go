// Command orchestrator is the perf-triage agent orchestrator: it receives
// triggers, checks memory, leases VMs from poold, dispatches `claude -p` jobs,
// distills results back into memory, and serves the dashboard contract.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Parag953/perf-agent/internal/api"
	"github.com/Parag953/perf-agent/internal/config"
	"github.com/Parag953/perf-agent/internal/dispatch"
	"github.com/Parag953/perf-agent/internal/llm"
	"github.com/Parag953/perf-agent/internal/orchestrator"
	"github.com/Parag953/perf-agent/internal/poold"
	"github.com/Parag953/perf-agent/internal/slack"
	"github.com/Parag953/perf-agent/internal/store"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmsgprefix)
	logger.SetPrefix("orchestrator ")
	cfg := config.Load()

	mem, err := store.Open(cfg.DBPath)
	if err != nil {
		logger.Fatalf("open memory store: %v", err)
	}
	defer mem.Close()

	var llmClient llm.Client
	switch cfg.LLMBackend() {
	case "api":
		llmClient = llm.NewAPIClient(cfg.AnthropicAPIKey, cfg.HaikuModel)
	case "stub":
		llmClient = llm.NewStubClient()
	default:
		llmClient = llm.NewCLIClient(cfg.ClaudeBin, cfg.HaikuModel)
	}

	pool := poold.New(cfg.PooldURL)
	disp := dispatch.New(cfg)
	hub := api.NewHub()
	notifier := slack.New(cfg.SlackBotToken, cfg.SlackChannel, logger)

	orch := orchestrator.New(cfg, pool, mem, llmClient, disp, hub, notifier, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	orch.Start(ctx)

	srv := &http.Server{
		Addr:    cfg.WebhookAddr,
		Handler: api.NewServer(orch, hub, logger).Handler(),
	}

	slackStatus := "off"
	if notifier != nil {
		slackStatus = "on(" + cfg.SlackChannel + ")"
	}
	logger.Printf("config: poold=%s dispatch=%s llm=%s(%s) workers=%d db=%s slack=%s",
		cfg.PooldURL, disp.Mode(), llmClient.Backend(), cfg.HaikuModel, cfg.Workers, cfg.DBPath, slackStatus)
	logger.Printf("listening on %s (dashboard at /, webhook at /webhook)", cfg.WebhookAddr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Print("shutting down…")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutCtx)
}
