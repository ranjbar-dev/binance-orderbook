package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/yourorg/binance-orderbook/config"
	"github.com/yourorg/binance-orderbook/orderbook"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	logger, err := newLogger(cfg.LogLevel)
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	logger.Info("Application start", zap.String("symbol", cfg.Symbol), zap.String("dataPath", cfg.DataPath))

	if err := os.MkdirAll(filepath.Dir(cfg.DataPath), 0o755); err != nil {
		logger.Error("Fatal error", zap.NamedError("err", err))
		os.Exit(1)
	}

	book := orderbook.NewBook(cfg.Symbol)
	fetcher := orderbook.NewSnapshotFetcher(cfg.RESTBaseURL)
	persister := orderbook.NewPersister(
		cfg.DataPath,
		time.Duration(cfg.FlushIntervalMS)*time.Millisecond,
		book.Snapshot,
		logger,
	)
	manager := orderbook.NewManager(cfg, logger, book, fetcher, persister)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runErr := manager.Run(ctx)
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		logger.Error("Fatal error", zap.NamedError("err", runErr))
	}

	if err := persister.FlushNow(); err != nil {
		logger.Error("Fatal error", zap.NamedError("err", err))
		os.Exit(1)
	}

	logger.Info("shutdown complete")
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		os.Exit(1)
	}
}

func newLogger(level string) (*zap.Logger, error) {
	config := zap.NewProductionConfig()
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, err
	}
	config.Level = zap.NewAtomicLevelAt(lvl)
	return config.Build()
}
