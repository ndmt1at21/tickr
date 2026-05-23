package tickr

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// RetentionPolicy controls how long terminal-state rows are kept.
type RetentionPolicy struct {
	Success    time.Duration // 0 => 24h
	Dead       time.Duration // 0 => 30d; set negative for "never purge DEAD"
	PurgeBatch int           // 0 => 5000
	PurgeEvery time.Duration // 0 => 1m
}

// StatsPolicy controls the QueueDepth sampler used by the metrics hook.
type StatsPolicy struct {
	Interval time.Duration // 0 disables sampling
}

// WorkerConfig configures a Worker.
type WorkerConfig struct {
	Storage  Storage
	Registry *HandlerRegistry

	WorkerID string // defaults to hostname-pid

	PollInterval   time.Duration // 0 => 100ms
	PollMaxBackoff time.Duration // 0 => 2s
	BatchSize      int           // 0 => 100
	PoolSize       int           // 0 => 32
	Lease          time.Duration // 0 => 30s
	ShutdownGrace  time.Duration // 0 => 30s

	Retention RetentionPolicy
	Stats     StatsPolicy

	Logger  Logger
	Metrics Metrics
	Tracer  Tracer

	ReclaimInterval time.Duration // 0 => 5s

	DisableReclaimer bool
	DisableJanitor   bool
}

// Worker runs the claim loop, dispatches handlers, and (when leader) runs
// the reclaimer and janitor background loops.
type Worker struct {
	cfg WorkerConfig
	eng *engine

	stopOnce sync.Once
	cancel   context.CancelFunc

	startMu sync.Mutex
	started bool
}

// NewWorker constructs a Worker. Storage and Registry are required.
func NewWorker(cfg WorkerConfig) (*Worker, error) {
	if cfg.Storage == nil {
		return nil, fmt.Errorf("tickr: WorkerConfig.Storage is required")
	}
	if cfg.Registry == nil {
		return nil, fmt.Errorf("tickr: WorkerConfig.Registry is required")
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = defaultWorkerID()
	}
	cfg.Logger = defaultedLogger(cfg.Logger)
	cfg.Metrics = defaultedMetrics(cfg.Metrics)
	cfg.Tracer = defaultedTracer(cfg.Tracer)

	eng := newEngine(engineConfig{
		storage:        cfg.Storage,
		registry:       cfg.Registry,
		workerID:       cfg.WorkerID,
		pollInterval:   cfg.PollInterval,
		pollMaxBackoff: cfg.PollMaxBackoff,
		batchSize:      cfg.BatchSize,
		poolSize:       cfg.PoolSize,
		lease:          cfg.Lease,
		shutdownGrace:  cfg.ShutdownGrace,
		logger:         cfg.Logger,
		metrics:        cfg.Metrics,
		tracer:         cfg.Tracer,
	})

	return &Worker{cfg: cfg, eng: eng}, nil
}

// Start blocks until ctx is cancelled (then drains in-flight work bounded
// by ShutdownGrace) or a fatal error occurs.
func (w *Worker) Start(ctx context.Context) error {
	w.startMu.Lock()
	if w.started {
		w.startMu.Unlock()
		return fmt.Errorf("tickr: worker already started")
	}
	w.started = true
	w.startMu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	defer cancel()

	var bgWg sync.WaitGroup
	if !w.cfg.DisableReclaimer {
		bgWg.Add(1)
		go func() { defer bgWg.Done(); w.runReclaimerLoop(ctx) }()
	}
	if !w.cfg.DisableJanitor {
		bgWg.Add(1)
		go func() { defer bgWg.Done(); w.runJanitorLoop(ctx) }()
	}
	if w.cfg.Stats.Interval > 0 {
		bgWg.Add(1)
		go func() { defer bgWg.Done(); w.runStatsLoop(ctx) }()
	}

	err := w.eng.run(ctx)
	bgWg.Wait()
	return err
}

// Stop cancels the worker's context. The blocking drain happens inside
// Start. Idempotent.
func (w *Worker) Stop(_ context.Context) error {
	w.stopOnce.Do(func() {
		if w.cancel != nil {
			w.cancel()
		}
	})
	return nil
}

// --- background loops -----------------------------------------------------

func (w *Worker) runReclaimerLoop(ctx context.Context) {
	interval := w.cfg.ReclaimInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tryReclaim(ctx)
		}
	}
}

func (w *Worker) tryReclaim(ctx context.Context) {
	acquired, unlock, err := w.cfg.Storage.TryLeaderLock(ctx, "tickr.reclaimer")
	if err != nil {
		w.cfg.Logger.Warn(ctx, "tickr: reclaimer leader lock errored", "err", err)
		return
	}
	if !acquired {
		return
	}
	defer unlock()
	n, err := w.cfg.Storage.ReclaimExpired(ctx, 500)
	if err != nil {
		w.cfg.Logger.Error(ctx, "tickr: reclaim failed", err)
		return
	}
	if n > 0 {
		w.cfg.Metrics.LeaseReclaimed("", int(n))
		w.cfg.Logger.Info(ctx, "tickr: reclaimed expired leases", "count", n)
	}
}

func (w *Worker) runJanitorLoop(ctx context.Context) {
	interval := w.cfg.Retention.PurgeEvery
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tryPurge(ctx)
		}
	}
}

func (w *Worker) tryPurge(ctx context.Context) {
	acquired, unlock, err := w.cfg.Storage.TryLeaderLock(ctx, "tickr.janitor")
	if err != nil {
		w.cfg.Logger.Warn(ctx, "tickr: janitor leader lock errored", "err", err)
		return
	}
	if !acquired {
		return
	}
	defer unlock()

	successAge := w.cfg.Retention.Success
	if successAge <= 0 {
		successAge = 24 * time.Hour
	}
	deadAge := w.cfg.Retention.Dead
	batch := w.cfg.Retention.PurgeBatch
	if batch <= 0 {
		batch = 5000
	}

	successCutoff := time.Now().Add(-successAge)
	for {
		n, err := w.cfg.Storage.PurgeTerminal(ctx, successCutoff, batch)
		if err != nil {
			w.cfg.Logger.Error(ctx, "tickr: purge SUCCESS failed", err)
			break
		}
		if n < int64(batch) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if deadAge < 0 {
		return
	}
	if deadAge == 0 {
		deadAge = 30 * 24 * time.Hour
	}
	deadCutoff := time.Now().Add(-deadAge)
	for {
		n, err := w.cfg.Storage.PurgeTerminal(ctx, deadCutoff, batch)
		if err != nil {
			w.cfg.Logger.Error(ctx, "tickr: purge DEAD failed", err)
			break
		}
		if n < int64(batch) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (w *Worker) runStatsLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.Stats.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.sampleStats(ctx)
		}
	}
}

func (w *Worker) sampleStats(ctx context.Context) {
	acquired, unlock, err := w.cfg.Storage.TryLeaderLock(ctx, "tickr.stats")
	if err != nil {
		return
	}
	if !acquired {
		return
	}
	defer unlock()
	stats, err := w.cfg.Storage.Stats(ctx)
	if err != nil {
		w.cfg.Logger.Warn(ctx, "tickr: stats sample failed", "err", err)
		return
	}
	for eventType, byStatus := range stats.ByEventType {
		for status, depth := range byStatus {
			w.cfg.Metrics.QueueDepth(eventType, status, depth)
		}
	}
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}
