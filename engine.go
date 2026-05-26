package tickr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// engineConfig is the worker engine's runtime config (internal).
type engineConfig struct {
	storage        Storage
	registry       *HandlerRegistry
	workerID       string
	pollInterval   time.Duration
	pollMaxBackoff time.Duration
	batchSize      int
	poolSize       int
	lease          time.Duration
	shutdownGrace  time.Duration

	// notify, when non-nil, wakes the poll loop out of its backoff
	// whenever a new message is enqueued. Coalesced; treat as hint only.
	notify <-chan struct{}

	logger  Logger
	metrics Metrics
	tracer  Tracer
	alerter Alerter
}

// engine drives the claim loop, dispatches handlers, and manages the lease
// extender for in-flight work.
type engine struct {
	cfg engineConfig

	inflight sync.WaitGroup

	perTypeMu       sync.Mutex
	perTypeInflight map[string]chan struct{}

	globalPool chan struct{}

	stopping atomic.Bool

	// succeedCh is non-nil when the storage implements BatchAcker.
	// Handler goroutines post completed message params here instead of
	// calling Succeed directly; ackFlusher batches them into one
	// transaction per flush cycle (batchSize items or 5 ms tick).
	succeedCh      chan BatchSucceedParam
	ackFlusherDone chan struct{}
}

func newEngine(cfg engineConfig) *engine {
	if cfg.poolSize <= 0 {
		cfg.poolSize = 32
	}
	e := &engine{
		cfg:             cfg,
		perTypeInflight: map[string]chan struct{}{},
		globalPool:      make(chan struct{}, cfg.poolSize),
	}
	if ba, ok := cfg.storage.(BatchAcker); ok {
		// Buffer: up to 4× the claim batch so handler goroutines never
		// block posting completions even during a slow flush.
		e.succeedCh = make(chan BatchSucceedParam, batchSizeOrDefault(cfg.batchSize)*4)
		e.ackFlusherDone = make(chan struct{})
		go e.ackFlusher(ba)
	}
	return e
}

// run drives the claim loop until ctx is cancelled, then waits for in-flight
// handlers up to ShutdownGrace.
func (e *engine) run(ctx context.Context) error {
	pollInterval := e.cfg.pollInterval
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	maxBackoff := e.cfg.pollMaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 2 * time.Second
	}

	currentDelay := pollInterval
	// Use a single reusable timer instead of time.After to avoid leaking
	// goroutines during long backoff periods.
	timer := time.NewTimer(currentDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return e.drain()
		default:
		}

		claimed := e.pollOnce(ctx)
		if claimed > 0 {
			currentDelay = pollInterval
		} else {
			currentDelay *= 2
			if currentDelay > maxBackoff {
				currentDelay = maxBackoff
			}
		}

		timer.Reset(currentDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return e.drain()
		case <-timer.C:
		case <-e.cfg.notify:
			// New work signalled — reset backoff and poll immediately.
			if !timer.Stop() {
				<-timer.C
			}
			currentDelay = pollInterval
		}
	}
}

func (e *engine) drain() error {
	e.stopping.Store(true)
	grace := e.cfg.shutdownGrace
	if grace <= 0 {
		grace = 30 * time.Second
	}
	done := make(chan struct{})
	go func() {
		// Wait for all handler goroutines to post their completions.
		// Because inflight.Done() is deferred after commitOutcome in each
		// handler, every send to succeedCh is guaranteed to happen before
		// inflight.Wait() returns — so it is safe to close the channel here.
		e.inflight.Wait()
		if e.succeedCh != nil {
			close(e.succeedCh)
			<-e.ackFlusherDone // wait for the final batch to be committed
		}
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(grace):
		return fmt.Errorf("tickr: shutdown grace exceeded with in-flight handlers")
	}
}

// ackFlusher collects BatchSucceedParam values from succeedCh and commits
// them in batches — either when the buffer reaches batchSize or after a 5 ms
// tick — whichever comes first. It exits when succeedCh is closed (by
// drain(), after all handler goroutines have finished) and does a final flush
// of any remaining items before returning.
func (e *engine) ackFlusher(ba BatchAcker) {
	defer close(e.ackFlusherDone)

	const flushInterval = 5 * time.Millisecond
	batchSize := batchSizeOrDefault(e.cfg.batchSize)
	buf := make([]BatchSucceedParam, 0, batchSize)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := ba.BatchSucceed(ctx, buf); err != nil {
			e.cfg.logger.Error(ctx, "tickr: batch succeed failed", err)
			fireAlert(e.cfg.alerter, e.cfg.logger, ctx, ErrorEvent{
				Kind: ErrorKindStorage,
				Err:  err,
			})
		}
		buf = buf[:0]
	}

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case p, ok := <-e.succeedCh:
			if !ok {
				// Channel closed — drain() has confirmed all handlers are done.
				// Flush any buffered params and exit.
				flush()
				return
			}
			buf = append(buf, p)
			if len(buf) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (e *engine) pollOnce(ctx context.Context) int {
	if e.stopping.Load() {
		return 0
	}
	eventTypes := e.cfg.registry.EventTypes()
	if len(eventTypes) == 0 {
		return 0
	}

	pollCtx, end := e.cfg.tracer.StartClaimSpan(ctx)
	start := time.Now()
	msgs, err := e.cfg.storage.Claim(pollCtx, ClaimParams{
		WorkerID:   e.cfg.workerID,
		Batch:      batchSizeOrDefault(e.cfg.batchSize),
		Lease:      leaseOrDefault(e.cfg.lease),
		EventTypes: eventTypes,
	})
	end(err)
	if err != nil {
		e.cfg.logger.Error(ctx, "tickr: claim failed", err)
		fireAlert(e.cfg.alerter, e.cfg.logger, ctx, ErrorEvent{Kind: ErrorKindClaim, Err: err})
		return 0
	}
	e.cfg.metrics.ClaimBatch(len(msgs), batchSizeOrDefault(e.cfg.batchSize), time.Since(start))

	// Partition into single-message dispatches and per-type batch groups.
	// Same-type batch messages run in one all-or-nothing handler invocation.
	var batchByType map[string][]*InboundMessage
	for _, m := range msgs {
		reg, ok := e.cfg.registry.lookup(m.Type)
		if !ok {
			e.failNoHandler(ctx, m)
			continue
		}
		if reg.batchHandler != nil {
			if batchByType == nil {
				batchByType = map[string][]*InboundMessage{}
			}
			batchByType[m.Type] = append(batchByType[m.Type], m)
			continue
		}
		e.dispatchSingle(ctx, m, reg)
	}

	for et, group := range batchByType {
		reg, _ := e.cfg.registry.lookup(et)
		e.dispatchBatches(ctx, group, reg)
	}
	return len(msgs)
}

func (e *engine) dispatchSingle(ctx context.Context, m *InboundMessage, reg registration) {
	select {
	case e.globalPool <- struct{}{}:
	case <-ctx.Done():
		e.releaseForShutdown(context.Background(), m, "shutdown before dispatch")
		return
	}

	var perType chan struct{}
	if reg.cfg.maxInflight > 0 {
		perType = e.semaphore(m.Type, reg.cfg.maxInflight)
		select {
		case perType <- struct{}{}:
		case <-ctx.Done():
			<-e.globalPool
			e.releaseForShutdown(context.Background(), m, "shutdown before dispatch")
			return
		}
	}

	e.inflight.Add(1)
	go e.runHandler(ctx, m, reg, perType)
}

func (e *engine) dispatchBatches(ctx context.Context, msgs []*InboundMessage, reg registration) {
	chunkSize := reg.cfg.maxBatchSize
	if chunkSize <= 0 || chunkSize > len(msgs) {
		chunkSize = len(msgs)
	}
	for i := 0; i < len(msgs); i += chunkSize {
		end := min(i+chunkSize, len(msgs))
		chunk := msgs[i:end]

		select {
		case e.globalPool <- struct{}{}:
		case <-ctx.Done():
			for _, m := range chunk {
				e.releaseForShutdown(context.Background(), m, "shutdown before dispatch")
			}
			continue
		}

		var perType chan struct{}
		if reg.cfg.maxInflight > 0 {
			perType = e.semaphore(chunk[0].Type, reg.cfg.maxInflight)
			select {
			case perType <- struct{}{}:
			case <-ctx.Done():
				<-e.globalPool
				for _, m := range chunk {
					e.releaseForShutdown(context.Background(), m, "shutdown before dispatch")
				}
				continue
			}
		}

		e.inflight.Add(1)
		go e.runBatchHandler(ctx, chunk, reg, perType)
	}
}

func (e *engine) semaphore(eventType string, capacity int) chan struct{} {
	e.perTypeMu.Lock()
	defer e.perTypeMu.Unlock()
	sem, ok := e.perTypeInflight[eventType]
	if !ok {
		sem = make(chan struct{}, capacity)
		e.perTypeInflight[eventType] = sem
	}
	return sem
}

func (e *engine) failNoHandler(ctx context.Context, m *InboundMessage) {
	err := fmt.Errorf("no handler registered for event type %q", m.Type)
	e.cfg.logger.Warn(ctx, "tickr: no handler", "event_type", m.Type, "message_id", m.ID)
	_ = e.cfg.storage.Fail(context.Background(), FailParams{
		MessageID: m.ID,
		Attempt:   m.Attempt,
		WorkerID:  e.cfg.workerID,
		Err:       err.Error(),
		Dead:      true,
	})
	e.cfg.metrics.MessageDead(m.Type)
	fireAlert(e.cfg.alerter, e.cfg.logger, ctx, ErrorEvent{
		Kind:      ErrorKindNoHandler,
		EventType: m.Type,
		MessageID: m.ID,
		Attempt:   m.Attempt,
		Err:       err,
	})
}

func (e *engine) releaseForShutdown(ctx context.Context, m *InboundMessage, reason string) {
	_ = e.cfg.storage.ReleaseShutdown(ctx, m.ID, m.Attempt, e.cfg.workerID, reason)
}

// runHandler dispatches one handler attempt with a derived ctx that carries
// the per-attempt timeout. A background goroutine extends the storage lease
// while the handler runs.
func (e *engine) runHandler(parentCtx context.Context, m *InboundMessage, reg registration, perType chan struct{}) {
	defer e.inflight.Done()
	defer func() { <-e.globalPool }()
	if perType != nil {
		defer func() { <-perType }()
	}

	timeout := reg.cfg.attemptTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	attemptCtx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	extenderDone := make(chan struct{})
	go e.extendLoop(parentCtx, m, attemptCtx, cancel, extenderDone)
	defer close(extenderDone)

	handlerCtx, endSpan := e.cfg.tracer.StartHandlerSpan(attemptCtx, m)

	start := time.Now()
	e.cfg.metrics.HandlerStarted(m.Type, m.Attempt)

	err := safeInvoke(reg.handler, handlerCtx, m)
	duration := time.Since(start)

	endSpan(err)

	outcome := e.resolveOutcome(parentCtx, err)
	e.cfg.metrics.HandlerCompleted(m.Type, m.Attempt, duration, outcome)

	completionCtx, completionCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer completionCancel()
	e.commitOutcome(completionCtx, m, reg.cfg, err, outcome)
}

// runBatchHandler is the all-or-nothing counterpart to runHandler: one
// invocation processes a slice of same-type messages, then the resolved
// outcome is committed once per message.
func (e *engine) runBatchHandler(parentCtx context.Context, msgs []*InboundMessage, reg registration, perType chan struct{}) {
	defer e.inflight.Done()
	defer func() { <-e.globalPool }()
	if perType != nil {
		defer func() { <-perType }()
	}

	timeout := reg.cfg.attemptTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	attemptCtx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	extenderDone := make(chan struct{})
	go e.extendBatchLoop(parentCtx, msgs, attemptCtx, cancel, extenderDone)
	defer close(extenderDone)

	// Trace span scoped to the first message; otel impl extracts traceparent
	// from headers, which won't be uniform across a batch — pick a
	// representative one. Per-message metrics still fire below.
	handlerCtx, endSpan := e.cfg.tracer.StartHandlerSpan(attemptCtx, msgs[0])

	for _, m := range msgs {
		e.cfg.metrics.HandlerStarted(m.Type, m.Attempt)
	}
	start := time.Now()

	err := safeInvokeBatch(reg.batchHandler, handlerCtx, msgs)
	duration := time.Since(start)

	endSpan(err)

	outcome := e.resolveOutcome(parentCtx, err)
	for _, m := range msgs {
		e.cfg.metrics.HandlerCompleted(m.Type, m.Attempt, duration, outcome)
	}

	completionCtx, completionCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer completionCancel()
	for _, m := range msgs {
		e.commitOutcome(completionCtx, m, reg.cfg, err, outcome)
	}
}

func (e *engine) extendBatchLoop(parentCtx context.Context, msgs []*InboundMessage, attemptCtx context.Context, cancel context.CancelFunc, done <-chan struct{}) {
	lease := leaseOrDefault(e.cfg.lease)
	interval := max(lease/3, 100*time.Millisecond)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-attemptCtx.Done():
			return
		case <-t.C:
			until := time.Now().Add(lease)
			for _, m := range msgs {
				ok, err := e.cfg.storage.Extend(parentCtx, m.ID, e.cfg.workerID, until)
				if err != nil {
					e.cfg.logger.Warn(parentCtx, "tickr: extend lease errored", "message_id", m.ID, "err", err)
					fireAlert(e.cfg.alerter, e.cfg.logger, parentCtx, ErrorEvent{
						Kind:      ErrorKindLease,
						EventType: m.Type,
						MessageID: m.ID,
						Attempt:   m.Attempt,
						Err:       err,
					})
					continue
				}
				if !ok {
					e.cfg.logger.Warn(parentCtx, "tickr: lease lost in batch, cancelling attempt", "message_id", m.ID)
					fireAlert(e.cfg.alerter, e.cfg.logger, parentCtx, ErrorEvent{
						Kind:      ErrorKindLease,
						EventType: m.Type,
						MessageID: m.ID,
						Attempt:   m.Attempt,
						Err:       fmt.Errorf("lease lost"),
					})
					cancel()
					return
				}
			}
		}
	}
}

func (e *engine) extendLoop(parentCtx context.Context, m *InboundMessage, attemptCtx context.Context, cancel context.CancelFunc, done <-chan struct{}) {
	lease := leaseOrDefault(e.cfg.lease)
	interval := max(lease/3, 100*time.Millisecond)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-attemptCtx.Done():
			return
		case <-t.C:
			ok, err := e.cfg.storage.Extend(parentCtx, m.ID, e.cfg.workerID, time.Now().Add(lease))
			if err != nil {
				e.cfg.logger.Warn(parentCtx, "tickr: extend lease errored", "message_id", m.ID, "err", err)
				fireAlert(e.cfg.alerter, e.cfg.logger, parentCtx, ErrorEvent{
					Kind:      ErrorKindLease,
					EventType: m.Type,
					MessageID: m.ID,
					Attempt:   m.Attempt,
					Err:       err,
				})
				continue
			}
			if !ok {
				e.cfg.logger.Warn(parentCtx, "tickr: lease lost, cancelling attempt", "message_id", m.ID)
				fireAlert(e.cfg.alerter, e.cfg.logger, parentCtx, ErrorEvent{
					Kind:      ErrorKindLease,
					EventType: m.Type,
					MessageID: m.ID,
					Attempt:   m.Attempt,
					Err:       fmt.Errorf("lease lost"),
				})
				cancel()
				return
			}
		}
	}
}

func (e *engine) resolveOutcome(parentCtx context.Context, err error) Outcome {
	if err == nil {
		return OutcomeSuccess
	}
	if IsSkip(err) {
		return OutcomeSuccess
	}
	if errors.Is(err, context.Canceled) && parentCtx.Err() != nil && e.stopping.Load() {
		return OutcomeCanceled
	}
	return OutcomeRetry
}

func (e *engine) commitOutcome(ctx context.Context, m *InboundMessage, cfg handlerConfig, err error, outcome Outcome) {
	switch outcome {
	case OutcomeSuccess:
		if e.succeedCh != nil {
			// Post to the flusher; it holds the blocking send so the handler
			// goroutine is unblocked as soon as the channel has room. The
			// channel is sized to 4× batchSize so this is almost never the
			// slow path. A blocking send here is intentional: it keeps
			// back-pressure on the handler pool when the flusher falls behind.
			e.succeedCh <- BatchSucceedParam{
				MessageID: m.ID,
				Attempt:   m.Attempt,
				WorkerID:  e.cfg.workerID,
			}
			return
		}
		if serr := e.cfg.storage.Succeed(ctx, m.ID, m.Attempt, e.cfg.workerID); serr != nil {
			e.cfg.logger.Error(ctx, "tickr: succeed failed", serr, "message_id", m.ID)
			fireAlert(e.cfg.alerter, e.cfg.logger, ctx, ErrorEvent{
				Kind:      ErrorKindStorage,
				EventType: m.Type,
				MessageID: m.ID,
				Attempt:   m.Attempt,
				Err:       serr,
			})
		}
	case OutcomeCanceled:
		if rerr := e.cfg.storage.ReleaseShutdown(ctx, m.ID, m.Attempt, e.cfg.workerID, "worker shutdown"); rerr != nil {
			e.cfg.logger.Error(ctx, "tickr: release-on-shutdown failed", rerr, "message_id", m.ID)
			fireAlert(e.cfg.alerter, e.cfg.logger, ctx, ErrorEvent{
				Kind:      ErrorKindStorage,
				EventType: m.Type,
				MessageID: m.ID,
				Attempt:   m.Attempt,
				Err:       rerr,
			})
		}
	case OutcomeRetry, OutcomeDead:
		dead := outcome == OutcomeDead
		if IsDeadLetter(err) {
			dead = true
		}
		if cfg.deadLetterIf != nil && cfg.deadLetterIf(err) {
			dead = true
		}
		maxAttempts := m.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = cfg.maxAttempts
		}
		if !dead && m.Attempt >= maxAttempts {
			dead = true
		}

		fail := FailParams{
			MessageID: m.ID,
			Attempt:   m.Attempt,
			WorkerID:  e.cfg.workerID,
			Err:       err.Error(),
			Dead:      dead,
		}
		if !dead {
			delay, ok := ExtractRetryAfter(err)
			if !ok {
				policy := cfg.retry
				if policy == nil {
					policy = DefaultRetryPolicy()
				}
				delay = policy.NextDelay(m.Attempt, err)
			}
			fail.NextRetryAt = time.Now().Add(delay)
		}
		if dead {
			e.cfg.metrics.MessageDead(m.Type)
		}
		if ferr := e.cfg.storage.Fail(ctx, fail); ferr != nil {
			e.cfg.logger.Error(ctx, "tickr: fail-write failed", ferr, "message_id", m.ID)
			fireAlert(e.cfg.alerter, e.cfg.logger, ctx, ErrorEvent{
				Kind:      ErrorKindStorage,
				EventType: m.Type,
				MessageID: m.ID,
				Attempt:   m.Attempt,
				Err:       ferr,
			})
		}

		kind := ErrorKindHandler
		if dead {
			kind = ErrorKindDeadLetter
		}
		fireAlert(e.cfg.alerter, e.cfg.logger, ctx, ErrorEvent{
			Kind:      kind,
			EventType: m.Type,
			MessageID: m.ID,
			Attempt:   m.Attempt,
			Err:       err,
		})
	}
}

func safeInvoke(h Handler, ctx context.Context, m *InboundMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = fmt.Errorf("tickr: handler panic: %w", e)
			} else {
				err = fmt.Errorf("tickr: handler panic: %v", r)
			}
		}
	}()
	return h(ctx, m)
}

func safeInvokeBatch(h BatchHandler, ctx context.Context, msgs []*InboundMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = fmt.Errorf("tickr: batch handler panic: %w", e)
			} else {
				err = fmt.Errorf("tickr: batch handler panic: %v", r)
			}
		}
	}()
	return h(ctx, msgs)
}

func batchSizeOrDefault(n int) int {
	if n <= 0 {
		return 100
	}
	return n
}

func leaseOrDefault(d time.Duration) time.Duration {
	if d <= 0 {
		return 30 * time.Second
	}
	return d
}
