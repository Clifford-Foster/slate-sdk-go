// Contract: contracts/sidecar.md — Part B rules B10 to B12, the pull activation stream and its ack.

package bbsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// The reconnect ladder's numbers (rule K19): they are anti-stampede behavior for a fleet recovering
// from a shared outage, so they are a platform property pinned by contract, not a local default.
const (
	defaultBackoffBase    = 500 * time.Millisecond
	defaultBackoffCap     = 15 * time.Second
	defaultJitterFraction = 0.2
)

// streamEventActivation is the one SSE frame name the pull stream carries; every other name — the
// retired event frame of an older sidecar included — is skipped (rules K15a, S10, G8).
const streamEventActivation = "activation"

// Backoff is the pull consumer's reconnect ladder; zero fields take rule K19's defaults.
type Backoff struct {
	// Base is the first wait, doubling with each consecutive failure.
	Base time.Duration
	// Cap bounds the doubled wait before jitter is spread across it.
	Cap time.Duration
	// JitterFraction is the fraction each wait is spread by, either way.
	JitterFraction float64
	// Rand returns a fraction in [0,1); it is the jitter's test seam.
	Rand func() float64
	// Sleep waits for d, reporting false when ctx is cancelled first; it is the ladder's test seam.
	Sleep func(ctx context.Context, d time.Duration) bool
}

// delay returns the wait for a consecutive-failure count: base x 2ⁿ, capped, then jittered (rule K19).
func (b Backoff) delay(attempt int) time.Duration {
	base, ceiling := b.Base, b.Cap
	if base <= 0 {
		base = defaultBackoffBase
	}
	if ceiling <= 0 {
		ceiling = defaultBackoffCap
	}
	delay := base
	for i := 0; i < attempt && delay < ceiling; i++ {
		delay *= 2
	}
	if delay > ceiling {
		delay = ceiling
	}
	fraction := b.JitterFraction
	if fraction <= 0 {
		fraction = defaultJitterFraction
	}
	random := b.Rand
	if random == nil {
		random = rand.Float64
	}
	// A fraction drawn at 0.5 spreads by nothing, which is how a test observes the bare ladder.
	spread := 1 + fraction*(2*random()-1)
	if spread < 0 {
		spread = 0
	}
	return time.Duration(float64(delay) * spread)
}

// wait sleeps for the attempt's delay, reporting false when the loop was cancelled instead.
func (b Backoff) wait(ctx context.Context, attempt int) bool {
	sleep := b.Sleep
	if sleep == nil {
		sleep = sleepContext
	}
	return sleep(ctx, b.delay(attempt))
}

// sleepContext waits for d unless ctx is cancelled first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// PullConsumer is the pull mode's activation stream plus its ack, and nothing else (rule K15).
type PullConsumer struct {
	client *Client
	config map[string]any
}

// NewPullConsumer builds a pull consumer sharing the data-plane client's options (rules S1, S2, K15).
func NewPullConsumer(opts ...Option) (*PullConsumer, error) {
	client, err := NewClient(opts...)
	if err != nil {
		return nil, err
	}
	config, err := ComponentConfig()
	if err != nil {
		return nil, err
	}
	return &PullConsumer{client: client, config: config}, nil
}

// Activations opens the activation stream (rules K15, B10).
func (p *PullConsumer) Activations(ctx context.Context) (*ActivationStream, error) {
	// The stream runs on the caller's context alone: no per-call budget is attached, so no
	// response-read deadline can end a connection the sidecar is keeping alive with comments (rule S2).
	request, err := http.NewRequestWithContext(
		ctx, http.MethodGet, p.client.transport.baseURL+"/v1/activations/stream", nil)
	if err != nil {
		return nil, fmt.Errorf("bbsdk: building the activation stream request: %w", err)
	}
	response, err := p.client.transport.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("bbsdk: opening the activation stream: %w", err)
	}
	if !succeeded(response.StatusCode) {
		payload, readErr := io.ReadAll(response.Body)
		if closeErr := response.Body.Close(); closeErr != nil && readErr == nil {
			readErr = closeErr
		}
		if readErr != nil {
			return nil, fmt.Errorf("bbsdk: reading the activation stream's error body: %w", readErr)
		}
		return nil, parseError(response.StatusCode, payload, false)
	}
	return &ActivationStream{body: response.Body, reader: newSSEReader(response.Body), consumer: p}, nil
}

// Ack completes an activation with the rule-K5 result envelope (rules K15, K17).
func (p *PullConsumer) Ack(ctx context.Context, a *Activation, writes Writes) error {
	result, err := a.Result(writes)
	if err != nil {
		return err
	}
	return p.ack(ctx, a.ActivationID, result)
}

// Close releases the transport resources the consumer owns; closing is idempotent (rules S16, K15).
func (p *PullConsumer) Close() error {
	return p.client.Close()
}

// ack posts one result envelope to the activation's ack path (sidecar.md rule B11).
func (p *PullConsumer) ack(ctx context.Context, activationID string, result Result) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("bbsdk: encoding the ack envelope: %w", err)
	}
	target := p.client.transport.baseURL + "/v1/activations/" + url.PathEscape(activationID) + "/ack"
	status, response, err := p.client.transport.call(
		ctx, http.MethodPost, target, encoded, p.client.transport.budget)
	if err != nil {
		return fmt.Errorf("bbsdk: acking %q: %w", activationID, err)
	}
	if status == http.StatusConflict {
		// ACTIVATION_EXPIRED: the activation already timed out or completed, so there is nothing to
		// do and nothing to report (rule K17).
		return nil
	}
	if !succeeded(status) {
		return parseError(status, response, false)
	}
	return nil
}

// ActivationStream is one open activation stream connection (rule K15).
type ActivationStream struct {
	body      io.ReadCloser
	reader    *sseReader
	consumer  *PullConsumer
	closeOnce sync.Once
	closeErr  error
}

// Next returns the next activation, io.EOF when the stream ends cleanly (rules K15, K15a). The
// stream carries activations only: the retired event frame (rule K15a) reaching it from an older
// sidecar is skipped exactly as any unknown frame name is, and is never acked.
func (s *ActivationStream) Next(ctx context.Context) (*Activation, error) {
	// A stream's lifetime is the context Activations opened it with — the connection is what a
	// blocked read is waiting on — so this context is the caller's cancellation check, never a
	// response deadline (rule S2). Its error is returned rather than swallowed (rule G2).
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		frame, err := s.reader.next()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("bbsdk: reading the activation stream: %w", err)
		}
		if frame.event != streamEventActivation {
			// An unknown frame name is skipped without error: the SDK never fails a component
			// because the sidecar grew — or, here, still carries — a frame (rules G8, S10, K15a).
			continue
		}
		// Under single_flight several subject activations may be in flight at once, so this is not a
		// one-at-a-time queue; a redelivery after a reconnect arrives with its original id and reaches
		// the handler again (rules B10, B39, K20).
		return newActivation(decodeActivation([]byte(frame.data)), s.consumer.client, s.consumer.config), nil
	}
}

// Close closes this stream connection; closing is idempotent (rule K15).
func (s *ActivationStream) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.body.Close() })
	return s.closeErr
}

// pullOptions is RunPull's whole configuration: the consumer's client options plus the loop's own.
type pullOptions struct {
	client  []Option
	backoff Backoff
	onError func(error)
}

// PullOption configures the RunPull loop (rules K16, K18, K19).
type PullOption func(*pullOptions) error

// WithClientOptions supplies the client options RunPull builds its consumer with (rules S1, K16).
func WithClientOptions(opts ...Option) PullOption {
	return func(o *pullOptions) error {
		o.client = append(o.client, opts...)
		return nil
	}
}

// WithBackoff sets the reconnect ladder RunPull climbs; zero fields take rule K19's defaults.
func WithBackoff(backoff Backoff) PullOption {
	return func(o *pullOptions) error {
		o.backoff = backoff
		return nil
	}
}

// WithErrorHook receives every error RunPull absorbs: a handler failure, an ack failure, a stream drop (rule K18).
func WithErrorHook(hook func(error)) PullOption {
	return func(o *pullOptions) error {
		o.onError = hook
		return nil
	}
}

// The event-handler option is retired with the event frame (rule K15a); an event frame from an older
// sidecar is skipped by Next like any unknown name, and no ack is issued for it.

// RunPull runs the whole pull loop — connect, dispatch, ack, reconnect — until ctx is done (rules K16 to K19).
func RunPull(ctx context.Context, h ActivationFunc, opts ...PullOption) error {
	settings := pullOptions{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&settings); err != nil {
			return err
		}
	}
	consumer, err := NewPullConsumer(settings.client...)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := consumer.Close(); closeErr != nil {
			report(settings.onError, closeErr)
		}
	}()
	attempt := 0
	for ctx.Err() == nil {
		delivered, err := consumer.pump(ctx, h, settings.onError)
		if delivered {
			// The ladder's counter resets on a delivered activation, never merely on a connection.
			attempt = 0
		}
		if err != nil && ctx.Err() == nil {
			report(settings.onError, err)
		}
		if ctx.Err() != nil {
			break
		}
		if !settings.backoff.wait(ctx, attempt) {
			break
		}
		attempt++
	}
	return nil
}

// pump runs one stream connection to its end, reporting whether it delivered an activation — the
// signal that resets the reconnect ladder (rule K19).
func (p *PullConsumer) pump(ctx context.Context, h ActivationFunc, onError func(error)) (bool, error) {
	stream, err := p.Activations(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := stream.Close(); closeErr != nil {
			report(onError, closeErr)
		}
	}()
	delivered := false
	for {
		activation, err := stream.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return delivered, nil
			}
			return delivered, err
		}
		// The ladder's counter resets on a delivered activation (rule K19).
		delivered = true
		writes, handlerErr := h(ctx, activation)
		if handlerErr != nil {
			// A pull component that cannot answer simply does not ack: the sidecar's own budget
			// fails and traces the activation and releases the guard. The SDK fabricates no ctx.fail
			// code the component did not choose, and never exits the process (rule K18).
			report(onError, handlerErr)
			continue
		}
		result, resultErr := activation.Result(writes)
		if resultErr != nil {
			// An envelope carrying both writes and a Fail is the boundary's BAD_RESPONSE; not
			// acking it takes the same no-ack path a handler error does (rules K5, K18).
			report(onError, resultErr)
			continue
		}
		if ackErr := p.ack(ctx, activation.ActivationID, result); ackErr != nil {
			// A 422 BAD_RESPONSE leaves the activation in flight, so closing the stream and entering
			// the reconnect ladder is what gets it redelivered (rule K17).
			return delivered, ackErr
		}
	}
}
