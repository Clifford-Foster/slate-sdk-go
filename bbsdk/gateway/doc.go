// Package gateway is the consumer-side client for the control plane's public workspace-state API.
//
// Contract: contracts/bb_sdk_go.md — rules W1-W5.
// Contract: contracts/control_plane.md — Part A, the public API this package speaks.
//
// gateway.Client is this SDK's one credentialed surface (rule W1). A gateway is a program OUTSIDE
// the fleet — it serves its own clients and reaches a workspace's blackboard through the control
// plane's public routes with an API key as a bearer over TLS. It is not the component boundary: a
// component inside the fleet holds no credential and speaks to its own sidecar on unauthenticated
// loopback (bbsdk.Client, rule G5). The two live in separate packages because they are separate
// boundaries, and bbsdk.Client gains no credential option.
//
// # What the gateway owns, and this SDK does not (rule W5)
//
// Per-client authentication — the gateway's own user model — and per-client rate limiting: the
// platform's per-key bucket is a backstop, not the gateway's quota (control_plane.md rule H3).
// Replay dedup on the gateway's own clients' message ids. Level-shaped keys: the board holds the
// latest value of a key, never an event log, which is why Watch reports every resync. And payload
// validation before writing: an API-plane writer has no manifest, so its writes pass no sidecar
// shape gate (sidecar.md rules A31/B8); the platform's backstop is reader-side read_expectations /
// strict. CORS stays closed platform-side — the gateway serves browsers itself.
//
// # Fan-out is a documented shape, not a helper (rule W4)
//
// N workspaces are N Watch calls in N goroutines on ONE client; an errgroup is the natural form.
// The SDK starts no goroutine the caller did not:
//
//	group, ctx := errgroup.WithContext(ctx)
//	for _, workspace := range workspaces {
//		group.Go(func() error {
//			return client.Watch(ctx, workspace, nil, func(entry bbsdk.Entry) {
//				merged <- update{workspace, entry}
//			}, gateway.WatchOptions{OnResynced: func() { resynced <- workspace }})
//		})
//	}
//	err := group.Wait()
package gateway
