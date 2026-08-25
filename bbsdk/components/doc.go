// Package components is the Go composite component runtime: the Component protocol a component blackbox implements, the
// decoder for compose's canonical chain document, the deploy-time communication resolution, and the runner
// that executes one traversal per activation. It is a library like the rest of the SDK — it starts no
// goroutine outside a call the caller made, opens no listener, and reads no file: the chain reaches it
// as bytes.
//
// Contract: contracts/components_runtime.md
package components
