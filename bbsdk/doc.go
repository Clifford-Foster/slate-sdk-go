// Package bbsdk is the Go component SDK: the library a compiled Go component uses to speak the
// component-to-sidecar boundary. It presents the sidecar's loopback data plane as a Go client, the
// activation delivery legs in both modes, the one result envelope, and the supervisor's injected
// configuration. It is a library, not a framework: it starts nothing the component did not ask it
// to start.
//
// Contract: contracts/bb_sdk_go.md
package bbsdk
