// Package blackboard implements the Blackboard Platform's core coordination library:
// the read/write interface to a JetStream KV bucket, the manager that owns
// blackboard lifecycle, the precondition DSL agents activate on, the agent
// activation lifecycle itself, and the component-card and health registries.
//
// Contract: contracts/blackboard_platform.md
package blackboard
