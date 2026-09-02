// Package schema is the Go SDK's code-first shape surface: it derives a draft 2020-12 document from
// a struct type, decodes a board value into that type, writes the committed schemas.gen.json artifact
// the toolchain reads, and emits struct models from documents authored schema-first.
//
// Contract: contracts/bb_sdk_go.md
package schema
