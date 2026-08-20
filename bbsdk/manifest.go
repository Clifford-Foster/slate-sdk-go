package bbsdk

import "github.com/Clifford-Foster/slate-sdk-go/manifest"

// DefaultManifestPath is where a built component image carries its manifest (build_service.md Part B).
const DefaultManifestPath = "/bb/manifest.yaml"

// LoadManifest loads and validates the component's own baked manifest, for self-description (rule M2).
func LoadManifest() (*manifest.Manifest, error) {
	// The SDK parses no manifest of its own: manifest.md is implemented once, in src/manifest, and
	// judged there (rule M1). The manifest is provenance, never the SDK's configuration source and
	// never an authorization source — the environment configures, and the sidecar enforces.
	return manifest.Load(DefaultManifestPath)
}
