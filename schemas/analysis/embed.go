// Package contracts embeds the versioned durable analysis schemas.
package contracts

import "embed"

// Files contains the versioned contracts used by downstream readers.
//
//go:embed *-v*.json
var Files embed.FS
