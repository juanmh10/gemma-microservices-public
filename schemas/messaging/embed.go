// Package contracts embeds the fixed messaging contracts and fictional recipient.
package contracts

import "embed"

//go:embed *-v*.json
var Files embed.FS
