// Package charts ships the Helm charts with the worker that renders them.
package charts

import "embed"

//go:embed all:sandbox
var FS embed.FS
