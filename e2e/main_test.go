//go:build integration

package e2e

import (
	"testing"

	"github.com/jmbarzee/temporal-agent-sandbox/e2e/harness"
)

func TestMain(m *testing.M) {
	harness.Main(m)
}
