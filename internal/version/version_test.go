package version

import "testing"

func TestInfoDefaults(t *testing.T) {
	got := Info()
	if got.Version != "dev" || got.Commit != "unknown" || got.BuildTime != "unknown" {
		t.Fatalf("unexpected defaults: %+v", got)
	}
}
