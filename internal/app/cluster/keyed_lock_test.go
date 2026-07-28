package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyedLockerReclaimsIdleEntries(t *testing.T) {
	var locker keyedLocker[string]
	for i := 0; i < 100; i++ {
		unlock := locker.lock("cluster")
		unlock()
	}
	require.Empty(t, locker.entries)
}
