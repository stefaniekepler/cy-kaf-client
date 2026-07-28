package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestResolverLookup(t *testing.T) {
	r := appcluster.NewResolver([]cluster.Definition{{Name: "a"}, {Name: "b"}})
	d, err := r.Lookup("b")
	require.NoError(t, err)
	require.Equal(t, "b", d.Name)
	_, err = r.Lookup("nope")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
	require.Equal(t, []string{"a", "b"},
		[]string{r.Definitions()[0].Name, r.Definitions()[1].Name}) // 保序
}

func TestResolverIsReadOnly(t *testing.T) {
	r := appcluster.NewResolver([]cluster.Definition{{Name: "prod", ReadOnly: true}, {Name: "dev"}})
	require.True(t, r.IsReadOnly("prod"))
	require.False(t, r.IsReadOnly("dev"))
	require.False(t, r.IsReadOnly("nope")) // 未知集群：false，404 判定留给 handler 层
}

func TestResolverReplaceSwapsDefs(t *testing.T) {
	r := appcluster.NewResolver([]cluster.Definition{{Name: "a"}})
	_, err := r.Lookup("b")
	require.Error(t, err) // b absent initially

	r.Replace([]cluster.Definition{{Name: "b", ReadOnly: true}})

	_, err = r.Lookup("a")
	require.Error(t, err, "old cluster a must be gone after Replace")
	d, err := r.Lookup("b")
	require.NoError(t, err)
	require.Equal(t, "b", d.Name)
	require.True(t, r.IsReadOnly("b"))
	require.Equal(t, []cluster.Definition{{Name: "b", ReadOnly: true}}, r.Definitions())
}

// TestResolverReplaceIsRaceFreeUnderConcurrentReads runs Replace against a
// storm of concurrent Lookup/IsReadOnly/Definitions reads; with -race (make
// verify's default) it fails loudly if the defs swap isn't mutex-guarded.
func TestResolverReplaceIsRaceFreeUnderConcurrentReads(t *testing.T) {
	r := appcluster.NewResolver([]cluster.Definition{{Name: "a"}})
	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			_, _ = r.Lookup("a")
			_ = r.IsReadOnly("a")
			_ = r.Definitions()
		}
		close(done)
	}()
	for i := 0; i < 2000; i++ {
		r.Replace([]cluster.Definition{{Name: "a"}})
	}
	<-done
}
