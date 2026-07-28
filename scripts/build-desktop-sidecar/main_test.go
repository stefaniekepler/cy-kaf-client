package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTargetForTriple(t *testing.T) {
	cases := map[string]target{
		"x86_64-apple-darwin":    {GOOS: "darwin", GOARCH: "amd64"},
		"aarch64-apple-darwin":   {GOOS: "darwin", GOARCH: "arm64"},
		"x86_64-pc-windows-msvc": {GOOS: "windows", GOARCH: "amd64", Ext: ".exe"},
	}
	for triple, want := range cases {
		got, err := targetForTriple(triple)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	_, err := targetForTriple("x86_64-unknown-linux-gnu")
	require.Error(t, err)
}
