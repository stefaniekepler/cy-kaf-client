package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadDesktopInitAcceptsExactly32RandomBytes(t *testing.T) {
	token := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x5a}, 32))
	line := fmt.Sprintf("CY_KAF_INIT {\"protocol\":1,\"sessionToken\":%q}\n", token)

	got, err := readDesktopInit(context.Background(), strings.NewReader(line))

	require.NoError(t, err)
	require.Equal(t, token, got)
}

func TestReadDesktopInitRejectsMalformedOrShortTokens(t *testing.T) {
	inputs := []string{
		"not-an-init\n",
		"CY_KAF_INIT {\"protocol\":2,\"sessionToken\":\"x\"}\n",
		"CY_KAF_INIT {\"protocol\":1,\"sessionToken\":\"eA\"}\n",
		"CY_KAF_INIT {\"protocol\":1,\"sessionToken\":\"eA\",\"extra\":true}\n",
	}

	for _, input := range inputs {
		_, err := readDesktopInit(context.Background(), strings.NewReader(input))

		require.EqualError(t, err, "invalid desktop initialization")
		require.NotContains(t, err.Error(), input)
		require.NotContains(t, err.Error(), "sessionToken")
	}
}

func TestReadDesktopInitTimesOutWithoutLeakingInput(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		require.NoError(t, reader.Close())
		require.NoError(t, writer.Close())
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := readDesktopInit(ctx, reader)

	require.EqualError(t, err, "desktop initialization timed out")
}

func TestDesktopParentEOFTriggersCancel(t *testing.T) {
	reader, writer := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchDesktopParent(reader, cancel)

	require.NoError(t, writer.Close())
	require.Eventually(t, func() bool { return ctx.Err() != nil }, time.Second, 10*time.Millisecond)
	require.NoError(t, reader.Close())
}

func TestWriteDesktopReadyNeverIncludesSessionMaterial(t *testing.T) {
	var out bytes.Buffer

	require.NoError(t, writeDesktopReady(&out, "http://127.0.0.1:43127"))

	require.JSONEq(
		t,
		`{"protocol":1,"origin":"http://127.0.0.1:43127"}`,
		strings.TrimPrefix(strings.TrimSpace(out.String()), "CY_KAF_READY "),
	)
	require.NotContains(t, out.String(), "sessionToken")
}

func TestWriteDesktopErrorOnlyAllowsFixedCodes(t *testing.T) {
	for _, code := range []string{
		"INIT_INVALID",
		"CONFIG_INVALID",
		"PORT_UNAVAILABLE",
		"START_FAILED",
	} {
		var out bytes.Buffer
		require.NoError(t, writeDesktopError(&out, code))
		require.JSONEq(
			t,
			fmt.Sprintf(`{"protocol":1,"code":%q}`, code),
			strings.TrimPrefix(strings.TrimSpace(out.String()), "CY_KAF_ERROR "),
		)
	}

	var out bytes.Buffer
	require.Error(t, writeDesktopError(&out, "SECRET=do-not-print"))
	require.Empty(t, out.String())
}
