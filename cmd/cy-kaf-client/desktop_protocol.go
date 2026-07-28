package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	desktopProtocolVersion = 1
	desktopInitPrefix      = "CY_KAF_INIT "
	desktopReadyPrefix     = "CY_KAF_READY "
	desktopErrorPrefix     = "CY_KAF_ERROR "
	desktopInitMaxBytes    = 4096
	desktopSessionBytes    = 32
)

var (
	errInvalidDesktopInit = errors.New("invalid desktop initialization")
	errDesktopInitTimeout = errors.New("desktop initialization timed out")
)

type desktopInitWire struct {
	Protocol     int    `json:"protocol"`
	SessionToken string `json:"sessionToken"`
}

type desktopReadyWire struct {
	Protocol int    `json:"protocol"`
	Origin   string `json:"origin"`
}

type desktopErrorWire struct {
	Protocol int    `json:"protocol"`
	Code     string `json:"code"`
}

type desktopInitResult struct {
	token string
	err   error
}

func readDesktopInit(ctx context.Context, reader io.Reader) (string, error) {
	result := make(chan desktopInitResult, 1)
	go func() {
		token, err := scanDesktopInit(reader)
		result <- desktopInitResult{token: token, err: err}
	}()

	select {
	case <-ctx.Done():
		return "", errDesktopInitTimeout
	case got := <-result:
		return got.token, got.err
	}
}

func scanDesktopInit(reader io.Reader) (string, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024), desktopInitMaxBytes)
	if !scanner.Scan() {
		return "", errInvalidDesktopInit
	}

	line := scanner.Text()
	if !strings.HasPrefix(line, desktopInitPrefix) {
		return "", errInvalidDesktopInit
	}

	var wire desktopInitWire
	decoder := json.NewDecoder(strings.NewReader(strings.TrimPrefix(line, desktopInitPrefix)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return "", errInvalidDesktopInit
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return "", errInvalidDesktopInit
	}
	if wire.Protocol != desktopProtocolVersion {
		return "", errInvalidDesktopInit
	}

	raw, err := base64.RawURLEncoding.Strict().DecodeString(wire.SessionToken)
	if err != nil || len(raw) != desktopSessionBytes {
		return "", errInvalidDesktopInit
	}
	return wire.SessionToken, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return errInvalidDesktopInit
}

func watchDesktopParent(reader io.Reader, cancel context.CancelFunc) {
	_, _ = io.Copy(io.Discard, reader)
	cancel()
}

func writeDesktopReady(writer io.Writer, origin string) error {
	return writeDesktopMessage(writer, desktopReadyPrefix, desktopReadyWire{
		Protocol: desktopProtocolVersion,
		Origin:   origin,
	})
}

func writeDesktopError(writer io.Writer, code string) error {
	switch code {
	case "INIT_INVALID", "CONFIG_INVALID", "PORT_UNAVAILABLE", "START_FAILED":
	default:
		return fmt.Errorf("invalid desktop error code")
	}
	return writeDesktopMessage(writer, desktopErrorPrefix, desktopErrorWire{
		Protocol: desktopProtocolVersion,
		Code:     code,
	})
}

func writeDesktopMessage(writer io.Writer, prefix string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode desktop protocol message: %w", err)
	}
	if _, err := fmt.Fprintf(writer, "%s%s\n", prefix, payload); err != nil {
		return fmt.Errorf("write desktop protocol message: %w", err)
	}
	return nil
}
