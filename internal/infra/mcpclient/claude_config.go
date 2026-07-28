package mcpclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const maxClaudeConfigBytes int64 = 8 << 20

type ConfigPathResolver interface {
	Resolve(Client) (string, error)
	ResolveTarget(Client) (*configTarget, error)
}

type ClaudeEntryReader interface {
	ReadClaude(string) (Entry, bool, error)
}

type configPathResolver struct {
	environment EnvironmentSnapshot
	userHome    string
}

type configTarget struct {
	path     string
	homePath string
	relative string
	root     *os.Root
	homeInfo os.FileInfo
}

type claudeEntryReader struct {
	maxBytes int64
}

func NewConfigPathResolver(env []string, userHome string) ConfigPathResolver {
	return NewConfigPathResolverWithSnapshot(
		newEnvironmentSnapshot(env, runtime.GOOS),
		userHome,
	)
}

func NewConfigPathResolverWithSnapshot(
	environment EnvironmentSnapshot,
	userHome string,
) ConfigPathResolver {
	if environment == nil {
		environment = newEnvironmentSnapshot(nil, runtime.GOOS)
	}
	return &configPathResolver{environment: environment, userHome: userHome}
}

func NewClaudeEntryReader(maxBytes int64) ClaudeEntryReader {
	if maxBytes <= 0 || maxBytes > maxClaudeConfigBytes {
		maxBytes = maxClaudeConfigBytes
	}
	return &claudeEntryReader{maxBytes: maxBytes}
}

func (resolver *configPathResolver) Resolve(client Client) (string, error) {
	target, err := resolver.ResolveTarget(client)
	if err != nil {
		return "", err
	}
	defer func() { _ = target.root.Close() }()
	return target.path, nil
}

func (resolver *configPathResolver) ResolveTarget(client Client) (*configTarget, error) {
	if resolver == nil {
		return nil, ErrInvalidConfig
	}
	home := resolver.userHome
	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			return nil, ErrInvalidConfig
		}
	}
	home, err := filepath.Abs(filepath.Clean(home))
	if err != nil {
		return nil, ErrInvalidConfig
	}
	home, err = filepath.EvalSymlinks(home)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	before, err := os.Lstat(home)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		return nil, ErrInvalidConfig
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	opened, err := root.Stat(".")
	after, afterErr := os.Lstat(home)
	if err != nil || afterErr != nil ||
		!os.SameFile(before, opened) || !os.SameFile(opened, after) {
		_ = root.Close()
		return nil, ErrInvalidConfig
	}

	var directory, filename string
	override := false
	switch client {
	case ClientCodex:
		directory, filename = environmentValue(resolver.environment, "CODEX_HOME"), "config.toml"
		if directory == "" {
			directory = filepath.Join(home, ".codex")
		} else {
			override = true
		}
	case ClientClaudeCode:
		directory, filename = environmentValue(resolver.environment, "CLAUDE_CONFIG_DIR"), ".claude.json"
		if directory == "" {
			directory = home
		} else {
			override = true
		}
	default:
		_ = root.Close()
		return nil, ErrInvalidConfig
	}
	if !filepath.IsAbs(directory) || directory != filepath.Clean(directory) {
		_ = root.Close()
		return nil, ErrInvalidConfig
	}
	if override && !pathBelow(home, directory) {
		_ = root.Close()
		return nil, ErrInvalidConfig
	}
	target := filepath.Join(directory, filename)
	if !pathBelow(home, target) || validateConfigComponents(home, target) != nil {
		_ = root.Close()
		return nil, ErrInvalidConfig
	}
	relative, err := filepath.Rel(home, target)
	if err != nil || relative == "." || filepath.IsAbs(relative) {
		_ = root.Close()
		return nil, ErrInvalidConfig
	}
	return &configTarget{
		path:     target,
		homePath: home,
		relative: relative,
		root:     root,
		homeInfo: opened,
	}, nil
}

func (resolver *configPathResolver) EnvironmentSnapshot() EnvironmentSnapshot {
	if resolver == nil {
		return nil
	}
	return resolver.environment
}

func environmentValue(snapshot EnvironmentSnapshot, key string) string {
	if snapshot == nil {
		return ""
	}
	if snapshot.environmentGOOS() == "windows" {
		key = strings.ToUpper(key)
	}
	prefix := key + "="
	for _, entry := range snapshot.environmentEntries() {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func pathBelow(home, target string) bool {
	relative, err := filepath.Rel(home, target)
	if err != nil || relative == "." || relative == "" || filepath.IsAbs(relative) {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func validateConfigComponents(home, target string) error {
	homeInfo, err := os.Lstat(home)
	if err != nil || homeInfo.Mode()&os.ModeSymlink != 0 || !homeInfo.IsDir() {
		return ErrInvalidConfig
	}
	relative, err := filepath.Rel(home, target)
	if err != nil {
		return ErrInvalidConfig
	}
	current := home
	parts := strings.Split(relative, string(filepath.Separator))
	for index, part := range parts {
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) {
			return nil
		}
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 {
			return ErrInvalidConfig
		}
		if index == len(parts)-1 {
			if !info.Mode().IsRegular() {
				return ErrInvalidConfig
			}
		} else if !info.IsDir() {
			return ErrInvalidConfig
		}
	}
	return nil
}

func (reader *claudeEntryReader) ReadClaude(path string) (Entry, bool, error) {
	if reader == nil || reader.maxBytes <= 0 {
		return Entry{}, false, ErrInvalidConfig
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, ErrInvalidConfig
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return Entry{}, false, ErrInvalidConfig
	}
	value, err := io.ReadAll(io.LimitReader(file, reader.maxBytes+1))
	if err != nil || int64(len(value)) > reader.maxBytes {
		return Entry{}, false, ErrInvalidConfig
	}

	top, valid := decodeObject(value, func(string) bool { return true })
	if !valid {
		return Entry{}, false, ErrInvalidConfig
	}
	rawServers := top["mcpServers"]
	if len(rawServers) == 0 {
		return Entry{}, false, nil
	}
	servers, valid := decodeObject(rawServers, func(string) bool { return true })
	if !valid {
		return Entry{}, false, ErrInvalidConfig
	}
	rawEntry := servers[ServerName]
	if len(rawEntry) == 0 || bytes.Equal(bytes.TrimSpace(rawEntry), []byte("null")) {
		return Entry{}, false, nil
	}
	fields, valid := decodeObject(rawEntry, func(string) bool { return true })
	if !valid || len(fields["type"]) == 0 {
		return Entry{}, false, ErrInvalidConfig
	}
	var transport string
	if json.Unmarshal(fields["type"], &transport) != nil || transport == "" {
		return Entry{}, false, ErrInvalidConfig
	}
	if transport != "stdio" {
		return Entry{Name: ServerName, Transport: transport, Enabled: true}, true, nil
	}
	for field := range fields {
		switch field {
		case "type", "command", "args", "env":
		default:
			return Entry{}, false, ErrInvalidConfig
		}
	}
	var command string
	var args []string
	if len(fields["command"]) == 0 ||
		len(fields["args"]) == 0 ||
		json.Unmarshal(fields["command"], &command) != nil ||
		json.Unmarshal(fields["args"], &args) != nil ||
		command == "" ||
		args == nil {
		return Entry{}, false, ErrInvalidConfig
	}
	var env map[string]string
	if len(fields["env"]) != 0 {
		var validEnv bool
		env, validEnv = decodeStringMap(fields["env"])
		if !validEnv {
			return Entry{}, false, ErrInvalidConfig
		}
	}
	return Entry{
		Name:      ServerName,
		Transport: "stdio",
		Enabled:   true,
		Command:   command,
		Args:      cloneStrings(args),
		Env:       cloneStringMap(env),
	}, true, nil
}
