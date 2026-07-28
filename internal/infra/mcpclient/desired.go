package mcpclient

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

func NewDesiredServer(
	executable string,
	configPath string,
	configExplicit bool,
) (DesiredServer, error) {
	command, err := canonicalExecutable(executable, runtime.GOOS)
	if err != nil {
		return DesiredServer{}, fmt.Errorf("%w", ErrInvalidExecutable)
	}

	args := []string{"mcp"}
	if configExplicit {
		config, configErr := canonicalConfig(configPath)
		if configErr != nil {
			return DesiredServer{}, fmt.Errorf("%w", ErrInvalidConfig)
		}
		args = append(args, "--config", config)
	}

	return DesiredServer{
		Name:    ServerName,
		Command: command,
		Args:    args,
	}, nil
}

func AddArgs(client Client, desired DesiredServer) []string {
	switch client {
	case ClientCodex:
		return append(
			[]string{"mcp", "add", ServerName, "--", desired.Command},
			desired.Args...,
		)
	case ClientClaudeCode:
		return append(
			[]string{
				"mcp", "add", "--transport", "stdio", "--scope", "user",
				ServerName, "--", desired.Command,
			},
			desired.Args...,
		)
	default:
		panic("validated client enum")
	}
}

func CodexGetArgs() []string {
	return []string{"mcp", "get", "--json", ServerName}
}

func RemoveArgs(client Client) []string {
	switch client {
	case ClientCodex:
		return []string{"mcp", "remove", ServerName}
	case ClientClaudeCode:
		return []string{"mcp", "remove", "--scope", "user", ServerName}
	default:
		panic("validated client enum")
	}
}

func ManualCommand(client Client, desired DesiredServer, goos string) string {
	command := ""
	switch client {
	case ClientCodex:
		command = "codex"
	case ClientClaudeCode:
		command = "claude"
	default:
		panic("validated client enum")
	}

	argv := append([]string{command}, AddArgs(client, desired)...)
	quote := quotePOSIXDisplay
	if goos == "windows" {
		quote = quoteWindowsDisplay
	}
	for index := range argv {
		argv[index] = quote(argv[index])
	}
	return strings.Join(argv, " ")
}

func canonicalConfig(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) {
		return "", ErrInvalidConfig
	}
	canonical, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return "", ErrInvalidConfig
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		return "", ErrInvalidConfig
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.Mode().IsRegular() {
		return "", ErrInvalidConfig
	}
	return filepath.Clean(canonical), nil
}

func quotePOSIXDisplay(argument string) string {
	if argument != "" && strings.IndexFunc(argument, func(character rune) bool {
		return !isPOSIXSafe(character)
	}) == -1 {
		return argument
	}
	return "'" + strings.ReplaceAll(argument, "'", "'\"'\"'") + "'"
}

func isPOSIXSafe(character rune) bool {
	switch {
	case character >= 'a' && character <= 'z':
		return true
	case character >= 'A' && character <= 'Z':
		return true
	case character >= '0' && character <= '9':
		return true
	}
	return strings.ContainsRune("_@%+=:,./-", character)
}

func quoteWindowsDisplay(argument string) string {
	if argument != "" && !strings.ContainsAny(argument, " \t\n\v\"") {
		return argument
	}

	var quoted strings.Builder
	quoted.WriteByte('"')
	backslashes := 0
	for _, character := range argument {
		switch character {
		case '\\':
			backslashes++
		case '"':
			quoted.WriteString(strings.Repeat("\\", backslashes*2+1))
			quoted.WriteRune(character)
			backslashes = 0
		default:
			quoted.WriteString(strings.Repeat("\\", backslashes))
			quoted.WriteRune(character)
			backslashes = 0
		}
	}
	quoted.WriteString(strings.Repeat("\\", backslashes*2))
	quoted.WriteByte('"')
	return quoted.String()
}
