package mcpclient

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDesiredAddArgsUseExactOfficialCLIContracts(t *testing.T) {
	desired := DesiredServer{
		Name:    ServerName,
		Command: "/Applications/Cy KafClient.app/Contents/MacOS/cy-kaf-client",
		Args:    []string{"mcp"},
	}

	require.Equal(t, []string{
		"mcp", "add", "cy-kaf-client", "--",
		"/Applications/Cy KafClient.app/Contents/MacOS/cy-kaf-client", "mcp",
	}, AddArgs(ClientCodex, desired))
	require.Equal(t, []string{
		"mcp", "add", "--transport", "stdio", "--scope", "user",
		"cy-kaf-client", "--",
		"/Applications/Cy KafClient.app/Contents/MacOS/cy-kaf-client", "mcp",
	}, AddArgs(ClientClaudeCode, desired))
	require.Equal(t, []string{"mcp", "get", "--json", "cy-kaf-client"}, CodexGetArgs())
	require.Equal(t, []string{"mcp", "remove", "cy-kaf-client"}, RemoveArgs(ClientCodex))
	require.Equal(t,
		[]string{"mcp", "remove", "--scope", "user", "cy-kaf-client"},
		RemoveArgs(ClientClaudeCode),
	)
}

func TestDesiredExplicitConfigIsCanonicalAndAppended(t *testing.T) {
	executable := writeTestFile(t, filepath.Join(t.TempDir(), "cy-kaf-client"), 0o755)
	configDir := t.TempDir()
	configTarget := writeTestFile(t, filepath.Join(configDir, "config target.yaml"), 0o600)
	configLink := filepath.Join(configDir, "config link.yaml")
	require.NoError(t, makeTestSymlink(configTarget, configLink))

	desired, err := NewDesiredServer(executable, configLink, true)
	require.NoError(t, err)
	require.Equal(t, DesiredServer{
		Name:    ServerName,
		Command: executable,
		Args:    []string{"mcp", "--config", configTarget},
	}, desired)
	require.Equal(t, []string{
		"mcp", "add", "cy-kaf-client", "--",
		executable, "mcp", "--config", configTarget,
	}, AddArgs(ClientCodex, desired))
	require.Equal(t, []string{
		"mcp", "add", "--transport", "stdio", "--scope", "user",
		"cy-kaf-client", "--",
		executable, "mcp", "--config", configTarget,
	}, AddArgs(ClientClaudeCode, desired))
}

func TestDesiredMissingDefaultConfigIsOmitted(t *testing.T) {
	executable := writeTestFile(t, filepath.Join(t.TempDir(), "cy-kaf-client"), 0o755)

	desired, err := NewDesiredServer(
		executable,
		filepath.Join(t.TempDir(), "missing-default.yaml"),
		false,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"mcp"}, desired.Args)
}

func TestDesiredRejectsInvalidExplicitConfig(t *testing.T) {
	executable := writeTestFile(t, filepath.Join(t.TempDir(), "cy-kaf-client"), 0o755)
	configDir := t.TempDir()

	for name, configPath := range map[string]string{
		"empty":     "",
		"relative":  "config.yaml",
		"missing":   filepath.Join(configDir, "missing.yaml"),
		"directory": configDir,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewDesiredServer(executable, configPath, true)
			require.ErrorIs(t, err, ErrInvalidConfig)
		})
	}
}

func TestDesiredCanonicalizesExecutableSymlink(t *testing.T) {
	dir := t.TempDir()
	target := writeTestFile(t, filepath.Join(dir, "versioned-sidecar"), 0o755)
	link := filepath.Join(dir, "cy-kaf-client")
	require.NoError(t, makeTestSymlink(target, link))

	desired, err := NewDesiredServer(link, "", false)
	require.NoError(t, err)
	require.Equal(t, target, desired.Command)
	require.True(t, filepath.IsAbs(desired.Command))
	require.Equal(t, filepath.Clean(desired.Command), desired.Command)
}

func TestDesiredRejectsUnsafeExecutableTargets(t *testing.T) {
	dir := t.TempDir()
	dangling := filepath.Join(dir, "dangling")
	require.NoError(t, makeTestSymlink(filepath.Join(dir, "missing-target"), dangling))
	directoryLink := filepath.Join(dir, "directory-link")
	require.NoError(t, makeTestSymlink(dir, directoryLink))
	nonExecutable := writeTestFile(t, filepath.Join(dir, "not-executable"), 0o600)

	cases := map[string]string{
		"relative":          "cy-kaf-client",
		"missing":           filepath.Join(dir, "missing"),
		"directory":         dir,
		"dangling symlink":  dangling,
		"directory symlink": directoryLink,
	}
	if runtime.GOOS != "windows" {
		cases["non-executable"] = nonExecutable
	}
	for name, path := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewDesiredServer(path, "", false)
			require.ErrorIs(t, err, ErrInvalidExecutable)
		})
	}
}

func TestManualCommandPOSIXQuotesSpacesAndSingleQuotesAndRoundTrips(t *testing.T) {
	desired := DesiredServer{
		Name:    ServerName,
		Command: "/Applications/Cy KafClient's App/cy-kaf-client",
		Args:    []string{"mcp", "--config", "/tmp/Ada's config.yaml"},
	}

	got := ManualCommand(ClientCodex, desired, "darwin")
	require.Equal(t,
		"codex mcp add cy-kaf-client -- '/Applications/Cy KafClient'\"'\"'s App/cy-kaf-client' mcp --config '/tmp/Ada'\"'\"'s config.yaml'",
		got,
	)
	require.Equal(t, append([]string{"codex"}, AddArgs(ClientCodex, desired)...), parsePOSIXDisplay(t, got))
}

func TestManualCommandWindowsQuotesSpacesAndQuotesAndRoundTrips(t *testing.T) {
	desired := DesiredServer{
		Name:    ServerName,
		Command: `C:\Program Files\Cy "Kaf"\cy-kaf-client.exe`,
		Args:    []string{"mcp", "--config", `C:\Users\Ada Kaf\config "dev".yaml`},
	}

	got := ManualCommand(ClientClaudeCode, desired, "windows")
	require.Equal(t,
		`claude mcp add --transport stdio --scope user cy-kaf-client -- "C:\Program Files\Cy \"Kaf\"\cy-kaf-client.exe" mcp --config "C:\Users\Ada Kaf\config \"dev\".yaml"`,
		got,
	)
	require.Equal(t,
		append([]string{"claude"}, AddArgs(ClientClaudeCode, desired)...),
		parseWindowsDisplay(got),
	)
}

func parsePOSIXDisplay(t *testing.T, command string) []string {
	t.Helper()
	var (
		args    []string
		current strings.Builder
		quoted  bool
		state   byte
	)
	flush := func() {
		if current.Len() > 0 || quoted {
			args = append(args, current.String())
			current.Reset()
			quoted = false
		}
	}
	for index := 0; index < len(command); index++ {
		char := command[index]
		switch state {
		case '\'':
			if char == '\'' {
				state = 0
			} else {
				current.WriteByte(char)
			}
		case '"':
			switch char {
			case '"':
				state = 0
			case '\\':
				index++
				require.Less(t, index, len(command))
				current.WriteByte(command[index])
			default:
				current.WriteByte(char)
			}
		default:
			switch char {
			case '\'', '"':
				quoted = true
				state = char
			case '\\':
				index++
				require.Less(t, index, len(command))
				current.WriteByte(command[index])
			case ' ', '\t':
				flush()
			default:
				current.WriteByte(char)
			}
		}
	}
	require.Zero(t, state)
	flush()
	return args
}

func parseWindowsDisplay(command string) []string {
	var args []string
	for index := 0; index < len(command); {
		for index < len(command) && (command[index] == ' ' || command[index] == '\t') {
			index++
		}
		if index == len(command) {
			break
		}
		var current strings.Builder
		inQuotes := false
		for index < len(command) {
			if !inQuotes && (command[index] == ' ' || command[index] == '\t') {
				break
			}
			slashes := 0
			for index < len(command) && command[index] == '\\' {
				slashes++
				index++
			}
			if index < len(command) && command[index] == '"' {
				current.WriteString(strings.Repeat("\\", slashes/2))
				if slashes%2 == 1 {
					current.WriteByte('"')
				} else {
					inQuotes = !inQuotes
				}
				index++
				continue
			}
			current.WriteString(strings.Repeat("\\", slashes))
			if index < len(command) {
				current.WriteByte(command[index])
				index++
			}
		}
		args = append(args, current.String())
	}
	return args
}
