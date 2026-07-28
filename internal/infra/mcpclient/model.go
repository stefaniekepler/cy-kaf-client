package mcpclient

import "errors"

type Client string

const (
	ClientCodex      Client = "codex"
	ClientClaudeCode Client = "claude-code"
	ServerName              = "cy-kaf-client"
)

var (
	ErrClientNotFound    = errors.New("MCP client not found")
	ErrInvalidExecutable = errors.New("invalid MCP executable")
	ErrInvalidConfig     = errors.New("invalid MCP config")
)

type DesiredServer struct {
	Name    string
	Command string
	Args    []string
}

type Locator interface {
	Find(Client) (string, error)
}

type clientLaunchLocator interface {
	FindWithLaunchDirectory(Client) (string, string, error)
}
