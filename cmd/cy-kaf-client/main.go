// Command cy-kaf-client runs either the development CLI server or the Go
// sidecar supervised by the native desktop shell.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	mcpIntent := len(os.Args) > 1 && os.Args[1] == "mcp"
	if mcpIntent {
		configureMCPLogging(false, os.Stderr)
	}

	command, err := parseCommand(os.Args[1:], os.Stderr)
	if err != nil {
		exitCode := optionsErrorExitCode(err)
		if exitCode != 0 {
			if mcpIntent {
				logMCPExit(mcpFailure("OPTIONS_INVALID", err))
				os.Exit(exitCode)
			}
			slog.Error("参数错误", "err", err)
			os.Exit(exitCode)
		}
		return
	}

	if command.MCP != nil {
		configureMCPLogging(command.MCP.Debug, os.Stderr)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := runMCP(
			ctx,
			*command.MCP,
			&mcp.StdioTransport{},
			productionMCPRuntimeDeps(),
		); err != nil {
			logMCPExit(err)
			os.Exit(1)
		}
		return
	}

	configureLogging(command.HTTP.Debug)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *command.HTTP, os.Stdin, os.Stdout, productionRuntimeDeps()); err != nil {
		slog.Error("cy-kaf-client 退出", "err", err)
		os.Exit(1)
	}
}

func configureLogging(debug bool) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))
}
