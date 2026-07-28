package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
)

const mcpHelpText = `Usage: cy-kaf-client mcp [options]

Options:
  --config PATH  Path to the Kafka cluster configuration
  --debug        Enable debug logging on stderr
  --help         Show this help
`

type mcpOptions struct {
	ConfigPath     string
	ConfigExplicit bool
	Debug          bool
}

type command struct {
	HTTP *cliOptions
	MCP  *mcpOptions
}

func parseCommand(args []string, output io.Writer) (command, error) {
	if len(args) > 0 && args[0] == "mcp" {
		opts, err := parseMCPOptions(args[1:], output)
		if err != nil {
			return command{}, err
		}
		return command{MCP: &opts}, nil
	}
	opts, err := parseOptionsWithOutput(args, output)
	if err != nil {
		return command{}, err
	}
	return command{HTTP: &opts}, nil
}

func parseMCPOptions(args []string, output io.Writer) (mcpOptions, error) {
	var opts mcpOptions
	flags := flag.NewFlagSet("cy-kaf-client mcp", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.ConfigPath, "config", defaultConfigPath(), "Path to the Kafka cluster configuration")
	flags.BoolVar(&opts.Debug, "debug", false, "Enable debug logging on stderr")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			if _, writeErr := io.WriteString(output, mcpHelpText); writeErr != nil {
				return mcpOptions{}, fmt.Errorf("write MCP help: %w", writeErr)
			}
		}
		return mcpOptions{}, fmt.Errorf("parse MCP options: %w", err)
	}
	if flags.NArg() != 0 {
		return mcpOptions{}, fmt.Errorf("unexpected MCP arguments")
	}
	flags.Visit(func(current *flag.Flag) {
		if current.Name == "config" {
			opts.ConfigExplicit = true
		}
	})
	return opts, nil
}
