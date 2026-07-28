package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type cliOptions struct {
	Port           int
	PortExplicit   bool
	ConfigPath     string
	ConfigExplicit bool
	NoBrowser      bool
	Desktop        bool
	Debug          bool
}

func defaultConfigPath() string {
	base, err := os.UserConfigDir()
	if err != nil {
		return "config.yaml"
	}
	return filepath.Join(base, "cy-kaf-client", "config.yaml")
}

func parseOptions(args []string) (cliOptions, error) {
	return parseOptionsWithOutput(args, io.Discard)
}

func parseOptionsWithOutput(args []string, output io.Writer) (cliOptions, error) {
	var opts cliOptions
	flags := flag.NewFlagSet("cy-kaf-client", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.IntVar(&opts.Port, "port", 8080, "HTTP 端口（仅绑定 127.0.0.1）")
	flags.StringVar(&opts.ConfigPath, "config", defaultConfigPath(), "配置文件路径（上游 kafka.clusters[] 同形）")
	flags.BoolVar(&opts.NoBrowser, "no-browser", false, "启动后不自动打开浏览器")
	flags.BoolVar(&opts.Desktop, "desktop", false, "由桌面客户端监督运行")
	flags.BoolVar(&opts.Debug, "debug", false, "调试日志")
	if err := flags.Parse(args); err != nil {
		return cliOptions{}, fmt.Errorf("parse options: %w", err)
	}
	if flags.NArg() != 0 {
		return cliOptions{}, fmt.Errorf("unexpected arguments")
	}
	flags.Visit(func(current *flag.Flag) {
		switch current.Name {
		case "port":
			opts.PortExplicit = true
		case "config":
			opts.ConfigExplicit = true
		}
	})
	if opts.Port < 0 || opts.Port > 65535 {
		return cliOptions{}, fmt.Errorf("port must be between 0 and 65535")
	}
	return opts, nil
}

func optionsErrorExitCode(err error) int {
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	return 2
}
