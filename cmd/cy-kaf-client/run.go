package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	"github.com/cy-kaf/cy-kaf-client/internal/infra/config"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

const (
	desktopInitTimeout = 5 * time.Second
	shutdownTimeout    = 5 * time.Second
)

type runtimeDeps struct {
	Listen      func(network, address string) (net.Listener, error)
	OpenBrowser func(string)
}

func productionRuntimeDeps() runtimeDeps {
	return runtimeDeps{
		Listen:      net.Listen,
		OpenBrowser: openBrowser,
	}
}

func run(
	parent context.Context,
	opts cliOptions,
	stdin io.Reader,
	stdout io.Writer,
	deps runtimeDeps,
) error {
	if deps.Listen == nil {
		return fmt.Errorf("listen dependency is required")
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	sessionToken := ""
	if opts.Desktop {
		initCtx, stopInit := context.WithTimeout(ctx, desktopInitTimeout)
		var err error
		sessionToken, err = readDesktopInit(initCtx, stdin)
		stopInit()
		if err != nil {
			return desktopFailure(stdout, "INIT_INVALID", err)
		}
	}

	cfg, err := config.Load(opts.ConfigPath, opts.ConfigExplicit)
	if err != nil {
		if opts.Desktop {
			return desktopFailure(stdout, "CONFIG_INVALID", err)
		}
		return err
	}

	port := opts.Port
	if !opts.PortExplicit && cfg.Server.Port != 0 {
		port = cfg.Server.Port
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	listener, err := deps.Listen("tcp", address)
	if err != nil {
		if opts.Desktop {
			return desktopFailure(stdout, "PORT_UNAVAILABLE", err)
		}
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	defer func() { _ = listener.Close() }()

	origin, err := loopbackOrigin(listener.Addr())
	if err != nil {
		if opts.Desktop {
			return desktopFailure(stdout, "START_FAILED", err)
		}
		return err
	}

	var desktop *api.DesktopOptions
	if opts.Desktop {
		desktop = &api.DesktopOptions{
			SessionToken: sessionToken,
			Origin:       origin,
			Shutdown:     cancel,
		}
	}

	application, err := wireApplication(
		ctx,
		cfg,
		opts.ConfigPath,
		opts.ConfigExplicit,
		desktop,
	)
	if err != nil {
		if opts.Desktop {
			return desktopFailure(stdout, "START_FAILED", err)
		}
		return err
	}
	defer func() {
		cancel()
		application.Cleanup()
	}()

	server := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           application.Handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- server.Serve(listener)
	}()

	select {
	case serveErr := <-serveResult:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			if opts.Desktop {
				return desktopFailure(stdout, "START_FAILED", serveErr)
			}
			return fmt.Errorf("serve HTTP: %w", serveErr)
		}
	default:
	}

	slog.Info(
		"cy-kaf-client 启动",
		"url", origin,
		"version", version.Info().Version,
		"clusters", application.ClusterCount,
	)
	if opts.Desktop {
		if err := writeDesktopReady(stdout, origin); err != nil {
			cancel()
			_ = shutdownServer(server)
			return fmt.Errorf("write desktop ready: %w", err)
		}
		go watchDesktopParent(stdin, cancel)
	} else if !opts.NoBrowser && deps.OpenBrowser != nil {
		deps.OpenBrowser(origin)
	}

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serveResult:
		cancel()
	}

	shutdownErr := shutdownServer(server)
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", serveErr)
	}
	if shutdownErr != nil {
		return fmt.Errorf("shutdown HTTP: %w", shutdownErr)
	}
	return nil
}

func loopbackOrigin(address net.Addr) (string, error) {
	tcpAddress, ok := address.(*net.TCPAddr)
	if !ok || tcpAddress.Port < 1 || tcpAddress.Port > 65535 {
		return "", fmt.Errorf("listener did not expose a valid TCP port")
	}
	return "http://127.0.0.1:" + strconv.Itoa(tcpAddress.Port), nil
}

func shutdownServer(server *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
		return err
	}
	return nil
}

func desktopFailure(writer io.Writer, code string, cause error) error {
	if err := writeDesktopError(writer, code); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}
