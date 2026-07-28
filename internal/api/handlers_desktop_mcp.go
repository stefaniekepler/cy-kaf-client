package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/cy-kaf/cy-kaf-client/internal/app/mcpsettings"
)

const desktopMCPMaxBodyBytes int64 = 16 << 10

type desktopMCPSettingsResponse struct {
	Enabled     bool `json:"enabled"`
	AllowWrites bool `json:"allowWrites"`
}

type desktopMCPUpdateRequest struct {
	Enabled       bool `json:"enabled"`
	AllowWrites   bool `json:"allowWrites"`
	ConfirmWrites bool `json:"confirmWrites"`
}

type desktopMCPConfigureRequest struct {
	Replace bool `json:"replace"`
}

type desktopMCPIntegrationResponse struct {
	Client        mcpsettings.Client `json:"client"`
	Status        mcpsettings.Status `json:"status"`
	CanConfigure  bool               `json:"canConfigure"`
	ManualCommand string             `json:"manualCommand"`
	Message       string             `json:"message"`
}

type desktopMCPIntegrationsResponse struct {
	Clients []desktopMCPIntegrationResponse `json:"clients"`
}

type desktopMCPErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func mountDesktopMCP(router chi.Router, service DesktopMCPServicer) {
	router.Get("/__desktop/mcp-settings", func(w http.ResponseWriter, r *http.Request) {
		policy, err := service.Get(r.Context())
		if err != nil {
			writeDesktopMCPError(w, http.StatusInternalServerError, "invalid_request", "Invalid request.")
			return
		}
		writeJSON(w, http.StatusOK, desktopMCPSettings(policy))
	})
	router.Put("/__desktop/mcp-settings", func(w http.ResponseWriter, r *http.Request) {
		body, ok := decodeDesktopMCPJSON[desktopMCPUpdateRequest](w, r)
		if !ok {
			return
		}
		policy, err := service.Update(r.Context(), mcpsettings.UpdateRequest{
			Enabled:       body.Enabled,
			AllowWrites:   body.AllowWrites,
			ConfirmWrites: body.ConfirmWrites,
		})
		if err != nil {
			writeDesktopMCPServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, desktopMCPSettings(policy))
	})
	router.Get("/__desktop/mcp-integrations", func(w http.ResponseWriter, r *http.Request) {
		integrations, err := service.Integrations(r.Context())
		if err != nil {
			writeDesktopMCPServiceError(w, err)
			return
		}
		clients, ok := desktopMCPIntegrations(integrations)
		if !ok {
			writeDesktopMCPError(
				w, http.StatusBadGateway, "client_command_failed", "Client command failed.",
			)
			return
		}
		writeJSON(w, http.StatusOK, desktopMCPIntegrationsResponse{Clients: clients})
	})
	mountDesktopMCPConfigure(
		router, service, "/__desktop/mcp-integrations/codex/configure", mcpsettings.ClientCodex,
	)
	mountDesktopMCPConfigure(
		router, service, "/__desktop/mcp-integrations/claude-code/configure",
		mcpsettings.ClientClaudeCode,
	)
}

func mountDesktopMCPConfigure(
	router chi.Router,
	service DesktopMCPServicer,
	path string,
	client mcpsettings.Client,
) {
	router.Post(path, func(w http.ResponseWriter, r *http.Request) {
		body, ok := decodeDesktopMCPJSON[desktopMCPConfigureRequest](w, r)
		if !ok {
			return
		}
		integration, err := service.Configure(r.Context(), client, body.Replace)
		if err != nil {
			writeDesktopMCPServiceError(w, err)
			return
		}
		integration.Client = client
		response, valid := desktopMCPIntegration(integration)
		if !valid {
			writeDesktopMCPError(
				w, http.StatusBadGateway, "client_command_failed", "Client command failed.",
			)
			return
		}
		writeJSON(w, http.StatusOK, response)
	})
}

func mountDesktopNotFound(router chi.Router, desktop bool) {
	router.Handle("/__desktop/*", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !desktop {
			writeDesktopMCPError(w, http.StatusNotFound, "desktop_only", "Desktop only.")
			return
		}
		writeDesktopMCPError(w, http.StatusNotFound, "invalid_request", "Invalid request.")
	}))
}

func decodeDesktopMCPJSON[T any](
	w http.ResponseWriter,
	r *http.Request,
) (*T, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, desktopMCPMaxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var body *T
	if err := decoder.Decode(&body); err != nil || body == nil {
		writeDesktopMCPError(w, http.StatusBadRequest, "invalid_request", "Invalid request.")
		return nil, false
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeDesktopMCPError(w, http.StatusBadRequest, "invalid_request", "Invalid request.")
		return nil, false
	}
	return body, true
}

func desktopMCPSettings(policy mcpsettings.Policy) desktopMCPSettingsResponse {
	return desktopMCPSettingsResponse{
		Enabled: policy.Enabled, AllowWrites: policy.Enabled && policy.AllowWrites,
	}
}

func desktopMCPIntegrations(
	integrations []mcpsettings.Integration,
) ([]desktopMCPIntegrationResponse, bool) {
	responses := make([]desktopMCPIntegrationResponse, 0, len(integrations))
	for _, integration := range integrations {
		response, ok := desktopMCPIntegration(integration)
		if !ok {
			return nil, false
		}
		responses = append(responses, response)
	}
	return responses, true
}

func desktopMCPIntegration(
	integration mcpsettings.Integration,
) (desktopMCPIntegrationResponse, bool) {
	if !desktopMCPClientAllowed(integration.Client) ||
		!desktopMCPStatusAllowed(integration.Status) {
		return desktopMCPIntegrationResponse{}, false
	}
	message := ""
	if integration.Status == mcpsettings.StatusError {
		message = "Client command failed."
	}
	return desktopMCPIntegrationResponse{
		Client:        integration.Client,
		Status:        integration.Status,
		CanConfigure:  integration.CanConfigure,
		ManualCommand: integration.ManualCommand,
		Message:       message,
	}, true
}

func desktopMCPClientAllowed(client mcpsettings.Client) bool {
	return client == mcpsettings.ClientCodex || client == mcpsettings.ClientClaudeCode
}

func desktopMCPStatusAllowed(status mcpsettings.Status) bool {
	switch status {
	case mcpsettings.StatusClientNotFound,
		mcpsettings.StatusNotConfigured,
		mcpsettings.StatusConfigured,
		mcpsettings.StatusConflict,
		mcpsettings.StatusRepairRequired,
		mcpsettings.StatusError:
		return true
	default:
		return false
	}
}

func writeDesktopMCPServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, mcpsettings.ErrMCPDisabled):
		writeDesktopMCPError(w, http.StatusConflict, "mcp_disabled", "MCP is disabled.")
	case errors.Is(err, mcpsettings.ErrWriteConfirmationRequired):
		writeDesktopMCPError(
			w, http.StatusConflict, "write_confirmation_required",
			"Confirm write access before enabling it.",
		)
	case errors.Is(err, mcpsettings.ErrClientNotFound):
		writeDesktopMCPError(w, http.StatusNotFound, "client_not_found", "Client not found.")
	case errors.Is(err, mcpsettings.ErrConcurrentModification):
		writeDesktopMCPError(
			w, http.StatusConflict, "concurrent_modification",
			"Configuration changed externally.",
		)
	case errors.Is(err, mcpsettings.ErrRollbackFailed):
		writeDesktopMCPError(w, http.StatusInternalServerError, "rollback_failed", "Rollback failed.")
	case errors.Is(err, mcpsettings.ErrVerification):
		writeDesktopMCPError(
			w, http.StatusBadGateway, "verification_failed",
			"Configuration verification failed.",
		)
	case errors.Is(err, mcpsettings.ErrClientCommand):
		writeDesktopMCPError(
			w, http.StatusBadGateway, "client_command_failed", "Client command failed.",
		)
	default:
		writeDesktopMCPError(
			w, http.StatusInternalServerError, "client_command_failed", "Client command failed.",
		)
	}
}

func writeDesktopMCPError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, desktopMCPErrorResponse{Code: code, Message: message})
}
