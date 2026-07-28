package api_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type fakeQuotaServicer struct {
	known     map[string]bool
	list      []cluster.ClientQuota
	listErr   error
	upsertErr error
	called    bool
	upserted  []cluster.ClientQuota
}

func (f *fakeQuotaServicer) resolve(name string) error {
	if f.known != nil && f.known[name] {
		return nil
	}
	return appcluster.ErrUnknownCluster
}

func (f *fakeQuotaServicer) ListQuotas(_ context.Context, name string) ([]cluster.ClientQuota, error) {
	f.called = true
	if err := f.resolve(name); err != nil {
		return nil, err
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.list, nil
}

func (f *fakeQuotaServicer) UpsertQuotas(_ context.Context, name string, quota cluster.ClientQuota) error {
	f.called = true
	if err := f.resolve(name); err != nil {
		return err
	}
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.upserted = append(f.upserted, quota)
	return nil
}

func withQuotas(f *fakeQuotaServicer) func(*api.Deps) {
	return func(d *api.Deps) { d.Quotas = f }
}

func TestListQuotas200MapsAllDimensionsAndFloatConversion(t *testing.T) {
	const domainValue = 1.234567890123
	f := &fakeQuotaServicer{
		known: map[string]bool{"c1": true},
		list: []cluster.ClientQuota{{
			User: "alice", ClientID: "billing-client", IP: "192.0.2.10",
			Quotas: map[string]float64{"producer_byte_rate": domainValue},
		}},
	}
	srv := newTestServer(withQuotas(f))
	defer srv.Close()

	var out []struct {
		User     *string             `json:"user"`
		ClientID *string             `json:"clientId"`
		IP       *string             `json:"ip"`
		Quotas   *map[string]float32 `json:"quotas"`
	}
	req, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/c1/clientquotas", &out)
	require.Equal(t, http.StatusOK, code)
	require.Len(t, out, 1)
	require.Equal(t, "alice", *out[0].User)
	require.Equal(t, "billing-client", *out[0].ClientID)
	require.Equal(t, "192.0.2.10", *out[0].IP)
	require.Equal(t, float32(domainValue), (*out[0].Quotas)["producer_byte_rate"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestListQuotas200OmitsEmptyDimensionsAndQuotas(t *testing.T) {
	f := &fakeQuotaServicer{
		known: map[string]bool{"c1": true},
		list:  []cluster.ClientQuota{{Quotas: map[string]float64{}}},
	}
	srv := newTestServer(withQuotas(f))
	defer srv.Close()

	var out []map[string]any
	req, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/c1/clientquotas", &out)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, []map[string]any{{}}, out)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpsertClientQuotas204MapsAllDimensionsAndFloatConversion(t *testing.T) {
	f := &fakeQuotaServicer{known: map[string]bool{"c1": true}}
	srv := newTestServer(withQuotas(f))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/clientquotas",
		`{"user":"alice","clientId":"billing-client","ip":"192.0.2.10","quotas":{"producer_byte_rate":1.234567}}`)
	require.Equal(t, http.StatusNoContent, code)
	require.Empty(t, body)
	require.Len(t, f.upserted, 1)
	require.Equal(t, cluster.ClientQuota{
		User: "alice", ClientID: "billing-client", IP: "192.0.2.10",
		Quotas: map[string]float64{"producer_byte_rate": float64(float32(1.234567))},
	}, f.upserted[0])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpsertClientQuotasRejectsMissingOrExplicitlyBlankEntityWithoutService(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "empty object", body: `{}`},
		{name: "quotas only", body: `{"quotas":{}}`},
		{name: "empty user", body: `{"user":""}`},
		{name: "blank client id", body: `{"clientId":" \t"}`},
		{name: "empty ip beside valid user", body: `{"user":"alice","ip":""}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeQuotaServicer{known: map[string]bool{"c1": true}}
			srv := newTestServer(withQuotas(f))
			defer srv.Close()

			req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/clientquotas", tc.body)
			require.Equal(t, http.StatusBadRequest, code)
			assertErrorEnvelope(t, body, "invalid quota request", "")
			require.False(t, f.called)
			validateResponseOnlyAgainstContract(t, req, code, hdr, body)
		})
	}
}

func TestUpsertClientQuotasBadBody400(t *testing.T) {
	f := &fakeQuotaServicer{known: map[string]bool{"c1": true}}
	srv := newTestServer(withQuotas(f))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/clientquotas", "{bad")
	require.Equal(t, http.StatusBadRequest, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
	require.False(t, f.called)
}

func TestUpsertClientQuotasRejectsNullOrMultipleJSONValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "null", body: `null`},
		{name: "multiple values", body: `{"user":"alice"}{"user":"bob"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeQuotaServicer{known: map[string]bool{"c1": true}}
			srv := newTestServer(withQuotas(f))
			defer srv.Close()

			_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/clientquotas", tc.body)
			if assert.Equal(t, http.StatusBadRequest, code) {
				assertErrorEnvelope(t, body, "invalid request body", "")
			}
			assert.False(t, f.called)
		})
	}
}

func TestUpsertClientQuotasRejectsBodyOver10MiBWithoutService(t *testing.T) {
	f := &fakeQuotaServicer{known: map[string]bool{"c1": true}}
	srv := newTestServer(withQuotas(f))
	defer srv.Close()
	huge := strings.Repeat("a", (10<<20)+1)

	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/clientquotas",
		`{"user":"`+huge+`","quotas":{}}`)
	require.Equal(t, http.StatusBadRequest, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
	require.False(t, f.called)
	validateResponseOnlyAgainstContract(t, req, code, hdr, body)
}

func TestUpsertClientQuotasMapsBadQuotaRequestFromServiceTo400(t *testing.T) {
	f := &fakeQuotaServicer{
		known:     map[string]bool{"c1": true},
		upsertErr: fmt.Errorf("%w: rejected", appcluster.ErrBadQuotaRequest),
	}
	srv := newTestServer(withQuotas(f))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/c1/clientquotas", `{"user":"alice","quotas":{}}`)
	require.Equal(t, http.StatusBadRequest, code)
	assertErrorEnvelope(t, body, "invalid quota request", "rejected")
	require.True(t, f.called)
	validateResponseOnlyAgainstContract(t, req, code, hdr, body)
}

func TestUpsertClientQuotasReadOnly403DoesNotCallService(t *testing.T) {
	f := &fakeQuotaServicer{known: map[string]bool{"ro": true}}
	srv := newTestServer(withQuotas(f), withReadOnly("ro"))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/ro/clientquotas",
		`{"user":"alice","quotas":{"producer_byte_rate":1}}`)
	require.Equal(t, http.StatusForbidden, code)
	assertErrorEnvelope(t, body, "read-only mode", "")
	require.False(t, f.called)
}

func TestQuotaUnknownCluster404(t *testing.T) {
	for _, tc := range []struct {
		name, method, body string
	}{
		{name: "list", method: http.MethodGet},
		{name: "upsert", method: http.MethodPost, body: `{"user":"alice","quotas":{"producer_byte_rate":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeQuotaServicer{}
			srv := newTestServer(withQuotas(f))
			defer srv.Close()

			var code int
			var body []byte
			if tc.method == http.MethodGet {
				_, code, _, body = doJSON(t, tc.method, srv, "/api/clusters/nope/clientquotas", nil)
			} else {
				_, code, _, body = bodyJSON(t, tc.method, srv, "/api/clusters/nope/clientquotas", tc.body)
			}
			require.Equal(t, http.StatusNotFound, code)
			assertErrorEnvelope(t, body, "cluster not found", "")
		})
	}
}

func TestQuotaBackendError500DoesNotLeakCause(t *testing.T) {
	backendErr := errors.New("kadm quota failure: broker-secret-detail")
	for _, tc := range []struct {
		name, method, body string
	}{
		{name: "list", method: http.MethodGet},
		{name: "upsert", method: http.MethodPost, body: `{"user":"alice","quotas":{"producer_byte_rate":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeQuotaServicer{known: map[string]bool{"c1": true}, listErr: backendErr, upsertErr: backendErr}
			srv := newTestServer(withQuotas(f))
			defer srv.Close()

			var code int
			var body []byte
			if tc.method == http.MethodGet {
				_, code, _, body = doJSON(t, tc.method, srv, "/api/clusters/c1/clientquotas", nil)
			} else {
				_, code, _, body = bodyJSON(t, tc.method, srv, "/api/clusters/c1/clientquotas", tc.body)
			}
			require.Equal(t, http.StatusInternalServerError, code)
			assertErrorEnvelope(t, body, "failed to", "broker-secret-detail")
		})
	}
}
