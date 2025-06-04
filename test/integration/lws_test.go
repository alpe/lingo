package integration

import (
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestLWSSetup(t *testing.T) {
	sysCfg := baseSysCfg(t)
	initTest(t, sysCfg)
	m := modelForTest(t)
	m.Spec.ResourceProfile = resourceProfileCPU + ":1:2"
	require.NoError(t, testK8sClient.Create(testCtx, m))

	totalBackendRequests := &atomic.Int32{}
	testModelBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		totalBackendRequests.Add(1)
		w.WriteHeader(200)
	}))
	updateModelWithBackend(t, m, testModelBackend)

	// NOTE: Update to 1 min replicas needs to happen after the Model is updated with the backend because
	// updates to annotations will not be propogated to Pods if the Model is updated after the Pods are created.
	updateModel(t, m, func() {
		m.Spec.MinReplicas = 1
	}, "Set min replicas to 1")

	markAllModelPodsReady(t, m)

	// Send OpenAI-API requests.

	selectors := []string{modelLabelSelectorForTest(t)}

	// logPods(t)

	//requireOpenAIModelList(t, selectors, []string{
	//	m.Name,
	//	apiutils.MergeModelAdapter(m.Name, adapter1),
	//	apiutils.MergeModelAdapter(m.Name, adapter2),
	//}, "Model list should contain the model and its adapters")

	sendOpenAIInferenceRequest(t, m.Name, selectors, http.StatusOK, "", "inference request 1")
	//sendOpenAIInferenceRequest(t, apiutils.MergeModelAdapter(m.Name, adapter1), selectors, http.StatusOK, "", "inference request 2 to adapter1")
	require.Equal(t, int32(1), totalBackendRequests.Load(), "Adapter should not be loaded yet")
}
