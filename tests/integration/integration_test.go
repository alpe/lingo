package integration

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	disv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

func TestScaleUpAndDown(t *testing.T) {
	const modelName = "test-model-a"
	deploy := testDeployment(modelName)

	require.NoError(t, testK8sClient.Create(testCtx, deploy))

	backendComplete := make(chan struct{})

	backendRequests := &atomic.Int32{}
	testBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("Serving request from testBackend")
		backendRequests.Add(1)
		<-backendComplete
		w.WriteHeader(200)
	}))

	// Mock an EndpointSlice.
	withMockEndpointSlice(t, testBackend, modelName)

	// Wait for deployment mapping to sync.
	time.Sleep(3 * time.Second)

	// Send request number 1
	var wg sync.WaitGroup
	sendRequests(t, &wg, modelName, 1, http.StatusOK)

	requireDeploymentReplicas(t, deploy, 1)
	require.Equal(t, int32(1), backendRequests.Load(), "ensure the request made its way to the backend")
	completeRequests(backendComplete, 1)

	// Ensure the deployment scaled scaled past 1.
	// 1/2 should be admitted
	// 1/2 should remain in queue
	sendRequests(t, &wg, modelName, 2, http.StatusOK)
	requireDeploymentReplicas(t, deploy, 2)

	// Make sure deployment will not be scaled past default max (3).
	sendRequests(t, &wg, modelName, 2, http.StatusOK)
	requireDeploymentReplicas(t, deploy, 3)

	// Have the mock backend respond to the remaining 4 requests.
	completeRequests(backendComplete, 4)

	// Ensure scale-down.
	requireDeploymentReplicas(t, deploy, 0)

	t.Logf("Waiting for wait group")
	wg.Wait()
}

func TestHandleModelUndeployment(t *testing.T) {
	const modelName = "test-model-b"
	deploy := testDeployment(modelName)

	require.NoError(t, testK8sClient.Create(testCtx, deploy))

	backendComplete := make(chan struct{})

	backendRequests := &atomic.Int32{}
	testBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("Serving request from testBackend")
		backendRequests.Add(1)
		<-backendComplete
		w.WriteHeader(200)
	}))

	// Mock an EndpointSlice.
	withMockEndpointSlice(t, testBackend, modelName)

	// Wait for deployment mapping to sync.
	time.Sleep(3 * time.Second)

	// Send request number 1
	var wg sync.WaitGroup
	// send single request to scale up and block on the handler to build a queue
	sendRequests(t, &wg, modelName, 1, http.StatusOK)

	requireDeploymentReplicas(t, deploy, 1)
	require.Equal(t, int32(1), backendRequests.Load(), "ensure the request made its way to the backend")
	// Add some more requests to the queue but with 404 expected
	// because the deployment is deleted before un-queued
	sendRequests(t, &wg, modelName, 2, http.StatusNotFound)

	require.NoError(t, testK8sClient.Delete(testCtx, deploy))

	// Check that the deployment was deleted
	err := testK8sClient.Get(testCtx, client.ObjectKey{
		Namespace: deploy.Namespace,
		Name:      deploy.Name,
	}, deploy)

	// ErrNotFound is desired since we delete the resource earlier
	assert.True(t, apierrors.IsNotFound(err))
	// release blocked request
	completeRequests(backendComplete, 1)

	// Wait for deployment mapping to sync.
	require.Eventually(t, func() bool {
		return queueManager.TotalCounts()[modelName+"-deploy"] == 0
	}, 3*time.Second, 100*time.Millisecond)

	t.Logf("Waiting for wait group")
	wg.Wait()
}

func TestRetryMiddleware(t *testing.T) {
	const modelName = "test-model-c"
	deploy := testDeployment(modelName)
	require.NoError(t, testK8sClient.Create(testCtx, deploy))

	// Wait for deployment mapping to sync.
	time.Sleep(3 * time.Second)
	backendRequests := &atomic.Int32{}
	var serverCodes []int
	testBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expBody := []byte(fmt.Sprintf(`{"model": %q}`, modelName))
		gotBody, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, expBody, gotBody)

		i := backendRequests.Add(1)
		code := serverCodes[i-1]
		t.Logf("Serving request from testBackend: %d; code: %d\n", i, code)
		w.WriteHeader(code)
		_, err = w.Write([]byte(strconv.Itoa(code)))
		require.NoError(t, err)
	}))

	// Mock an EndpointSlice.
	withMockEndpointSlice(t, testBackend, modelName)

	specs := map[string]struct {
		serverCodes    []int
		header         []tuple
		expResultCode  int
		expResultBody  string
		expBackendHits int32
	}{
		"max retries - succeeds": {
			serverCodes:    []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusOK},
			expResultCode:  http.StatusOK,
			expResultBody:  "200",
			expBackendHits: 4,
		},
		"max retries - fails": {
			serverCodes:    []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusBadGateway},
			expResultCode:  http.StatusBadGateway,
			expResultBody:  "502",
			expBackendHits: 4,
		},
		"non retryable error code": {
			serverCodes:    []int{http.StatusNotImplemented},
			expResultCode:  http.StatusNotImplemented,
			expResultBody:  "501",
			expBackendHits: 1,
		},
		"200 status code": {
			serverCodes:    []int{http.StatusOK},
			expResultCode:  http.StatusOK,
			expResultBody:  "200",
			expBackendHits: 1,
		},
		"200 status code - model header": {
			serverCodes:    []int{http.StatusOK},
			header:         []tuple{{k: "X-Model", v: modelName}},
			expResultCode:  http.StatusOK,
			expResultBody:  "200",
			expBackendHits: 1,
		},
		"503 with model header": {
			serverCodes:    []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable},
			header:         []tuple{{k: "X-Model", v: modelName}},
			expResultCode:  http.StatusServiceUnavailable,
			expResultBody:  "503",
			expBackendHits: 4,
		},
	}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			// setup
			serverCodes = spec.serverCodes
			backendRequests.Store(0)

			// when single request sent
			gotBody := <-sendRequest(t, &sync.WaitGroup{}, modelName, spec.expResultCode, spec.header...)
			// then only the last body is written
			assert.Equal(t, spec.expResultBody, gotBody)
			require.Equal(t, spec.expBackendHits, backendRequests.Load(), "ensure backend hit")
		})
	}
}

func withMockEndpointSlice(t *testing.T, testBackend *httptest.Server, modelName string) {
	testBackendURL, err := url.Parse(testBackend.URL)
	require.NoError(t, err)
	testBackendPort, err := strconv.Atoi(testBackendURL.Port())
	require.NoError(t, err)
	require.NoError(t, testK8sClient.Create(testCtx,
		endpointSlice(
			modelName,
			testBackendURL.Hostname(),
			int32(testBackendPort),
		),
	))
}

func requireDeploymentReplicas(t *testing.T, deploy *appsv1.Deployment, n int32) {
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		err := testK8sClient.Get(testCtx, types.NamespacedName{Namespace: deploy.Namespace, Name: deploy.Name}, deploy)
		assert.NoError(t, err, "getting the deployment")
		assert.NotNil(t, deploy.Spec.Replicas, "scale-up should have occurred")
		assert.Equal(t, n, *deploy.Spec.Replicas, "scale-up should have occurred")
	}, 3*time.Second, time.Second/2, "waiting for the deployment to be scaled up")
}

func sendRequests(t *testing.T, wg *sync.WaitGroup, modelName string, n int, expCode int) {
	for i := 0; i < n; i++ {
		sendRequest(t, wg, modelName, expCode)
	}
}

type tuple struct {
	k, v string
}

func sendRequest(t *testing.T, wg *sync.WaitGroup, modelName string, expCode int, headers ...tuple) <-chan string {
	t.Helper()
	wg.Add(1)
	bodyRespChan := make(chan string, 1)
	go func() {
		defer wg.Done()
		defer close(bodyRespChan)

		body := []byte(fmt.Sprintf(`{"model": %q}`, modelName))
		req, err := http.NewRequest(http.MethodPost, testServer.URL, bytes.NewReader(body))
		requireNoError(err)
		for _, e := range headers {
			req.Header.Add(e.k, e.v)
		}

		res, err := testHTTPClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, expCode, res.StatusCode)
		got, err := io.ReadAll(res.Body)
		_ = res.Body.Close()
		require.NoError(t, err)
		bodyRespChan <- string(got)
	}()
	return bodyRespChan
}

func completeRequests(c chan struct{}, n int) {
	for i := 0; i < n; i++ {
		c <- struct{}{}
	}
}

func testDeployment(name string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-deploy",
			Namespace: testNamespace,
			Labels: map[string]string{
				"app": name,
			},
			Annotations: map[string]string{
				"lingo.substratus.ai/models": name,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "model",
							Image: "some-model:1.2.3",
						},
					},
				},
			},
		},
	}
}

func endpointSlice(name, ip string, port int32) *disv1.EndpointSlice {
	return &disv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-deploy",
			Namespace: testNamespace,
			Labels: map[string]string{
				disv1.LabelServiceName: name + "-deploy",
			},
		},
		AddressType: disv1.AddressTypeIPv4,
		Endpoints: []disv1.Endpoint{
			{
				// Address is not used, see TestMain() where this is re-written at request-time.
				Addresses: []string{"10.0.0.10"},
				Conditions: disv1.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
			},
		},
		Ports: []disv1.EndpointPort{
			{
				Name:     ptr.To("http"),
				Port:     ptr.To(port),
				Protocol: ptr.To(corev1.ProtocolTCP),
			},
		},
	}
}
