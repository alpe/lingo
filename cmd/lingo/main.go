package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/substratusai/lingo/pkg/autoscaler"
	"github.com/substratusai/lingo/pkg/deployments"
	"github.com/substratusai/lingo/pkg/endpoints"
	"github.com/substratusai/lingo/pkg/leader"
	"github.com/substratusai/lingo/pkg/proxy"
	"github.com/substratusai/lingo/pkg/queue"
	"github.com/substratusai/lingo/pkg/stats"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}

func getEnvInt(key string, defaultValue int) int {
	if envVar := os.Getenv(key); envVar != "" {
		val, err := strconv.Atoi(envVar)
		if err != nil {
			log.Fatalf("invalid value for %s: %v", key, err)
		}
		return val
	}
	return defaultValue
}

func run() error {
	namespace := os.Getenv("NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	concurrency := getEnvInt("CONCURRENCY", 100)
	scaleDownDelay := getEnvInt("SCALE_DOWN_DELAY", 30)

	var metricsAddr string
	var probeAddr string
	var concurrencyPerReplica int
	var maxRetriesOnErr int
	var bodySizeLimit int64

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8082", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.IntVar(&concurrencyPerReplica, "concurrency", concurrency, "the number of simultaneous requests that can be processed by each replica")
	flag.IntVar(&scaleDownDelay, "scale-down-delay", scaleDownDelay, "seconds to wait before scaling down")
	flag.IntVar(&maxRetriesOnErr, "max-retries", 0, "max number of retries on a http error code: 502,503,504")
	flag.Int64Var(&bodySizeLimit, "max-body-size", 64*1024, "max number of bytes allowed for a request body")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// TODO: Add Deployments to cache list.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				namespace: {},
			},
		},
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		// LeaderElection is done in the Autoscaler.
		LeaderElection: false,
	})
	if err != nil {
		return fmt.Errorf("starting manager: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("clientset: %w", err)
	}
	ctx := ctrl.SetupSignalHandler()

	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("getting hostname: %v", err)
	}

	le := leader.NewElection(clientset, hostname, namespace)

	queueManager := queue.NewManager(concurrencyPerReplica)
	metricsRegistry := prometheus.WrapRegistererWithPrefix("lingo_", metrics.Registry)
	queue.NewMetricsCollector(queueManager).MustRegister(metricsRegistry)

	endpointManager, err := endpoints.NewManager(mgr, queueManager.UpdateQueueSizeForReplicas)
	if err != nil {
		return fmt.Errorf("setting up endpoint manager: %w", err)
	}
	// The autoscaling leader will scrape other lingo instances.
	// Exclude this instance from being scraped by itself.
	endpointManager.ExcludePods[hostname] = struct{}{}

	deploymentManager, err := deployments.NewManager(mgr)
	if err != nil {
		return fmt.Errorf("setting up deloyment manager: %w", err)
	}
	deploymentManager.Namespace = namespace
	deploymentManager.ScaleDownPeriod = time.Duration(scaleDownDelay) * time.Second
	deployments.NewMetricsCollector(deploymentManager).MustRegister(metricsRegistry)
	if err := mgr.AddReadyzCheck("readyz", deploymentManager.ReadinessChecker); err != nil {
		return fmt.Errorf("setup readiness handler: %w", err)
	}

	autoscaler, err := autoscaler.New(mgr)
	if err != nil {
		return fmt.Errorf("setting up autoscaler: %w", err)
	}
	autoscaler.Interval = 3 * time.Second
	autoscaler.AverageCount = 10 // 10 * 3 seconds = 30 sec avg
	autoscaler.LeaderElection = le
	autoscaler.Deployments = deploymentManager
	autoscaler.ConcurrencyPerReplica = concurrencyPerReplica
	autoscaler.Queues = queueManager
	autoscaler.Endpoints = endpointManager
	go autoscaler.Start()

	proxy.MustRegister(metricsRegistry)
	var proxyHandler http.Handler = proxy.NewHandler(deploymentManager, endpointManager, queueManager)
	proxyHandler = proxy.NewRetryMiddleware(maxRetriesOnErr, proxyHandler)
	proxyHandler = proxy.LimitBodySizeMiddleware(proxyHandler, bodySizeLimit)
	proxyServer := &http.Server{Addr: ":8080", Handler: proxyHandler}

	statsHandler := &stats.Handler{
		Queues: queueManager,
	}
	statsServer := &http.Server{Addr: ":8083", Handler: statsHandler}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			statsServer.Shutdown(context.Background())
			proxyServer.Shutdown(context.Background())
			wg.Done()
		}()
		if err := mgr.Start(ctx); err != nil {
			setupLog.Error(err, "problem running manager")
			os.Exit(1)
		}
	}()
	go func() {
		if err := statsServer.ListenAndServe(); err != nil {
			setupLog.Error(err, "error serving stats")
			os.Exit(1)
		}
	}()
	go func() {
		setupLog.Info("Starting leader election")
		le.Start(ctx)
	}()
	defer func() {
		setupLog.Info("waiting on manager to stop")
		wg.Wait()
		setupLog.Info("manager stopped")
	}()

	if ok := mgr.GetCache().WaitForCacheSync(ctx); !ok {
		return fmt.Errorf("client cache could not be synced")
	}
	if err := deploymentManager.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap deloyment manager: %w", err)
	}

	if err := proxyServer.ListenAndServe(); err != nil {
		return fmt.Errorf("listen and serve: %w", err)
	}

	return nil
}
