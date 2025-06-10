package modelcontroller

import (
	"context"
	"errors"
	"fmt"
	kubeaiv1 "github.com/substratusai/kubeai/api/k8s/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type AdaptersPodReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	recorder      record.EventRecorder
	AdapterSource interface {
		reconcileAdapters(ctx context.Context, pods []*corev1.Pod, adapters []kubeaiv1.Adapter) error
	}
}

func (r *AdaptersPodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("adapters-pod-controller").
		For(&corev1.Pod{}).
		//WithOptions(controller.Options{NeedLeaderElection: ptr.To(false)}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 5, // todo (Alex): what is a good value? make configurable?
		}).
		Complete(r)
}

func (r *AdaptersPodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	modelName, ok := pod.Labels[kubeaiv1.PodModelLabel]
	if !ok {
		return ctrl.Result{}, nil
	}
	var model kubeaiv1.Model
	if err := r.Get(ctx, types.NamespacedName{Name: modelName, Namespace: req.Namespace}, &model); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	logger.Info("Reconciling pod adapters", "name", req.NamespacedName, "model", modelName)
	if err := r.AdapterSource.reconcileAdapters(ctx, []*corev1.Pod{&pod}, model.Spec.Adapters); err != nil {
		if errors.Is(err, errReturnEarly) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("reconciling adapters: %w", err)
	}

	return ctrl.Result{}, nil
}
