package modelcontroller

import (
	"context"
	"errors"
	"fmt"
	kubeaiv1 "github.com/substratusai/kubeai/api/k8s/v1"
	"github.com/substratusai/kubeai/internal/k8sutils"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"math"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"strconv"
	"strings"
)

const LabelGroupRole = "kubeai.org/group-role"
const (
	GroupRoleHead   = "head"
	GroupRoleWorker = "worker"
)

// calculateLWSPlan calculates the LeaderWorkerSet plan for the given Model.
// It assumes the list of Pods represents an accurate snapshot of the current state.
// It returns a plan that contains LeaderWorkerSets to create and delete.
func (r *ModelReconciler) calculateLWSPlan(ctx context.Context, model *kubeaiv1.Model, config ModelConfig) (ExecutablePlan, error) {
	if config.LWSConfig.groupSize < 2 {
		return nil, errors.New("LWS group size must be greater than 1")
	}
	//logger := log.FromContext(ctx)

	plan := &LSWPlan{model: model, details: make([]string, 0)}
	lws := new(lwsv1.LeaderWorkerSet)
	lwsKey := apitypes.NamespacedName{Name: lwsName(model), Namespace: model.Namespace}
	if err := r.Client.Get(context.Background(), lwsKey, lws); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get LeaderWorkerSet: %w", err)
		}
		lwsTemplate, err := r.buildLeaderWorkerSet(model, config)
		if err != nil {
			return nil, fmt.Errorf("building LeaderWorkerSet: %w", err)
		}
		lws = lwsTemplate
		plan.toCreateLWS = append(plan.toCreateLWS, lws)
	}

	model.Status.Replicas.All = lws.Status.Replicas
	model.Status.Replicas.Ready = lws.Status.ReadyReplicas

	var desiredReplicas int32
	// NOTE: Replicas could be nil if autoscaling is disabled.
	if model.Spec.Replicas != nil {
		desiredReplicas = *model.Spec.Replicas
	}
	observedReplicas := *lws.Spec.Replicas
	replicaDiff := observedReplicas - desiredReplicas
	replicaDiffAbs := int32(math.Abs(float64(replicaDiff)))
	switch {
	case replicaDiff < 0:
		// Create replicas
		plan.details = append(plan.details, fmt.Sprintf("Scaling up from %d to %d. Adding %d LWS groups", observedReplicas, desiredReplicas, replicaDiffAbs))
		lws.Spec.Replicas = ptr.To(desiredReplicas)
		plan.toScaleLWS = append(plan.toScaleLWS, lws)
	case replicaDiff > 0:
		// Delete replicas
		//plan.details = append(plan.details, fmt.Sprintf("Scaling down to %d groups", replicaDiffAbs))
		plan.details = append(plan.details, fmt.Sprintf("Scaling down from %d to %d. Adding %d LWS groups", observedReplicas, desiredReplicas, replicaDiffAbs))
		lws.Spec.Replicas = ptr.To(desiredReplicas)
		plan.toScaleLWS = append(plan.toScaleLWS, lws)
	default: // at the correct scale, do nothing
	}
	return plan, nil
}

func lwsName(model *kubeaiv1.Model) string {
	return model.Name
}

// LSWPlan represents a plan for LeaderWorkerSets to create and delete.
type LSWPlan struct {
	model       *kubeaiv1.Model
	toCreateLWS []*lwsv1.LeaderWorkerSet
	toScaleLWS  []*lwsv1.LeaderWorkerSet
	toDeleteLWS []*lwsv1.LeaderWorkerSet
	details     []string
}

// execute implements the ExecutablePlan interface.
// It creates and deletes LeaderWorkerSets according to the plan.
// It returns the added and removed Pods, which will be empty for LeaderWorkerSets.
func (lp *LSWPlan) execute(ctx context.Context, k8sClient client.Client, scheme *runtime.Scheme) (added, removed []*corev1.Pod, err error) {
	logger := log.FromContext(ctx)
	detailsCSV := strings.Join(lp.details, ", ")
	logger.Info("Executing LeaderWorkerSet plan", "modelName", lp.model.Name, "details", detailsCSV, "to-delete", len(lp.toDeleteLWS))

	// Delete LeaderWorkerSets
	for _, lws := range lp.toDeleteLWS {
		logger.Info("Deleting LeaderWorkerSet", "name", lws.Name)
		if err := k8sClient.Delete(ctx, lws); err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("LeaderWorkerSet already deleted", "name", lws.Name)
			} else {
				return nil, nil, fmt.Errorf("deleting LeaderWorkerSet: %w", err)
			}
		}
	}

	// Create LeaderWorkerSets
	for _, lws := range lp.toCreateLWS {
		logger.Info("Creating LeaderWorkerSet", "name", lws.Name)

		if err := ctrl.SetControllerReference(lp.model, lws, scheme); err != nil {
			return nil, nil, fmt.Errorf("setting controller reference for LeaderWorkerSet: %w", err)
		}
		if err := k8sClient.Create(ctx, lws, k8sutils.DefaultCreateOptions()); err != nil {
			if apierrors.IsAlreadyExists(err) {
				logger.Info("LeaderWorkerSet already exists", "name", lws.Name)
			} else {
				return nil, nil, fmt.Errorf("creating LeaderWorkerSet: %w", err)
			}
		}
	}

	// Update LeaderWorkerSets
	for _, lws := range lp.toScaleLWS {
		logger.Info("Updating LeaderWorkerSet", "name", lws.Name)
		scale := &autoscalingv1.Scale{
			Spec: autoscalingv1.ScaleSpec{Replicas: *lws.Spec.Replicas},
		}
		if err := k8sClient.SubResource("scale").Update(ctx, lws, client.WithSubResourceBody(scale)); err != nil {
			return nil, nil, fmt.Errorf("update scale: %w", err)
		}
	}

	return nil, nil, nil
}

func (r *ModelReconciler) buildLeaderWorkerSet(model *kubeaiv1.Model, modelConfig ModelConfig) (*lwsv1.LeaderWorkerSet, error) {
	var podForModel *corev1.Pod
	switch model.Spec.Engine {
	case kubeaiv1.VLLMEngine:
		podForModel = r.vLLMPodForModel(model, modelConfig)
	default:
		return nil, errors.New("only VLLM supported currently")
	}

	if err := applyJSONPatchToPod(r.ModelServerPods.JSONPatches, podForModel); err != nil {
		return nil, err
	}

	manifestName := lwsName(model)
	lbs := labelsForModel(model)
	ann := map[string]string{
		"kubeai.org/tensor-parallel-size":   strconv.Itoa(modelConfig.LWSConfig.tensorParallel),
		"kubeai.org/pipeline-parallel-size": strconv.Itoa(modelConfig.LWSConfig.groupSize),
	}

	headPod := podForModel.DeepCopy()
	headPod.ObjectMeta.Labels[LabelGroupRole] = GroupRoleHead
	headPod.Spec.Containers[0].Env = append(headPod.Spec.Containers[0].Env,
		corev1.EnvVar{Name: "LWS_GROUP_SIZE", Value: strconv.Itoa(modelConfig.LWSConfig.groupSize)},
		corev1.EnvVar{Name: "LWS_HEAD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		corev1.EnvVar{Name: "K8S_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		//corev1.EnvVar{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}}, //used by fake-gpu
	)
	args := append(headPod.Spec.Containers[0].Args,
		fmt.Sprintf("--tensor-parallel-size=%d", modelConfig.LWSConfig.tensorParallel),
		fmt.Sprintf("--pipeline_parallel_size=%d", modelConfig.LWSConfig.groupSize),
	)
	_ = args
	if false { // revisit when GPU test env is available
		headPod.Spec.Containers[0].Command = []string{
			"sh",
			"-c",
			`bash /vllm-workspace/examples/online_serving/multi-node-serving.sh leader --ray_address="$(LWS_HEAD_NAME).pod.${K8S_NAMESPACE}.cluster.local"" --ray_cluster_size=$(LWS_GROUP_SIZE); ` +
				headPod.Spec.Containers[0].Command[0] + " " + strings.Join(args, " "),
		}
		headPod.Spec.Containers[0].Args = []string{} // clear args, they are joined into the command
	}

	// setup worker
	workerPod := podForModel.DeepCopy()
	workerPod.ObjectMeta.Labels[LabelGroupRole] = GroupRoleWorker
	delete(workerPod.ObjectMeta.Labels, "model") // only head pod has this label
	workerPod.Spec.Containers[0].Env = append(workerPod.Spec.Containers[0].Env,
		corev1.EnvVar{Name: "LWS_HEAD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: fmt.Sprintf("metadata.annotations['%s']", lwsv1.LeaderPodNameAnnotationKey)}}},
		//corev1.EnvVar{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}}, //used by fake-gpu
	)
	if false { // revisit when GPU test env is available
		workerPod.Spec.Containers[0].Command = []string{
			"sh",
			"-c",
			"bash /vllm-workspace/examples/online_serving/multi-node-serving.sh worker --ray_address=$(LWS_LEADER_ADDRESS)",
		}
		workerPod.Spec.Containers[0].Args = []string{}
	}

	// Create the LeaderWorkerSet
	lws := &lwsv1.LeaderWorkerSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "leaderworkerset.x-k8s.io/v1",
			Kind:       "LeaderWorkerSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        manifestName,
			Namespace:   model.Namespace,
			Labels:      lbs,
			Annotations: ann,
		},
		Spec: lwsv1.LeaderWorkerSetSpec{
			Replicas: model.Spec.Replicas,
			RolloutStrategy: lwsv1.RolloutStrategy{Type: lwsv1.RollingUpdateStrategyType, RollingUpdateConfiguration: &lwsv1.RollingUpdateConfiguration{
				MaxUnavailable: intstr.IntOrString{IntVal: 1},
				MaxSurge:       intstr.IntOrString{IntVal: 0},
			}},
			StartupPolicy: lwsv1.LeaderCreatedStartupPolicy, // start workers immediately after the leader pod is created.
			NetworkConfig: &lwsv1.NetworkConfig{SubdomainPolicy: ptr.To(lwsv1.SubdomainUniquePerReplica)},
			LeaderWorkerTemplate: lwsv1.LeaderWorkerTemplate{
				RestartPolicy: lwsv1.NoneRestartPolicy,
				Size:          ptr.To(int32(modelConfig.LWSConfig.groupSize)),
				LeaderTemplate: &corev1.PodTemplateSpec{
					ObjectMeta: headPod.ObjectMeta,
					Spec:       headPod.Spec,
				},
				WorkerTemplate: corev1.PodTemplateSpec{
					ObjectMeta: workerPod.ObjectMeta,
					Spec:       workerPod.Spec,
				},
			},
		},
	}
	return lws, nil
}

var _ ExecutablePlan = NoopExecutablePlan{}

type NoopExecutablePlan struct{}

func (n NoopExecutablePlan) execute(ctx context.Context, client client.Client, scheme *runtime.Scheme) (added, removed []*corev1.Pod, err error) {
	return nil, nil, nil
}
