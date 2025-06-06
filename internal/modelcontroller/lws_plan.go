package modelcontroller

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/utils/ptr"
	"math"
	"strconv"
	"strings"

	kubeaiv1 "github.com/substratusai/kubeai/api/k8s/v1"
	"github.com/substratusai/kubeai/internal/k8sutils"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

const LabelGroupRole = "kubeai.org/group-role"
const (
	GroupRoleHead   = "head"
	GroupRoleWorker = "worker"
)

// calculateLWSPlan calculates the LeaderWorkerSet plan for the given Model.
// It assumes the list of Pods represents an accurate snapshot of the current state.
// It returns a plan that contains LeaderWorkerSets to create and delete.
func (r *ModelReconciler) calculateLWSPlan(allObservedPods *corev1.PodList, model *kubeaiv1.Model, config ModelConfig) (ExecutablePlan, error) {
	if config.LWSConfig.groupSize < 2 {
		return nil, fmt.Errorf("LWS group size must be greater than 1")
	}

	var observedHeadPods []*corev1.Pod
	var groupIndex = make(map[string][]*corev1.Pod)
	for _, pod := range allObservedPods.Items {
		// Group pods by their leader worker set name
		groupName := pod.Labels[lwsv1.SetNameLabelKey]
		groupIndex[groupName] = append(groupIndex[groupName], &pod)

		if pod.Labels[LabelGroupRole] == GroupRoleHead {
			observedHeadPods = append(observedHeadPods, &pod)
		}
	}

	plan := &LSWPlan{model: model, details: make([]string, 0)}

	// If there are no pods, create a new LeaderWorkerSet
	//if len(observedHeadPods.Items) == 0 {
	//	details = append(details, "Creating LeaderWorkerSet")
	//	toCreateLWS = append(toCreateLWS, lws)
	//} else {
	// todo (Alex): check model is modified
	// In a real implementation, we would check if the existing LeaderWorkerSet needs to be updated
	// For now, we'll just assume it's up to date
	//details = append(details, "LeaderWorkerSet already exists")
	// todo (Alex): scale up, down
	var desiredReplicas int32
	// NOTE: Replicas could be nil if autoscaling is disabled.
	if model.Spec.Replicas != nil {
		desiredReplicas = *model.Spec.Replicas
	}
	//if len(outOfDate) > 0 {
	//	desiredReplicas += r.ModelRollouts.Surge
	observedReplicas := int32(len(observedHeadPods))
	replicaDiff := observedReplicas - desiredReplicas
	replicaDiffAbs := int32(math.Abs(float64(replicaDiff)))
	switch {
	case replicaDiff < 0:
		// Create Pods.
		plan.details = append(plan.details, fmt.Sprintf("Creating %d LWS", replicaDiffAbs))
		for i := int32(0); i < replicaDiffAbs; i++ {
			lws, err := r.buildLeaderWorkerSet(model, config)
			if err != nil {
				return nil, fmt.Errorf("building LeaderWorkerSet: %w", err)
			}
			plan.toCreateLWS = append(plan.toCreateLWS, lws)
		}
	case replicaDiff > 0:
		// Delete Pods.
		plan.details = append(plan.details, fmt.Sprintf("Deleting %d LWS from %d", replicaDiffAbs, len(observedHeadPods)))
		toDeleteCount := replicaDiffAbs
		var lwsList lwsv1.LeaderWorkerSetList
		if err := r.Client.List(context.Background(), &lwsList, client.MatchingLabels{"model": model.Name}, client.InNamespace(model.Namespace)); err != nil {
			if apierrors.IsNotFound(err) {
				//logger.Info("LeaderWorkerSet already deleted", "name", lws.Name)
				return plan, nil
			}
			return nil, fmt.Errorf("listing LeaderWorkerSets: %w", err)
		}

		for _, lws := range lwsList.Items {
			if toDeleteCount == 0 {
				break
			}
			groupName := lws.Labels[lwsv1.SetNameLabelKey]
			plan.toDeleteLWS = append(plan.toDeleteLWS, &lwsList.Items[0])
			plan.toDeletePods = append(plan.toDeletePods, groupIndex[groupName]...)
			toDeleteCount--
		}
	default: // at the correct scale.
	}

	return plan, nil
}

// LSWPlan represents a plan for LeaderWorkerSets to create and delete.
type LSWPlan struct {
	model        *kubeaiv1.Model
	toCreateLWS  []*lwsv1.LeaderWorkerSet
	toDeleteLWS  []*lwsv1.LeaderWorkerSet
	toDeletePods []*corev1.Pod // tracking for later use
	details      []string
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

	var addedPods []*corev1.Pod
	// Create LeaderWorkerSets
	for _, lws := range lp.toCreateLWS {
		//if err := ctrl.SetControllerReference(lp.model, lws, scheme); err != nil {
		//	return nil, nil, fmt.Errorf("setting controller reference for LeaderWorkerSet: %w", err)
		//}
		if err := k8sClient.Create(ctx, lws, k8sutils.DefaultCreateOptions()); err != nil {
			if apierrors.IsAlreadyExists(err) {
				logger.Info("LeaderWorkerSet already exists", "name", lws.Name)
			} else {
				return nil, nil, fmt.Errorf("creating LeaderWorkerSet: %w", err)
			}
		}
		// LeaderWorkerSets manage their own pods, this query might be too early though
		var lwsPods corev1.PodList
		err := k8sClient.List(ctx, &lwsPods, client.InNamespace(lws.Namespace), client.MatchingLabels{lwsv1.SetNameLabelKey: lws.Name}) // todo: by group index
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, nil, fmt.Errorf("list pods for LeaderWorkerSet: %w", err)
		}
		for _, pod := range lwsPods.Items {
			addedPods = append(addedPods, &pod)
		}
	}
	return addedPods, lp.toDeletePods, nil
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

	expectedHash := k8sutils.PodHash(podForModel.Spec)
	podForModel.GenerateName = fmt.Sprintf("model-%s-%s", model.Name, expectedHash)
	k8sutils.SetLabel(podForModel, kubeaiv1.PodHashLabel, expectedHash)

	manifestName := model.Name
	lbs := labelsForModel(model)
	ann := map[string]string{
		"kubeai.org/tensor-parallel-size":   strconv.Itoa(modelConfig.LWSConfig.tensorParallel),
		"kubeai.org/pipeline-parallel-size": strconv.Itoa(modelConfig.LWSConfig.groupSize),
	}

	leaderPod := podForModel.DeepCopy()
	leaderPod.ObjectMeta.Labels[LabelGroupRole] = GroupRoleHead
	leaderPod.Spec.Containers[0].Env = append(leaderPod.Spec.Containers[0].Env,
		corev1.EnvVar{Name: "LWS_GROUP_SIZE", Value: strconv.Itoa(modelConfig.LWSConfig.groupSize)},
		corev1.EnvVar{Name: "LWS_HEAD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		corev1.EnvVar{Name: "K8S_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		//corev1.EnvVar{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}}, //used by fake-gpu
	)
	args := append(leaderPod.Spec.Containers[0].Args,
		fmt.Sprintf("--tensor-parallel-size=%d", modelConfig.LWSConfig.tensorParallel),
		fmt.Sprintf("--pipeline_parallel_size=%d", modelConfig.LWSConfig.groupSize),
	)
	_ = args
	if false { // revisit when GPU test env is available
		leaderPod.Spec.Containers[0].Command = []string{
			"sh",
			"-c",
			//`bash /vllm-workspace/examples/online_serving/multi-node-serving.sh leader --ray_address="$(LWS_HEAD_NAME).pod.${K8S_NAMESPACE}.cluster.local"" --ray_cluster_size=$(LWS_GROUP_SIZE); ` +
			//"bash /vllm-workspace/examples/online_serving/multi-node-serving.sh leader --ray_cluster_size=$(LWS_GROUP_SIZE); " +
			//leaderPod.Spec.Containers[0].Command[0] + " " + strings.Join(args, " "),
			//"echo ${LWS_LEADER_ADDRESS}; while true; do echo \"HTTP/1.1 200 OK\n\n\" | nc -l 8000; done",
		}
		leaderPod.Spec.Containers[0].Args = []string{} // clear args, they are joined into the command
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
			//"bash /vllm-workspace/examples/online_serving/multi-node-serving.sh worker --ray_address=$(LWS_LEADER_ADDRESS)",
			"echo ${LWS_LEADER_ADDRESS}; while true; do echo \"HTTP/1.1 200 OK\n\n\" | nc -l 8000; done",
		}
		workerPod.Spec.Containers[0].Args = []string{}
	}
	// Create the LeaderWorkerSet with simplified templates
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
			Replicas:        ptr.To(int32(1)), // todo (Alex): define how to scale  model.Spec.Replicas,
			RolloutStrategy: lwsv1.RolloutStrategy{Type: lwsv1.RollingUpdateStrategyType},
			StartupPolicy:   lwsv1.LeaderCreatedStartupPolicy, // start workers immediately after the leader pod is created.

			LeaderWorkerTemplate: lwsv1.LeaderWorkerTemplate{
				Size: ptr.To(int32(modelConfig.LWSConfig.groupSize)),
				LeaderTemplate: &corev1.PodTemplateSpec{
					ObjectMeta: leaderPod.ObjectMeta,
					Spec:       leaderPod.Spec,
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
