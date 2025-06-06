package modelcontroller

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"testing"

	"github.com/stretchr/testify/assert"
	kubeaiv1 "github.com/substratusai/kubeai/api/k8s/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8testing "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

func TestLSWPlanExecute(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = lwsv1.AddToScheme(scheme)
	_ = kubeaiv1.AddToScheme(scheme)

	specs := map[string]struct {
		plan          *LSWPlan
		clientObjects []client.Object
		expAdded      int
		expRemoved    int
		expError      bool
	}{
		"No changes": {
			plan: &LSWPlan{
				model: &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model1"}},
			},
			clientObjects: []client.Object{},
			expAdded:      0,
			expRemoved:    0,
			expError:      false,
		},
		"Delete LWS successfully": {
			plan: &LSWPlan{
				model: &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model1"}},
				toDeleteLWS: []*lwsv1.LeaderWorkerSet{
					{ObjectMeta: metav1.ObjectMeta{Name: "lws1", Namespace: "default"}},
				},
				toDeletePods: []*corev1.Pod{{}},
			},
			clientObjects: []client.Object{
				&lwsv1.LeaderWorkerSet{ObjectMeta: metav1.ObjectMeta{Name: "lws1", Namespace: "default"}},
			},
			expAdded:   0,
			expRemoved: 1,
			expError:   false,
		},
		"Create LWS successfully": {
			plan: &LSWPlan{
				model: &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model1"}},
				toCreateLWS: []*lwsv1.LeaderWorkerSet{
					{ObjectMeta: metav1.ObjectMeta{Name: "lws1", Namespace: "default"}},
				},
			},
			clientObjects: []client.Object{&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default",
					Labels: map[string]string{lwsv1.SetNameLabelKey: "lws1"},
				},
			}},
			expAdded:   1,
			expRemoved: 0,
			expError:   false,
		},
		"LWS exists on creation": {
			plan: &LSWPlan{
				model: &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model1"}},
				toCreateLWS: []*lwsv1.LeaderWorkerSet{
					{ObjectMeta: metav1.ObjectMeta{Name: "lws1", Namespace: "default"}},
				},
			},
			clientObjects: []client.Object{
				&lwsv1.LeaderWorkerSet{ObjectMeta: metav1.ObjectMeta{Name: "lws1", Namespace: "default"}},
			},
			expAdded:   0,
			expRemoved: 0,
			expError:   false,
		},
		"Error while deleting nonexistent LWS": {
			plan: &LSWPlan{
				model: &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model1"}},
				toDeleteLWS: []*lwsv1.LeaderWorkerSet{
					{ObjectMeta: metav1.ObjectMeta{Name: "nonexistent", Namespace: "default"}},
				},
			},
			clientObjects: []client.Object{},
			expAdded:      0,
			expRemoved:    0,
			expError:      false,
		},
		"Delete pods successfully": {
			plan: &LSWPlan{
				model: &kubeaiv1.Model{ObjectMeta: metav1.ObjectMeta{Name: "model1"}},
				toDeletePods: []*corev1.Pod{
					{ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default"}},
				},
			},
			clientObjects: []client.Object{
				&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "default"}},
			},
			expAdded:   0,
			expRemoved: 1,
			expError:   false,
		},
	}

	for name, tt := range specs {
		t.Run(name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.clientObjects...).
				Build()
			added, removed, err := tt.plan.execute(context.Background(), fakeClient, scheme)

			if tt.expError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expAdded, len(added))
			assert.Equal(t, tt.expRemoved, len(removed))
		})
	}
}

type failingObjectTracker struct {
	k8testing.ObjectTracker
}

func (f *failingObjectTracker) Create(gvr schema.GroupVersionResource, obj runtime.Object, ns string) error {
	return fmt.Errorf("intentional create failure")
}
