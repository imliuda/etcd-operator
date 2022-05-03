/*
Copyright 2022 imliuda.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package etcd

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdv1alpha1 "github.com/imliuda/etcd-operator/apis/etcd/v1alpha1"
)

const (
	FinalizerName = "imliuda.github.io/etcd-operator"
)

var (
	logger = ctrl.Log.WithName("etcd-controller")
)

// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	locks sync.Map
}

//+kubebuilder:rbac:groups=etcd.imliuda.github.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=etcd.imliuda.github.io,resources=etcdclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=etcd.imliuda.github.io,resources=etcdclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the EtcdCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.10.0/pkg/reconcile
//
// Not using state machine here:
// https://maelvls.dev/kubernetes-conditions/
// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties
// Every time we enter reconcile, we use the latest Object
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger.Info("Reconcile start")
	defer logger.Info("Reconcile end")

	// Ignore delete objects
	cluster := &etcdv1alpha1.EtcdCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Only one reconcile can be execute for a cluster at the same time
	namespacedName := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	if _, ok := r.locks.LoadOrStore(namespacedName.String(), true); ok {
		return ctrl.Result{Requeue: true}, nil
	}

	desired := cluster.DeepCopy()

	// Handler finalizer
	// examine DeletionTimestamp to determine if object is under deletion
	if cluster.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted, so if it does not have our finalizer,
		// then lets add the finalizer and update the object. This is equivalent
		// registering our finalizer.
		if !controllerutil.ContainsFinalizer(desired, FinalizerName) {
			controllerutil.AddFinalizer(desired, FinalizerName)
			if err := r.Update(ctx, desired); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		// The object is being deleted
		if controllerutil.ContainsFinalizer(desired, FinalizerName) {
			// our finalizer is present, so lets handle any external dependency
			if err := r.ensureClusterDeleted(ctx, cluster); err != nil {
				return ctrl.Result{}, err
			}

			// remove our finalizer from the list and update it.
			controllerutil.RemoveFinalizer(desired, FinalizerName)
			if err := r.Update(ctx, desired); err != nil {
				return ctrl.Result{}, err
			}
		}

		// Stop reconciliation as the item is being deleted
		return ctrl.Result{}, nil
	}

	// If cluster is paused, we do nothing on things changed.
	// Until cluster is un-paused, we will reconcile to the the state of that point.
	if cluster.Spec.Paused {
		logger.Info("Cluster control has been paused: ", "cluster-name", cluster.Name)
		desired.Status.ControlPaused = true
		if err := r.Status().Patch(ctx, desired, client.MergeFrom(cluster)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// 1. Create service
	if err := r.ensureService(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Create seed pod
	if err := r.ensureSeedPod(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Create rest pods, pods number equal spec.Size
	// When this step done, the cluster may have old version pod, or have been marked as deleted
	if err := r.ensurePods(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

type ClusterFilter struct{}

func (r *ClusterFilter) Create(evt event.CreateEvent) bool {
	switch evt.Object.(type) {
	case *etcdv1alpha1.EtcdCluster:
		return true
	}
	return false
}

func (r *ClusterFilter) Update(evt event.UpdateEvent) bool {
	switch evt.ObjectNew.(type) {
	case *etcdv1alpha1.EtcdCluster:
		oldC := evt.ObjectOld.(*etcdv1alpha1.EtcdCluster)
		newC := evt.ObjectNew.(*etcdv1alpha1.EtcdCluster)

		logger.V(5).Info("Running update filter",
			"Old size", oldC.Spec.Size,
			"New size", newC.Spec.Size,
			"old paused", oldC.Spec.Paused,
			"new paused", newC.Spec.Paused,
			"old object deletion", !oldC.ObjectMeta.DeletionTimestamp.IsZero(),
			"new object deletion", !newC.ObjectMeta.DeletionTimestamp.IsZero())

		// Only care about size, version and paused fields
		if oldC.Spec.Size != newC.Spec.Size {
			return true
		}

		if oldC.Spec.Paused != newC.Spec.Paused {
			return true
		}

		if oldC.Spec.Version != newC.Spec.Version {
			return true
		}

		// If cluster has been marked as deleted, check if we have remove our finalizer
		// If it has our finalizer, indicating our cleaning up works has not been done.
		if oldC.DeletionTimestamp.IsZero() && !newC.DeletionTimestamp.IsZero() {
			if controllerutil.ContainsFinalizer(newC, FinalizerName) {
				return true
			}
		}
	case *v1.Pod:
		oldP := evt.ObjectOld.(*v1.Pod)
		newP := evt.ObjectNew.(*v1.Pod)
		if oldP.DeletionTimestamp.IsZero() && !newP.DeletionTimestamp.IsZero() {
			return true
		}
	}
	return false
}

func (r *ClusterFilter) Delete(evt event.DeleteEvent) bool {
	switch evt.Object.(type) {
	case *etcdv1alpha1.EtcdCluster:
		return true
	//case *v1.Pod:
	//	return true
	case *v1.Service:
		return true
	}
	return false
}

func (r *ClusterFilter) Generic(evt event.GenericEvent) bool {
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	filter := &ClusterFilter{}
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdv1alpha1.EtcdCluster{}).
		Owns(&v1.Pod{}).
		Owns(&v1.Service{}).
		WithEventFilter(filter).
		Complete(r)
}
