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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"strings"
	"sync"
	"time"

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

	clusters sync.Map
	resyncCh chan event.GenericEvent

}

//+kubebuilder:rbac:groups=etcd.imliuda.github.io,resources=etcdclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=etcd.imliuda.github.io,resources=etcdclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=etcd.imliuda.github.io,resources=etcdclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;create;delete;list;watch
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;create;delete;list;watch
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;create;delete;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// the EtcdCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.10.0/pkg/
//
// TODO Standardize logging level and format
//
// Not using state machine here:
// https://maelvls.dev/kubernetes-conditions/
// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties
// Every time we enter reconcile, we use the latest Object
//
// Original etcd-operator use a background go routine for each cluster, by selecting channel do sync works.
//
// Controller's work queue will ensure there is only one worker for a key
func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger.V(10).Info("Reconcile start")
	defer logger.V(10).Info("Reconcile end")

	// Ignore delete objects
	cluster := &etcdv1alpha1.EtcdCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
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
			r.clusters.Delete(r.getNamespacedName(cluster))

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

	r.clusters.Store(r.getNamespacedName(cluster), cluster)

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

	// 1. First time we see the cluster, initialize it
	if cluster.Status.Phase == etcdv1alpha1.ClusterPhaseNone {
		desired.Status.Phase = etcdv1alpha1.ClusterPhaseCreating
		err := r.Client.Status().Patch(ctx, desired, client.MergeFrom(cluster))
		return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, err
	}

	// 2. If cluster has desired members, Default to ""
	if cluster.Annotations[etcdv1alpha1.ClusterMembersAnnotation] == "" {
		ms := r.specMemberSet(cluster)
		desired.Annotations[etcdv1alpha1.ClusterMembersAnnotation] = strings.Join(ms.Names(), ",")
		err := r.Client.Patch(ctx, desired, client.MergeFrom(cluster))
		return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, err
	}

	// 3. Ensure services
	logger.Info("Ensuring cluster services", "cluster", cluster.Name)
	if err := r.ensureService(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Ensure bootstrapped, we will block here util cluster is up and healthy
	// sigs.k8s.io/controller-runtime/pkg/internal/controller/controller.go
	logger.Info("Ensuring cluster members", "cluster", cluster.Name)
	//if cluster.Status.Phase == etcdv1alpha1.ClusterPhaseCreating {
	if requeue, err := r.ensureMembers(ctx, cluster); requeue {
		return ctrl.Result{RequeueAfter: time.Second}, err
	}
	//}

	// 5. Ensure cluster scaled
	logger.Info("Ensuring cluster scaled", "cluster", cluster.Name)
	if requeue, err := r.ensureScaled(ctx, cluster); requeue {
		return ctrl.Result{Requeue: true}, err
	}

	// 6. Ensure cluster upgraded
	logger.Info("Ensuring cluster upgraded", "cluster", cluster.Name)
	if requeue, err := r.ensureUpgraded(ctx, cluster); requeue {
		return ctrl.Result{Requeue: true}, err
	}

	return ctrl.Result{}, nil
}


type Predicate struct{}

// Create will be trigger when object created or controller restart
// first time see the object
func (r *Predicate) Create(evt event.CreateEvent) bool {
	switch evt.Object.(type) {
	case *etcdv1alpha1.EtcdCluster:
		return true
	}
	return false
}

func (r *Predicate) Update(evt event.UpdateEvent) bool {
	switch evt.ObjectNew.(type) {
	case *etcdv1alpha1.EtcdCluster:
		oldC := evt.ObjectOld.(*etcdv1alpha1.EtcdCluster)
		newC := evt.ObjectNew.(*etcdv1alpha1.EtcdCluster)

		logger.V(5).Info("Running update filter",
			"Old size", oldC.Spec.Replicas,
			"New size", newC.Spec.Replicas,
			"old paused", oldC.Spec.Paused,
			"new paused", newC.Spec.Paused,
			"old object deletion", !oldC.ObjectMeta.DeletionTimestamp.IsZero(),
			"new object deletion", !newC.ObjectMeta.DeletionTimestamp.IsZero())

		// Only care about size, version and paused fields
		if oldC.Spec.Replicas != newC.Spec.Replicas {
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
	}
	return false
}

func (r *Predicate) Delete(evt event.DeleteEvent) bool {
	switch evt.Object.(type) {
	case *etcdv1alpha1.EtcdCluster:
		return true
	case *v1.Pod:
		return true
	case *v1.Service:
		return true
	}
	return false
}

func (r *Predicate) Generic(evt event.GenericEvent) bool {
	switch evt.Object.(type) {
	case *etcdv1alpha1.EtcdCluster:
		return true
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.clusters = sync.Map{}
	r.resyncCh = make(chan event.GenericEvent)

	filter := &Predicate{}
	return ctrl.NewControllerManagedBy(mgr).
		// See sigs.k8s.io/controller-runtime/pkg/predicate/predicate.go
		// builder.WithPredicates(predicate.AnnotationChangedPredicate{}, predicate.GenerationChangedPredicate{})
		// or
		// builder.WithPredicates(predicate.Funcs{
		//		// If Func is nil, then default is return true
		//		CreateFunc: func(createEvent event.CreateEvent) bool { return false },
		//		UpdateFunc: func(updateEvent event.UpdateEvent) bool { return false },
		//		GenericFunc: func(genericEvent event.GenericEvent) bool { return false },
		//		DeleteFunc: func(deleteEvent event.DeleteEvent) bool { return true }})
		For(&etcdv1alpha1.EtcdCluster{}).
		Owns(&v1.Pod{}).
		Owns(&v1.Service{}).
		Watches(&source.Channel{Source: r.resyncCh}, &handler.EnqueueRequestForObject{}).
		// or use WithEventFilter()
		WithEventFilter(filter).
		Complete(r)
}
