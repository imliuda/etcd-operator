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
	"fmt"
	etcdv1alpha1 "github.com/imliuda/etcd-operator/apis/etcd/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (r *EtcdClusterReconciler) ensureService(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) error {
	// 1. Client service
	service := &v1.Service{}
	namespacedName := types.NamespacedName{Namespace: cluster.Namespace, Name: ClientServiceName(cluster)}
	if err := r.Client.Get(ctx, namespacedName, service); err != nil {
		// Local cache not found
		if apierrors.IsNotFound(err) {
			service = NewClientService(cluster)
			if err := controllerutil.SetControllerReference(cluster, service, r.Scheme); err != nil {
				return err
			}
			// Remote may already exist, so we will return err, for the next time, this code will not execute
			if err := r.Client.Create(ctx, service); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	// Update status, will not trigger Reconcile
	desired := cluster.DeepCopy()
	desired.Status.ClientPort = ClientPort
	desired.Status.ServiceName = ClientServiceName(cluster)
	if err := r.Status().Patch(ctx, desired, client.MergeFrom(cluster)); err != nil {
		return err
	}

	// 2. Peer service
	namespacedName = types.NamespacedName{Namespace: cluster.Namespace, Name: PeerServiceName(cluster)}
	if err := r.Client.Get(ctx, namespacedName, service); err != nil {
		if apierrors.IsNotFound(err) {
			service = NewPeerService(cluster)
			if err := controllerutil.SetControllerReference(cluster, service, r.Scheme); err != nil {
				return err
			}
			if err := r.Client.Create(ctx, service); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

func (r *EtcdClusterReconciler) newEtcdPod(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, m *Member, state string) (*v1.Pod, error) {
	// Create pod
	pod := NewEtcdPod(cluster, m, state)
	//controllerutil.AddFinalizer(pod, FinalizerName)
	if err := controllerutil.SetControllerReference(cluster, pod, r.Scheme); err != nil {
		return nil, err
	}

	if cluster.IsPodPVEnabled() {
		pvc := NewEtcdPodPVC(cluster, m)
		if err := controllerutil.SetControllerReference(cluster, pvc, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Client.Create(ctx, pvc); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return nil, err
			}
		}
		AddEtcdVolumeToPod(pod, pvc)
	} else {
		AddEtcdVolumeToPod(pod, nil)
	}
	return pod, nil
}

//func (r *EtcdClusterReconciler) waitMemberRunning(ctx context.Context, member *Member, timeout time.Duration) error {
//	// Wait for running
//	interval := 5 * time.Second
//	pod := &v1.Pod{}
//	err := Retry(interval, int(timeout/(interval)), func() (bool, error) {
//		if err := r.Client.Get(ctx, types.NamespacedName{Name: member.Name, Namespace: member.Namespace}, pod); err != nil {
//			return false, err
//		}
//		logger.V(10).Info("Wait pod running", "pod", member.Name, "phase", pod.Status.Phase)
//		switch pod.Status.Phase {
//		case v1.PodRunning:
//			return true, nil
//		case v1.PodPending:
//			return false, nil
//		default:
//			return false, fmt.Errorf("unexpected pod status.phase: %v", pod.Status.Phase)
//		}
//	})
//
//	if err != nil {
//		if IsRetryFailure(err) {
//			return fmt.Errorf("failed to wait pod running, it is still pending: %v", err)
//		}
//		return fmt.Errorf("failed to wait pod running: %v", err)
//	}
//	return nil
//}

func (r *EtcdClusterReconciler) listMemberSet(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) (MemberSet, error) {
	members := MemberSet{}
	pods := &v1.PodList{}
	if err := r.Client.List(ctx, pods, client.InNamespace(cluster.Namespace),
		client.MatchingLabels(LabelsForCluster(cluster))); err != nil {
		return members, err
	}

	for _, pod := range pods.Items {
		m := &Member{
			Name:            pod.Name,
			Namespace:       pod.Namespace,
			SecurePeer:      cluster.Spec.TLS.IsSecurePeer(),
			SecureClient:    cluster.Spec.TLS.IsSecureClient(),
			RunningAndReady: IsRunningAndReady(&pod),
		}
		if cluster.Spec.Pod != nil {
			m.ClusterDomain = cluster.Spec.Pod.ClusterDomain
		}
		if _, ok := pod.Annotations[etcdVersionAnnotationKey]; ok {
			m.Version = pod.Annotations[etcdVersionAnnotationKey]
		}
		members.Add(m)
	}
	return members, nil
}

func (r *EtcdClusterReconciler) newMember(cluster *etcdv1alpha1.EtcdCluster, id int) *Member {
	m := &Member{
		Name:         fmt.Sprintf("%s-%d", cluster.Name, id),
		Namespace:    cluster.Namespace,
		SecurePeer:   cluster.Spec.TLS.IsSecurePeer(),
		SecureClient: cluster.Spec.TLS.IsSecureClient(),
	}
	if cluster.Spec.Pod != nil {
		m.ClusterDomain = cluster.Spec.Pod.ClusterDomain
	}
	return m
}

func (r *EtcdClusterReconciler) createMemberIds(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, ids []int) error {
	members := make([]*Member, 0)
	for _, id := range ids {
		members = append(members, r.newMember(cluster, id))
	}

	for _, m := range members {
		if err := r.createMember(ctx, cluster, m); err != nil {
			return err
		}
	}

	return nil
}

func (r *EtcdClusterReconciler) ensurePods(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) error {
	// Get current pods in this cluster
	members, err := r.listMemberSet(ctx, cluster)
	if err != nil {
		return err
	}

	toCreate := make([]int, 0)
	toDelete := make([]int, 0)

	ids := members.Ordinals()
	for i := 1; i <= cluster.Spec.Size; i++ {
		if _, ok := ids[i]; !ok {
			toCreate = append(toCreate, i)
		}
	}

	for id, _ := range ids {
		if id > cluster.Spec.Size {
			toDelete = append(toDelete, id)
		}
	}

	// Check cluster is healthy
	// remove toDelete
	allReady := true
	for _, m := range members {
		if !m.RunningAndReady {
			allReady = false
		}
	}

	logger.Info("Current members", "members", members, "toCreate", toCreate,
		"toDelete", toDelete, "allReady", allReady)

	if err = r.createMemberIds(ctx, cluster, toCreate); err != nil {
		return err
	}

	if allReady && len(toDelete) > 0 {
		id := toDelete[0]
		m := members.Get(id)
		if m != nil {
			if err := r.deleteMember(ctx, cluster, m, false); err != nil {
				return err
			}
		}
	}

	// delete old version
	if allReady && len(toDelete) == 0 {
		for i := 1; i <= cluster.Spec.Size; i++ {
			m := members.Get(i)
			if m != nil && m.Version != cluster.Spec.Version {
				if err := r.deleteMember(ctx, cluster, m, true); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

//func (r *EtcdClusterReconciler) getEtcdClient(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, members MemberSet) (*clientv3.Client, error) {
//	// etcdctl member add
//	var err error
//	cfg := clientv3.Config{
//		Endpoints:   members.ClientURLs(),
//		DialTimeout: DefaultDialTimeout,
//	}
//
//	if cluster.Spec.TLS.IsSecureClient() {
//		secret := &v1.Secret{}
//		if err := r.Client.Get(ctx, types.NamespacedName{Namespace: cluster.Name, Name: cluster.Spec.TLS.Static.OperatorSecret}, secret); err != nil {
//			return nil, err
//		}
//		cfg.TLS, err = NewTLSConfig(secret.Data[CliCertFile], secret.Data[CliKeyFile], secret.Data[CliCAFile])
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	logger.V(5).Info("Etcd client config", "config", cfg)
//
//	etcdcli, err := clientv3.New(cfg)
//	if err != nil {
//		return nil, err
//	}
//	return etcdcli, nil
//}

func (r *EtcdClusterReconciler) createMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, m *Member) error {
	logger.Info("Starting add new member to cluster", "cluster", cluster.Name)
	defer logger.Info("End add new member to cluster", "cluster", cluster.Name)

	// New Pod
	pod, err := r.newEtcdPod(ctx, cluster, m, "new")
	if err != nil {
		return err
	}

	// Create pod
	if err = r.Client.Create(ctx, pod); err != nil {
		return err
	}

	return nil
}

func (r *EtcdClusterReconciler) deleteMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, m *Member, keepPVC bool) error {
	pod := &v1.Pod{}
	pod.SetName(m.Name)
	pod.SetNamespace(m.Namespace)
	if err := r.Client.Delete(ctx, pod); err != nil {
		return err
	}

	if cluster.IsPodPVEnabled() && !keepPVC {
		pvc := &v1.PersistentVolumeClaim{}
		pvc.SetName(PVCNameFromMember(m.Name))
		if err := r.Client.Delete(ctx, pvc); err != nil {
			return err
		}
	}
	return nil
}

//func (r *EtcdClusterReconciler) ensurePods(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) error {
//	pods := &v1.PodList{}
//	if err := r.Client.List(ctx, pods, client.InNamespace(cluster.Namespace),
//		client.MatchingLabels(LabelsForCluster(cluster))); err != nil {
//		return err
//	}
//
//	validPods := make([]v1.Pod, 0)
//	deletePods := make([]v1.Pod, 0)
//
//	for _, pod := range pods.Items {
//		logger.V(10).Info("pod info", "version",
//			GetEtcdVersion(&pod), "spec version", cluster.Spec.Version, "isDeleted", !pod.DeletionTimestamp.IsZero())
//		if GetEtcdVersion(&pod) == cluster.Spec.Version && pod.DeletionTimestamp.IsZero() {
//			validPods = append(validPods, pod)
//		} else if pod.DeletionTimestamp.IsZero() {
//			deletePods = append(deletePods, pod)
//		}
//	}
//
//	scaleUpCount := cluster.Spec.Size - len(validPods)
//	logger.V(5).Info("Adding members", "spec size", cluster.Spec.Size, "valid count", len(validPods))
//	for i := 0; i < scaleUpCount; i++ {
//		if err := r.addOneMember(ctx, cluster); err != nil {
//			return err
//		}
//	}
//
//	scaleDownCount := len(validPods) - cluster.Spec.Size
//	if scaleDownCount > 0 {
//
//	}
//
//	deleteCount := len(validPods) - cluster.Spec.Size
//	deletePods = append(deletePods, validPods[:deleteCount]...)
//	for _, pod := range deletePods {
//		if err := r.removeOneMember(ctx, cluster, pod); err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

func (r *EtcdClusterReconciler) ensureClusterDeleted(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) error {
	if err := r.Client.Delete(ctx, cluster, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		return err
	}
	return nil
}
