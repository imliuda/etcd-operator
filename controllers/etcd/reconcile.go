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
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/google/uuid"
	etcdv1alpha1 "github.com/imliuda/etcd-operator/apis/etcd/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"strconv"
	"time"
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

func (r *EtcdClusterReconciler) newEtcdPod(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, m *Member, initialCluster []string, state string) (*v1.Pod, error) {
	// Create pod
	pod := NewEtcdPod(cluster, m, initialCluster, state, uuid.New().String())
	controllerutil.AddFinalizer(pod, FinalizerName)
	if err := controllerutil.SetControllerReference(cluster, pod, r.Scheme); err != nil {
		return nil, err
	}

	if cluster.IsPodPVEnabled() {
		pvc := NewEtcdPodPVC(cluster, m)
		if err := controllerutil.SetControllerReference(cluster, pvc, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Client.Create(ctx, pvc); err != nil {
			return nil, err
		}
		AddEtcdVolumeToPod(pod, pvc)
	} else {
		AddEtcdVolumeToPod(pod, nil)
	}
	return pod, nil
}

func (r *EtcdClusterReconciler) waitPodRunning(ctx context.Context, pod *v1.Pod, timeout time.Duration) error {
	// Wait for running
	interval := 5 * time.Second
	var retPod *v1.Pod
	err := Retry(interval, int(timeout/(interval)), func() (bool, error) {
		if err := r.Client.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, retPod); err != nil {
			return false, err
		}
		switch retPod.Status.Phase {
		case v1.PodRunning:
			return true, nil
		case v1.PodPending:
			return false, nil
		default:
			return false, fmt.Errorf("unexpected pod status.phase: %v", retPod.Status.Phase)
		}
	})

	if err != nil {
		if IsRetryFailure(err) {
			return fmt.Errorf("failed to wait pod running, it is still pending: %v", err)
		}
		return fmt.Errorf("failed to wait pod running: %v", err)
	}
	return nil
}

func (r *EtcdClusterReconciler) ensureSeedPod(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) error {
	// Get current pods in this cluster
	var err error
	pods := &v1.PodList{}
	if err = r.Client.List(ctx, pods, client.InNamespace(cluster.Namespace),
		client.MatchingLabels(LabelsForCluster(cluster))); err != nil {
		return err
	}

	// This cluster already have some pods
	timeout, _ := time.ParseDuration("180s")
	if len(pods.Items) == 1 {
		if err = r.waitPodRunning(ctx, &pods.Items[0], timeout); err != nil {
			return err
		}
	} else if len(pods.Items) > 1 {
		return nil
	}

	// If no pods in this cluster, create the first pod
	m := &Member{
		Name:         UniqueMemberName(cluster.Name),
		Namespace:    cluster.Namespace,
		SecurePeer:   cluster.Spec.TLS.IsSecurePeer(),
		SecureClient: cluster.Spec.TLS.IsSecureClient(),
	}
	if cluster.Spec.Pod != nil {
		m.ClusterDomain = cluster.Spec.Pod.ClusterDomain
	}

	members := NewMemberSet(m)

	var pod *v1.Pod
	pod, err = r.newEtcdPod(ctx, cluster, m, members.PeerURLPairs(), "new")
	if err != nil {
		return err
	}

	// Create pod
	if err = r.Client.Create(ctx, pod); err != nil {
		return err
	}

	// Wait for pod running
	if err = r.waitPodRunning(ctx, pod, timeout); err != nil {
		return err
	}

	return nil
}

func (r *EtcdClusterReconciler) addOneMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) error {
	var err error
	pods := &v1.PodList{}
	if err = r.Client.List(ctx, pods, client.InNamespace(cluster.Namespace),
		client.MatchingLabels(LabelsForCluster(cluster))); err != nil {
		return err
	}

	// Existing
	members := MemberSet{}
	for _, pod := range pods.Items {
		m := &Member{
			Name:         pod.Name,
			Namespace:    pod.Namespace,
			SecurePeer:   cluster.Spec.TLS.IsSecurePeer(),
			SecureClient: cluster.Spec.TLS.IsSecureClient(),
		}
		if cluster.Spec.Pod != nil {
			m.ClusterDomain = cluster.Spec.Pod.ClusterDomain
		}
		members.Add(m)
	}

	// New one
	m := &Member{
		Name:         UniqueMemberName(cluster.Name),
		Namespace:    cluster.Namespace,
		SecurePeer:   cluster.Spec.TLS.IsSecurePeer(),
		SecureClient: cluster.Spec.TLS.IsSecureClient(),
	}
	if cluster.Spec.Pod != nil {
		m.ClusterDomain = cluster.Spec.Pod.ClusterDomain
	}

	// etcdctl member add
	cfg := clientv3.Config{
		Endpoints:   members.ClientURLs(),
		DialTimeout: DefaultDialTimeout,
	}

	if cluster.Spec.TLS.IsSecureClient() {
		secret := &v1.Secret{}
		if err = r.Client.Get(ctx, types.NamespacedName{Namespace: cluster.Name, Name: cluster.Spec.TLS.Static.OperatorSecret}, secret); err != nil {
			return err
		}
		cfg.TLS, err = NewTLSConfig(secret.Data[CliCertFile], secret.Data[CliKeyFile], secret.Data[CliCAFile])
		if err != nil {
			return err
		}
	}

	etcdcli, err := clientv3.New(cfg)
	if err != nil {
		return fmt.Errorf("add one member failed: creating etcd client failed %v", err)
	}
	defer etcdcli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	resp, err := etcdcli.MemberAdd(ctx, []string{m.PeerURL()})
	cancel()
	if err != nil {
		return fmt.Errorf("fail to add new member (%s): %v", m.Name, err)
	}
	m.ID = resp.Member.ID
	members.Add(m)

	// New Pod
	var pod *v1.Pod
	pod, err = r.newEtcdPod(ctx, cluster, m, members.PeerURLPairs(), "existing")
	if err != nil {
		return err
	}

	// Create pod
	if err = r.Client.Create(ctx, pod); err != nil {
		return err
	}

	// Wait for pod running
	timeout, _ := time.ParseDuration("180s")
	if err = r.waitPodRunning(ctx, pod, timeout); err != nil {
		return err
	}

	return nil
}

func (r *EtcdClusterReconciler) removeOneMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, pod v1.Pod) error {
	var err error
	pods := &v1.PodList{}
	if err = r.Client.List(ctx, pods, client.InNamespace(cluster.Namespace),
		client.MatchingLabels(LabelsForCluster(cluster))); err != nil {
		return err
	}

	// Existing
	members := MemberSet{}
	for _, pod := range pods.Items {
		m := &Member{
			Name:         pod.Name,
			Namespace:    pod.Namespace,
			SecurePeer:   cluster.Spec.TLS.IsSecurePeer(),
			SecureClient: cluster.Spec.TLS.IsSecureClient(),
		}
		if cluster.Spec.Pod != nil {
			m.ClusterDomain = cluster.Spec.Pod.ClusterDomain
		}
		members.Add(m)
	}

	cfg := clientv3.Config{
		Endpoints:   members.ClientURLs(),
		DialTimeout: DefaultDialTimeout,
	}

	if cluster.Spec.TLS.IsSecureClient() {
		secret := &v1.Secret{}
		if err = r.Client.Get(ctx, types.NamespacedName{Namespace: cluster.Name, Name: cluster.Spec.TLS.Static.OperatorSecret}, secret); err != nil {
			return err
		}
		cfg.TLS, err = NewTLSConfig(secret.Data[CliCertFile], secret.Data[CliKeyFile], secret.Data[CliCAFile])
		if err != nil {
			return err
		}
	}

	etcdcli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer etcdcli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	myId, err := strconv.ParseUint(pod.Annotations[ClusterNodeId], 10, 64)
	cancel()
	if err != nil {
		return err
	}
	_, err = etcdcli.Cluster.MemberRemove(ctx, myId)
	if err != nil {
		switch err {
		case rpctypes.ErrMemberNotFound:
			logger.Info("etcd member () has been removed")
		default:
			return err
		}
	}

	if err := r.Client.Delete(ctx, &pod); err != nil {
		return err
	}

	m := &Member{
		Name:         UniqueMemberName(cluster.Name),
		Namespace:    cluster.Namespace,
		SecurePeer:   cluster.Spec.TLS.IsSecurePeer(),
		SecureClient: cluster.Spec.TLS.IsSecureClient(),
	}
	if cluster.Spec.Pod != nil {
		m.ClusterDomain = cluster.Spec.Pod.ClusterDomain
	}

	if cluster.IsPodPVEnabled() {
		pvc := &v1.PersistentVolumeClaim{}
		pvc.SetName(PVCNameFromMember(m.Name))
		if err = r.Client.Delete(ctx, pvc); err != nil {
			return err
		}
	}
	return nil

}

func (r *EtcdClusterReconciler) ensurePods(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) error {
	pods := &v1.PodList{}
	if err := r.Client.List(ctx, pods, client.InNamespace(cluster.Namespace),
		client.MatchingLabels(LabelsForCluster(cluster))); err != nil {
		return err
	}

	validPods := make([]v1.Pod, 0)
	deletePods := make([]v1.Pod, 0)
	if len(pods.Items) < cluster.Spec.Size {
		for _, pod := range pods.Items {
			if GetEtcdVersion(&pod) == cluster.Spec.Version {
				if !pod.DeletionTimestamp.IsZero() {
					deletePods = append(deletePods, pod)
				} else {
					validPods = append(validPods, pod)
				}
			} else {
				deletePods = append(deletePods, pod)
			}
		}
	}

	createCount := cluster.Spec.Size - len(validPods)
	for i := 0; i < createCount; i++ {
		if err := r.addOneMember(ctx, cluster); err != nil {
			return err
		}
	}
	for _, pod := range deletePods {
		if err := r.removeOneMember(ctx, cluster, pod); err != nil {
			return err
		}
	}

	return nil
}

func (r *EtcdClusterReconciler) ensureClusterDeleted(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) error {

	return nil
}
