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
	"errors"
	"fmt"
	etcdv1alpha1 "github.com/imliuda/etcd-operator/apis/etcd/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"strings"
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
	pod := NewEtcdPod(cluster, m, initialCluster, state)
	//controllerutil.AddFinalizer(pod, FinalizerName)
	if err := controllerutil.SetControllerReference(cluster, pod, r.Scheme); err != nil {
		return nil, err
	}

	if cluster.IsPodPVEnabled() {
		pvc := NewEtcdPodPVC(cluster, m)
		if err := controllerutil.SetControllerReference(cluster, pvc, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Client.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, err
		}
		AddEtcdVolumeToPod(pod, pvc)
	} else {
		AddEtcdVolumeToPod(pod, nil)
	}
	return pod, nil
}

func (r *EtcdClusterReconciler) podMembers(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) (MemberSet, error) {
	members := MemberSet{}
	pods := &v1.PodList{}
	if err := r.Client.List(ctx, pods, client.InNamespace(cluster.Namespace),
		client.MatchingLabels(LabelsForCluster(cluster))); err != nil {
		return members, err
	}

	for _, pod := range pods.Items {
		m := &Member{
			Name:         pod.Name,
			Namespace:    pod.Namespace,
			SecurePeer:   cluster.Spec.TLS.IsSecurePeer(),
			SecureClient: cluster.Spec.TLS.IsSecureClient(),
			Created:      true,
			Version:      pod.Labels[etcdv1alpha1.AppVersionLabel],
		}
		if cluster.Spec.Pod != nil {
			m.ClusterDomain = cluster.Spec.Pod.ClusterDomain
		}
		members.Add(m)
	}
	return members, nil
}

func (r *EtcdClusterReconciler) currentMembers(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) (MemberSet, error) {
	members := MemberSet{}

	// Normally will not happen
	ms, ok := cluster.Annotations[etcdv1alpha1.MembersAnnotation]
	if !ok || ms == "" {
		return members, errors.New("cluster spec has no members annotation")
	}

	names := strings.Split(ms, ",")

	pods := &v1.PodList{}
	if err := r.Client.List(ctx, pods, client.InNamespace(cluster.Namespace),
		client.MatchingLabels(LabelsForCluster(cluster))); err != nil {
		return members, err
	}
	podMaps := map[string]v1.Pod{}
	for _, pod := range pods.Items {
		podMaps[pod.Name] = pod
	}

	for _, name := range names {
		m := &Member{
			Name:            name,
			Namespace:       cluster.Namespace,
			SecurePeer:      cluster.Spec.TLS.IsSecurePeer(),
			SecureClient:    cluster.Spec.TLS.IsSecureClient(),
			Created:         false,
			RunningAndReady: false,
		}
		if cluster.Spec.Pod != nil {
			m.ClusterDomain = cluster.Spec.Pod.ClusterDomain
		}
		if pod, ok := podMaps[name]; ok {
			m.Created = true
			m.RunningAndReady = IsRunningAndReady(&pod)
			m.Version = pod.Labels[etcdv1alpha1.AppVersionLabel]
		}
		members.Add(m)
	}
	return members, nil
}

func (r *EtcdClusterReconciler) specMembers(cluster *etcdv1alpha1.EtcdCluster) MemberSet {
	ms := MemberSet{}
	for i := 1; i <= cluster.Spec.Replicas; i++ {
		ms.Add(r.newMember(cluster, i))
	}
	return ms
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

func (r *EtcdClusterReconciler) allMembersHealth(ms MemberSet) bool {
	// TODO use etcd api query member list and endpoint status
	for _, m := range ms {
		if !m.RunningAndReady {
			return false
		}
	}
	return true
}

func (r *EtcdClusterReconciler) ensureMembers(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) (bool, error) {
	// Get current pods in this cluster
	ms, err := r.currentMembers(ctx, cluster)
	if err != nil {
		return true, err
	}
	logger.V(10).Info("Expected members", "member set", ms)

	pms, err := r.podMembers(ctx, cluster)
	if err != nil {
		return true, err
	}
	logger.V(10).Info("Existing pod members", "members", pms)

	// Pods may be not created, or partial created, ensure all config member pods are created
	_, bootstrapped := cluster.Annotations[etcdv1alpha1.BootStrappedAnnotation]

	state := "new"
	if bootstrapped {
		state = "existing"
	}

	// Create member pod if needed
	for _, m := range ms {
		if !m.Created {
			m = r.newMember(cluster, m.Ordinal())
			logger.V(10).Info("Create cluster member", "member", m)
			if err = r.createMember(ctx, cluster, m, ms.PeerURLPairs(), state); err != nil {
				return true, err
			}
		}
	}

	diff := pms.Diff(ms)
	for _, m := range diff {
		// May be scaled up pod that has not added to cluster members
		if m.Ordinal() <= cluster.Spec.Replicas {
			continue
		}
		// May be scaled down pod that has not totally removed from etcd
		// TODO confirm this
		exists, err := r.memberExists(ctx, cluster, ms, m)
		if err != nil {
			return true, err
		}
		if exists {
			continue
		}
		logger.V(10).Info("Delete unused member", "member", m)
		if err = r.deleteMember(ctx, cluster, m, false); err != nil {
			return true, err
		}
	}

	// Ensure all config member pods are running and ready
	if !r.allMembersHealth(ms) {
		return true, nil
	}

	logger.V(10).Info("Cluster is healthy", "cluster", cluster.Name)

	desired := cluster.DeepCopy()
	if !bootstrapped {
		desired.Annotations[etcdv1alpha1.BootStrappedAnnotation] = "true"
		desired.Status.SetCondition(etcdv1alpha1.ConditionAvailable, v1.ConditionTrue, etcdv1alpha1.ReasonBootStrapped, "Cluster bootstrapped")
		desired.Status.SetCondition(etcdv1alpha1.ConditionProgressing, v1.ConditionFalse, etcdv1alpha1.ReasonBootStrapped, "")
	}

	condition := desired.Status.GetCondition(etcdv1alpha1.ConditionAvailable)
	if condition != nil {
		lastUpdate, err := time.Parse(time.RFC3339, condition.LastUpdateTime)
		if err != nil {
			desired.Status.SetCondition(etcdv1alpha1.ConditionAvailable, v1.ConditionTrue, etcdv1alpha1.ReasonResync, "Ensure members")
		}
		if time.Now().Sub(lastUpdate) >= time.Second*120 {
			desired.Status.SetCondition(etcdv1alpha1.ConditionAvailable, v1.ConditionTrue, etcdv1alpha1.ReasonResync, "Ensure members")
		}
	}

	if err = r.Client.Patch(ctx, desired, client.MergeFrom(cluster)); err != nil {
		return true, err
	}

	if !bootstrapped {
		r.recorder.Event(cluster, v1.EventTypeNormal, etcdv1alpha1.EventClusterBootStrapped, "Cluster bootstrapped")
	}

	return false, nil
}

func (r *EtcdClusterReconciler) ensureScaled(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) (bool, error) {
	// Get current members in this cluster
	ms, err := r.currentMembers(ctx, cluster)
	if err != nil {
		return true, err
	}

	// Ensure all members all health
	if !r.allMembersHealth(ms) {
		return true, nil
	}

	ids := ms.Ordinals()

	// Scale up one by one
	if len(ids) < cluster.Spec.Replicas {
		desired := cluster.DeepCopy()

		desired.Status.SetCondition(etcdv1alpha1.ConditionProgressing, v1.ConditionTrue, etcdv1alpha1.ReasonScaleUP,
			fmt.Sprintf("From %d to %d", len(ids), cluster.Spec.Replicas))
		if err = r.Client.Patch(ctx, desired, client.MergeFrom(cluster)); err != nil {
			return true, err
		}

		m := r.newMember(cluster, len(ids)+1)
		// Ensure pod created
		newms := ms.Duplicate()
		newms.Add(m)

		r.recorder.Eventf(cluster, v1.EventTypeNormal, etcdv1alpha1.EventMemberAdd,
			"Adding member with name %s", m.Name)

		if err = r.createMember(ctx, cluster, m, newms.PeerURLPairs(), "existing"); err != nil {
			return true, err
		}

		if err = r.addMember(ctx, cluster, ms, m); err != nil {
			return true, err
		}

		pod := &v1.Pod{}
		if err = r.Client.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: m.Name}, pod); err != nil {
			return true, err
		}

		if !IsRunningAndReady(pod) {
			return true, nil
		}

		desired.Annotations[etcdv1alpha1.MembersAnnotation] += "," + m.Name
		err = r.Client.Patch(ctx, desired, client.MergeFrom(cluster))

		r.recorder.Eventf(cluster, v1.EventTypeNormal, etcdv1alpha1.EventMemberAdded,
			"Member with name %s added", m.Name)

		// Cluster modified, next reconcile will enter r.ensureMembers()
		return true, err
	}

	// Scale down
	if len(ids) > cluster.Spec.Replicas {
		desired := cluster.DeepCopy()

		desired.Status.SetCondition(etcdv1alpha1.ConditionProgressing, v1.ConditionTrue, etcdv1alpha1.ReasonScaleDown,
			fmt.Sprintf("From %d to %d", len(ids), cluster.Spec.Replicas))
		if err = r.Client.Patch(ctx, desired, client.MergeFrom(cluster)); err != nil {
			return true, err
		}

		m := r.newMember(cluster, len(ids))

		r.recorder.Eventf(cluster, v1.EventTypeNormal, etcdv1alpha1.EventMemberAdd,
			"Remove member with name %s", m.Name)

		if err = r.removeMember(ctx, cluster, ms, m); err != nil {
			return true, err
		}

		exists, err := r.memberExists(ctx, cluster, ms, m)
		if err != nil {
			return true, nil
		}
		if exists {
			return true, nil
		}

		desired.Annotations[etcdv1alpha1.MembersAnnotation] = strings.Join(ms.Names(), ",")
		err = r.Client.Patch(ctx, desired, client.MergeFrom(cluster))

		r.recorder.Eventf(cluster, v1.EventTypeNormal, etcdv1alpha1.EventMemberRemoved,
			"Member with name %s removed", m.Name)

		return true, err
	}

	desired := cluster.DeepCopy()
	desired.Status.SetCondition(etcdv1alpha1.ConditionProgressing, v1.ConditionFalse, etcdv1alpha1.ReasonScaled, "")
	if err = r.Client.Patch(ctx, desired, client.MergeFrom(cluster)); err != nil {
		return true, err
	}

	return false, nil
}

func (r *EtcdClusterReconciler) createMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, m *Member, initialCluster []string, state string) error {
	logger.Info("Starting add new member to cluster", "cluster", cluster.Name)
	defer logger.Info("End add new member to cluster", "cluster", cluster.Name)

	// New Pod
	pod, err := r.newEtcdPod(ctx, cluster, m, initialCluster, state)
	if err != nil {
		return err
	}

	// Create pod
	if err = r.Client.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	return nil
}

func (r *EtcdClusterReconciler) deleteMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, m *Member, keepPVC bool) error {
	// Delete pvc first
	if cluster.IsPodPVEnabled() && !keepPVC {
		pvc := &v1.PersistentVolumeClaim{}
		pvc.SetName(PVCNameFromMember(m.Name))
		pvc.SetNamespace(m.Namespace)
		if err := r.Client.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	pod := &v1.Pod{}
	pod.SetName(m.Name)
	pod.SetNamespace(m.Namespace)
	if err := r.Client.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}

func (r *EtcdClusterReconciler) ensureUpgraded(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) (bool, error) {
	ms, err := r.currentMembers(ctx, cluster)
	if err != nil {
		return true, err
	}

	// Ensure all members all health
	if !r.allMembersHealth(ms) {
		return true, err
	}

	// When using cached objects, we may delete many pods, watch has latency.
	// So using cluster object as a queue, annotate pods to be upgraded first,
	// then check annotation and delete appropriate pods.
	toUpgrade := cluster.Annotations[etcdv1alpha1.UpgradeAnnotation]
	logger.V(9).Info("toUpgrade", "toUpgrade", toUpgrade)
	if toUpgrade == "" {
		// Pick one pod or more pods, but best not the header ones
		// TODO pick pods from last ordinal number
		for _, m := range ms {
			if m.Version != cluster.Spec.Version {
				desired := cluster.DeepCopy()
				desired.Annotations[etcdv1alpha1.UpgradeAnnotation] = m.Name
				desired.Status.SetCondition(etcdv1alpha1.ConditionProgressing, v1.ConditionTrue, etcdv1alpha1.ReasonUpgrade,
					fmt.Sprintf("Upgrading member %s from %s to %s", m.Name, m.Version, cluster.Spec.Version))
				if err = r.Client.Patch(ctx, desired, client.MergeFrom(cluster)); err != nil {
					return true, err
				}

				r.recorder.Eventf(cluster, v1.EventTypeNormal, etcdv1alpha1.EventMemberUpgraded,
					"Upgrade member with name %s, current version %s", m.Name, m.Version)

				return true, nil
			}
		}
	} else {
		pod := &v1.Pod{}
		if err = r.Client.Get(ctx, types.NamespacedName{Name: toUpgrade, Namespace: cluster.Namespace}, pod); err != nil {
			return true, err
		}

		logger.V(9).Info("Pod version", "version", GetEtcdVersion(pod), "runningReady", IsRunningAndReady(pod))

		if GetEtcdVersion(pod) != cluster.Spec.Version {
			// May be staled pod, but we may delete more than one time
			if err = r.Client.Delete(ctx, pod); err != nil {
				return true, err
			}
		}

		if GetEtcdVersion(pod) != cluster.Spec.Version || !IsRunningAndReady(pod) {
			return true, nil
		}

		desired := cluster.DeepCopy()
		delete(desired.Annotations, etcdv1alpha1.UpgradeAnnotation)
		if err = r.Client.Patch(ctx, desired, client.MergeFrom(cluster)); err != nil {
			return true, err
		}

		r.recorder.Eventf(cluster, v1.EventTypeNormal, etcdv1alpha1.EventMemberUpgraded,
			"Member with name %s upgraded, new version %s", pod.Name, cluster.Spec.Version)

		return true, nil
	}

	desired := cluster.DeepCopy()
	desired.Status.SetCondition(etcdv1alpha1.ConditionProgressing, v1.ConditionFalse, etcdv1alpha1.ReasonUpgraded, "")
	if err = r.Client.Patch(ctx, desired, client.MergeFrom(cluster)); err != nil {
		return true, err
	}

	return false, nil
}

func (r *EtcdClusterReconciler) ensureClusterDeleted(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) error {
	if err := r.Client.Delete(ctx, cluster, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		return err
	}
	return nil
}
