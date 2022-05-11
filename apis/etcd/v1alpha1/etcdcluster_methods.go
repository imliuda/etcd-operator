package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	"time"
)

func (tp *TLSPolicy) IsSecureClient() bool {
	if tp == nil || tp.Static == nil {
		return false
	}
	return len(tp.Static.OperatorSecret) != 0
}

func (tp *TLSPolicy) IsSecurePeer() bool {
	if tp == nil || tp.Static == nil || tp.Static.Member == nil {
		return false
	}
	return len(tp.Static.Member.PeerSecret) != 0
}

func (c *EtcdCluster) IsPodPVEnabled() bool {
	if podPolicy := c.Spec.Pod; podPolicy != nil {
		return podPolicy.PersistentVolumeClaimSpec != nil
	}
	return false
}

func (cs *EtcdClusterStatus) SetCondition(t ConditionType, status v1.ConditionStatus, reason, message string) {
	c := newClusterCondition(t, status, reason, message)
	cs.setClusterCondition(*c)
}

func (cs *EtcdClusterStatus) RemoveCondition(t ConditionType) {
	pos, _ := getClusterCondition(cs, t)
	if pos == -1 {
		return
	}
	cs.Conditions = append(cs.Conditions[:pos], cs.Conditions[pos+1:]...)
}

func (cs *EtcdClusterStatus) GetCondition(t ConditionType) *ClusterCondition {
	_, c := getClusterCondition(cs, t)
	return c
}

func newClusterCondition(condType ConditionType, status v1.ConditionStatus, reason, message string) *ClusterCondition {
	now := time.Now().Format(time.RFC3339)
	return &ClusterCondition{
		Type:               condType,
		Status:             status,
		LastUpdateTime:     now,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}
}

func getClusterCondition(status *EtcdClusterStatus, t ConditionType) (int, *ClusterCondition) {
	for i, c := range status.Conditions {
		if t == c.Type {
			return i, &c
		}
	}
	return -1, nil
}

func (cs *EtcdClusterStatus) setClusterCondition(c ClusterCondition) {
	pos, cp := getClusterCondition(cs, c.Type)
	if cp != nil &&
		cp.Status == c.Status && cp.Reason == c.Reason && cp.Message == c.Message {
		return
	}

	if cp != nil {
		cs.Conditions[pos] = c
	} else {
		cs.Conditions = append(cs.Conditions, c)
	}
}
