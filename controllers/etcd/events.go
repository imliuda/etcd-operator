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
	"fmt"
	etcdv1alpha1 "github.com/imliuda/etcd-operator/apis/etcd/v1alpha1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

func NewMemberAddEvent(memberName string, c *etcdv1alpha1.EtcdCluster) *v1.Event {
	event := newClusterEvent(c)
	event.Type = v1.EventTypeNormal
	event.Reason = "New Member Added"
	event.Message = fmt.Sprintf("New member %s added to cluster", memberName)
	return event
}

func MemberRemoveEvent(memberName string, c *etcdv1alpha1.EtcdCluster) *v1.Event {
	event := newClusterEvent(c)
	event.Type = v1.EventTypeNormal
	event.Reason = "Member Removed"
	event.Message = fmt.Sprintf("Existing member %s removed from the cluster", memberName)
	return event
}

func ReplacingDeadMemberEvent(memberName string, c *etcdv1alpha1.EtcdCluster) *v1.Event {
	event := newClusterEvent(c)
	event.Type = v1.EventTypeNormal
	event.Reason = "Replacing Dead Member"
	event.Message = fmt.Sprintf("The dead member %s is being replaced", memberName)
	return event
}

func MemberUpgradedEvent(memberName, oldVersion, newVersion string, c *etcdv1alpha1.EtcdCluster) *v1.Event {
	event := newClusterEvent(c)
	event.Type = v1.EventTypeNormal
	event.Reason = "Member Upgraded"
	event.Message = fmt.Sprintf("Member %s upgraded from %s to %s ", memberName, oldVersion, newVersion)
	return event
}

func newClusterEvent(c *etcdv1alpha1.EtcdCluster) *v1.Event {
	t := time.Now()
	return &v1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: c.Name + "-",
			Namespace:    c.Namespace,
		},
		InvolvedObject: v1.ObjectReference{
			APIVersion:      etcdv1alpha1.GroupVersion.String(),
			Kind:            etcdv1alpha1.EtcdClusterResourceKind,
			Name:            c.Name,
			Namespace:       c.Namespace,
			UID:             c.UID,
			ResourceVersion: c.ResourceVersion,
		},
		Source: v1.EventSource{
			Component: "etcd-operator",
		},
		// Each cluster event is unique so it should not be collapsed with other events
		FirstTimestamp: metav1.Time{Time: t},
		LastTimestamp:  metav1.Time{Time: t},
		Count:          int32(1),
	}
}


