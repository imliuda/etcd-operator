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

package v1alpha1

import (
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ConditionType string

const (
	EtcdClusterResourceKind = "EtcdCluster"

	ConditionAvailable   ConditionType = "Available"
	ConditionProgressing               = "Progressing"
	ConditionFailed                    = "Failed"

	// For Available condition
	ReasonBootStrapping = "BootStrapping"
	ReasonBootStrapped  = "BootStrapped"
	ReasonScaled        = "Scaled"
	ReasonUpgraded      = "Upgraded"
	ReasonResync        = "Resync"

	// For Progressing condition
	ControlPaused   = "ControlPaused"
	ReasonBoot      = "BootStrap"
	ReasonUpgrade   = "Upgrade"
	ReasonScaleUP   = "ScaleUp"
	ReasonScaleDown = "ScaleDown"

	// Labels
	ClusterLabel    = "etcd.imliuda.github.io/cluster"
	AppNameLabel    = "app.kubernetes.io/name"
	AppVersionLabel = "app.kubernetes.io/version"

	// Annotations
	MembersAnnotation      = "etcd.imliuda.github.io/members"
	UpgradeAnnotation      = "etcd.imliuda.github.io/upgrade"
	BootStrappedAnnotation = "etcd.imliuda.github.io/bootstrapped"

	EventClusterBootStrapped = "ClusterBootstrapped"
	EventClusterDeleted      = "ClusterDeleted"
	EventMemberAdd           = "MemberAdd"
	EventMemberAdded         = "MemberAdded"
	EventMemberRemove        = "MemberRemove"
	EventMemberRemoved       = "MemberRemoved"
	EventMemberUpgrade       = "MemberUpgrade"
	EventMemberUpgraded      = "MemberUpgraded"
	// TODO member health event
)

// EtcdClusterSpec defines the desired state of EtcdCluster
type EtcdClusterSpec struct {
	// Replicas is the expected size of the etcd cluster.
	// The etcd-operator will eventually make the size of the running
	// cluster equal to the expected size.
	// The vaild range of the size is from 1 to 7.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=7
	Replicas int `json:"replicas"`
	// Repository is the name of the repository that hosts
	// etcd container images. It should be direct clone of the repository in official
	// release:
	//   https://github.com/coreos/etcd/releases
	// That means, it should have exact same tags and the same meaning for the tags.
	//
	// By default, it is `quay.io/coreos/etcd`.
	// +kubebuilder:default=quay.io/coreos/etcd
	Repository string `json:"repository,omitempty"`

	// Version is the expected version of the etcd cluster.
	// The etcd-operator will eventually make the etcd cluster version
	// equal to the expected version.
	//
	// The version must follow the [semver]( http://semver.org) format, for example "3.2.13".
	// Only etcd released versions are supported: https://github.com/coreos/etcd/releases
	//
	// If version is not set, default is "3.2.13".
	// +kubebuilder:default="3.2.13"
	Version string `json:"version,omitempty"`

	// Paused is to pause the control of the operator for the etcd cluster.
	// +kubebuilder:default=false
	Paused bool `json:"paused,omitempty"`

	// Pod defines the policy to create pod for the etcd pod.
	//
	// Updating Pod does not take effect on any existing etcd pods.
	Pod *PodPolicy `json:"pod,omitempty"`

	// etcd cluster TLS configuration
	TLS *TLSPolicy `json:"TLS,omitempty"`
}

// TLSPolicy defines the TLS policy of an etcd cluster
type TLSPolicy struct {
	// StaticTLS enables user to generate static x509 certificates and keys,
	// put them into Kubernetes secrets, and specify them into here.
	Static *StaticTLS `json:"static,omitempty"`
}

type StaticTLS struct {
	// Member contains secrets containing TLS certs used by each etcd member pod.
	Member *MemberSecret `json:"member,omitempty"`
	// OperatorSecret is the secret containing TLS certs used by operator to
	// talk securely to this cluster.
	OperatorSecret string `json:"operatorSecret,omitempty"`
}

type MemberSecret struct {
	// PeerSecret is the secret containing TLS certs used by each etcd member pod
	// for the communication between etcd peers.
	PeerSecret string `json:"peerSecret,omitempty"`
	// ServerSecret is the secret containing TLS certs used by each etcd member pod
	// for the communication between etcd server and its clients.
	ServerSecret string `json:"serverSecret,omitempty"`
}

// PodPolicy defines the policy to create pod for the etcd container.
type PodPolicy struct {
	// Labels specifies the labels to attach to pods the operator creates for the
	// etcd cluster.
	// "app" and "etcd_*" labels are reserved for the internal use of the etcd operator.
	// Do not overwrite them.
	Labels map[string]string `json:"labels,omitempty"`

	// NodeSelector specifies a map of key-value pairs. For the pod to be eligible
	// to run on a node, the node must have each of the indicated key-value pairs as
	// labels.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// The scheduling constraints on etcd pods.
	Affinity *v1.Affinity `json:"affinity,omitempty"`
	// **DEPRECATED**. Use Affinity instead.
	AntiAffinity bool `json:"antiAffinity,omitempty"`

	// Resources is the resource requirements for the etcd container.
	// This field cannot be updated once the cluster is created.
	Resources v1.ResourceRequirements `json:"resources,omitempty"`

	// Tolerations specifies the pod's tolerations.
	Tolerations []v1.Toleration `json:"tolerations,omitempty"`

	// List of environment variables to set in the etcd container.
	// This is used to configure etcd process. etcd cluster cannot be created, when
	// bad environement variables are provided. Do not overwrite any flags used to
	// bootstrap the cluster (for example `--initial-cluster` flag).
	// This field cannot be updated.
	Envs []v1.EnvVar `json:"etcdEnv,omitempty"`

	// PersistentVolumeClaimSpec is the spec to describe PVC for the etcd container
	// This field is optional. If no PVC spec, etcd container will use emptyDir as volume
	// Note. This feature is in alpha stage. It is currently only used as non-stable storage,
	// not the stable storage. Future work need to make it used as stable storage.
	PersistentVolumeClaimSpec *v1.PersistentVolumeClaimSpec `json:"persistentVolumeClaimSpec,omitempty"`

	// Annotations specifies the annotations to attach to pods the operator creates for the
	// etcd cluster.
	// The "etcd.version" annotation is reserved for the internal use of the etcd operator.
	Annotations map[string]string `json:"annotations,omitempty"`

	// busybox init container image. default is busybox:1.28.0-glibc
	// busybox:latest uses uclibc which contains a bug that sometimes prevents name resolution
	// More info: https://github.com/docker-library/busybox/issues/27
	BusyboxImage string `json:"busyboxImage,omitempty"`

	// SecurityContext specifies the security context for the entire pod
	// More info: https://kubernetes.io/docs/tasks/configure-pod-container/security-context
	SecurityContext *v1.PodSecurityContext `json:"securityContext,omitempty"`

	// DNSTimeoutInSecond is the maximum allowed time for the init container of the etcd pod to
	// reverse DNS lookup its IP given the hostname.
	// The default is to wait indefinitely and has a vaule of 0.
	DNSTimeoutInSecond int64 `json:"DNSTimeoutInSecond,omitempty"`

	// ClusterDomain is the cluster domain to use for member URLs E.g.
	// '.cluster.local'.
	// The default is to not set a cluster domain explicitly.
	ClusterDomain string `json:"ClusterDomain,omitempty"`
}

// ClusterCondition represents one current condition of an etcd cluster.
// A condition might not show up if it is not happening.
// For example, if a cluster is not upgrading, the Upgrading condition would not show up.
// If a cluster is upgrading and encountered a problem that prevents the upgrade,
// the Upgrading condition's status will would be False and communicate the problem back.
type ClusterCondition struct {
	// Type of cluster condition.
	Type ConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status v1.ConditionStatus `json:"status"`
	// The last time this condition was updated.
	LastUpdateTime string `json:"lastUpdateTime,omitempty"`
	// Last time the condition transitioned from one status to another.
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
	// The reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// A human readable message indicating details about the transition.
	Message string `json:"message,omitempty"`
}

type MembersStatus struct {
	// Ready are the etcd members that are ready to serve requests
	// The member names are the same as the etcd pod names
	Ready []string `json:"ready,omitempty"`
	// Unready are the etcd members not ready to serve requests
	Unready []string `json:"unready,omitempty"`
}

// EtcdClusterStatus defines the observed state of EtcdCluster
type EtcdClusterStatus struct {
	// Phase is the brief state of cluster
	Phase string `json:"phase,omitempty"`

	// ControlPaused indicates the operator pauses the control of the cluster.
	// +kubebuilder:default=false
	ControlPaused bool `json:"controlPaused,omitempty"`

	// Condition keeps track of all cluster conditions, if they exist.
	// See https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md
	// https://maelvls.dev/kubernetes-conditions/
	Conditions []ClusterCondition `json:"conditions,omitempty"`

	// Replicas is the current size of the cluster
	// +kubebuilder:default=0
	Replicas int `json:"replicas,omitempty"`

	// ServiceName is the LB service for accessing etcd nodes.
	ServiceName string `json:"serviceName,omitempty"`

	// ClientPort is the port for etcd client to access.
	// It's the same on client LB service and etcd nodes.
	ClientPort int `json:"clientPort,omitempty"`

	// Members are the etcd members in the cluster
	Members MembersStatus `json:"members,omitempty"`

	// LabelSelector is for hpa
	LabelSelector string `json:"labelSelector,omitempty"`
}

// https://medium.com/@thescott111/autoscaling-kubernetes-custom-resource-using-the-hpa-957d00bb7993
// https://kubernetes.io/zh/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/
// https://kubernetes.io/docs/concepts/workloads/pods/disruptions/
// https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.labelSelector

// EtcdCluster is the Schema for the etcdclusters API
type EtcdCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdClusterSpec   `json:"spec,omitempty"`
	Status EtcdClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// EtcdClusterList contains a list of EtcdCluster
type EtcdClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EtcdCluster{}, &EtcdClusterList{})
}
