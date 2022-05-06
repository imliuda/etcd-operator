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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

const (
	etcdVolumeName                = "etcd-data"
	etcdVolumeMountDir            = "/var/etcd"
	dataDir                       = etcdVolumeMountDir + "/data"
	peerTLSDir                    = "/etc/etcdtls/member/peer-tls"
	peerTLSVolume                 = "member-peer-tls"
	serverTLSDir                  = "/etc/etcdtls/member/server-tls"
	serverTLSVolume               = "member-server-tls"
	operatorEtcdTLSDir            = "/etc/etcdtls/operator/etcd-tls"
	operatorEtcdTLSVolume         = "etcd-client-tls"

	// defaultDNSTimeout is the default maximum allowed time for the init container of the etcd pod
	// to reverse DNS lookup its IP. The default behavior is to wait forever and has a value of 0.
	defaultDNSTimeout = int64(0)

	defaultBusyboxImage = "busybox:1.28.0-glibc"

	CliCertFile = "etcd-client.crt"
	CliKeyFile  = "etcd-client.key"
	CliCAFile   = "etcd-client-ca.crt"
)

func GetEtcdVersion(pod *v1.Pod) string {
	return pod.Labels[etcdv1alpha1.AppVersionLabel]
}

func SetEtcdVersion(pod *v1.Pod, version string) {
	pod.Labels[etcdv1alpha1.AppVersionLabel] = version
}

func GetPodNames(pods []*v1.Pod) []string {
	if len(pods) == 0 {
		return nil
	}
	res := make([]string, 0)
	for _, p := range pods {
		res = append(res, p.Name)
	}
	return res
}

// PVCNameFromMember the way we get PVC name from the member name
func PVCNameFromMember(memberName string) string {
	return memberName
}

func ImageName(repo, version string) string {
	return fmt.Sprintf("%s:v%v", repo, version)
}

func PodWithNodeSelector(p *v1.Pod, ns map[string]string) *v1.Pod {
	p.Spec.NodeSelector = ns
	return p
}

func LabelsForCluster(cluster *etcdv1alpha1.EtcdCluster) map[string]string {
	return map[string]string{
		etcdv1alpha1.ClusterNameLabel: cluster.Name,
		etcdv1alpha1.AppNameLabel:    "etcd",
	}
}

// AddEtcdVolumeToPod abstract the process of appending volume spec to pod spec
func AddEtcdVolumeToPod(pod *v1.Pod, pvc *v1.PersistentVolumeClaim) {
	vol := v1.Volume{Name: etcdVolumeName}
	if pvc != nil {
		vol.VolumeSource = v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
		}
	} else {
		vol.VolumeSource = v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, vol)
}

func NewEtcdPod(cluster *etcdv1alpha1.EtcdCluster, m *Member, initialCluster []string, state string) *v1.Pod {
	pod := newEtcdPod(cluster, m, initialCluster, state)
	applyPodPolicy(pod, cluster.Spec.Pod)
	return pod
}

// NewEtcdPodPVC create PVC object from etcd pod's PVC spec
func NewEtcdPodPVC(cluster *etcdv1alpha1.EtcdCluster, m *Member) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PVCNameFromMember(m.Name),
			Namespace: cluster.Namespace,
			Labels:    LabelsForCluster(cluster),
		},
		Spec: *cluster.Spec.Pod.PersistentVolumeClaimSpec,
	}
	return pvc
}

func newEtcdPod(cluster *etcdv1alpha1.EtcdCluster, m *Member, initialCluster []string, state string) *v1.Pod {
	commands := fmt.Sprintf("/usr/local/bin/etcd --data-dir=%s --name=%s --initial-advertise-peer-urls=%s "+
		"--listen-peer-urls=%s --listen-client-urls=%s --advertise-client-urls=%s "+
		"--initial-cluster=%s --initial-cluster-state=%s",
		dataDir, m.Name, m.PeerURL(), m.ListenPeerURL(), m.ListenClientURL(), m.ClientURL(), strings.Join(initialCluster, ","), state)
	if m.SecurePeer {
		commands += fmt.Sprintf(" --peer-client-cert-auth=true --peer-trusted-ca-file=%[1]s/peer-ca.crt --peer-cert-file=%[1]s/peer.crt --peer-key-file=%[1]s/peer.key", peerTLSDir)
	}
	if m.SecureClient {
		commands += fmt.Sprintf(" --client-cert-auth=true --trusted-ca-file=%[1]s/server-ca.crt --cert-file=%[1]s/server.crt --key-file=%[1]s/server.key", serverTLSDir)
	}
	if state == "new" {
		commands = fmt.Sprintf("%s --initial-cluster-token=%s", commands, cluster.Name)
	}

	livenessProbe := newEtcdProbe(cluster.Spec.TLS.IsSecureClient())
	readinessProbe := newEtcdProbe(cluster.Spec.TLS.IsSecureClient())
	readinessProbe.InitialDelaySeconds = 1
	readinessProbe.TimeoutSeconds = 5
	readinessProbe.PeriodSeconds = 5
	readinessProbe.FailureThreshold = 3

	container := containerWithProbes(
		etcdContainer(strings.Split(commands, " "),
			cluster.Spec.Repository, cluster.Spec.Version),
		livenessProbe,
		readinessProbe)

	volumes := []v1.Volume{}

	if m.SecurePeer {
		container.VolumeMounts = append(container.VolumeMounts, v1.VolumeMount{
			MountPath: peerTLSDir,
			Name:      peerTLSVolume,
		})
		volumes = append(volumes, v1.Volume{Name: peerTLSVolume, VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{SecretName: cluster.Spec.TLS.Static.Member.PeerSecret},
		}})
	}
	if m.SecureClient {
		container.VolumeMounts = append(container.VolumeMounts, v1.VolumeMount{
			MountPath: serverTLSDir,
			Name:      serverTLSVolume,
		}, v1.VolumeMount{
			MountPath: operatorEtcdTLSDir,
			Name:      operatorEtcdTLSVolume,
		})
		volumes = append(volumes, v1.Volume{Name: serverTLSVolume, VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{SecretName: cluster.Spec.TLS.Static.Member.ServerSecret},
		}}, v1.Volume{Name: operatorEtcdTLSVolume, VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{SecretName: cluster.Spec.TLS.Static.OperatorSecret},
		}})
	}

	DNSTimeout := defaultDNSTimeout
	if cluster.Spec.Pod != nil {
		DNSTimeout = cluster.Spec.Pod.DNSTimeoutInSecond
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        m.Name,
			Namespace:   cluster.Namespace,
			Labels:      LabelsForCluster(cluster),
			Annotations: map[string]string{},
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{{
				// busybox:latest uses uclibc which contains a bug that sometimes prevents name resolution
				// More info: https://github.com/docker-library/busybox/issues/27
				//Image default: "busybox:1.28.0-glibc",
				Image: imageNameBusybox(cluster.Spec.Pod),
				Name:  "check-dns",
				// In etcd 3.2, TLS listener will do a reverse-DNS lookup for pod IP -> hostname.
				// If DNS entry is not warmed up, it will return empty result and peer connection will be rejected.
				// In some cases the DNS is not created correctly so we need to time out after a given period.
				Command: []string{"/bin/sh", "-c", fmt.Sprintf(`
					TIMEOUT_READY=%d
					while ( ! nslookup %s )
					do
						# If TIMEOUT_READY is 0 we should never time out and exit 
						TIMEOUT_READY=$(( TIMEOUT_READY-1 ))
                        if [ $TIMEOUT_READY -eq 0 ];
				        then
				            echo "Timed out waiting for DNS entry"
				            exit 1
				        fi
						sleep 1
					done`, DNSTimeout, m.Addr())},
			}},
			Containers:    []v1.Container{container},
			RestartPolicy: v1.RestartPolicyAlways,
			Volumes:       volumes,
			// DNS A record: `[m.Name].[cluster.Name].Namespace.svc`
			// For example, etcd-795649v9kq in default namesapce will have DNS name
			// `etcd-795649v9kq.etcd.default.svc`.
			Hostname:                     m.Name,
			Subdomain:                    PeerServiceName(cluster),
			AutomountServiceAccountToken: func(b bool) *bool { return &b }(false),
			SecurityContext:              podSecurityContext(cluster.Spec.Pod),
		},
	}
	SetEtcdVersion(pod, cluster.Spec.Version)
	return pod
}

func etcdContainer(cmd []string, repo, version string) v1.Container {
	c := v1.Container{
		Command: cmd,
		Name:    "etcd",
		Image:   ImageName(repo, version),
		Ports: []v1.ContainerPort{
			{
				Name:          "server",
				ContainerPort: int32(PeerPort),
				Protocol:      v1.ProtocolTCP,
			},
			{
				Name:          "client",
				ContainerPort: int32(ClientPort),
				Protocol:      v1.ProtocolTCP,
			},
		},
		VolumeMounts: etcdVolumeMounts(),
	}

	return c
}

func applyPodPolicy(pod *v1.Pod, policy *etcdv1alpha1.PodPolicy) {
	if policy == nil {
		return
	}

	if policy.Affinity != nil {
		pod.Spec.Affinity = policy.Affinity
	}

	if len(policy.NodeSelector) != 0 {
		pod = PodWithNodeSelector(pod, policy.NodeSelector)
	}
	if len(policy.Tolerations) != 0 {
		pod.Spec.Tolerations = policy.Tolerations
	}

	mergeLabels(pod.Labels, policy.Labels)

	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i] = containerWithRequirements(pod.Spec.Containers[i], policy.Resources)
		if pod.Spec.Containers[i].Name == "etcd" {
			pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, policy.Envs...)
		}
	}

	for i := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i] = containerWithRequirements(pod.Spec.InitContainers[i], policy.Resources)
	}

	for key, value := range policy.Annotations {
		pod.ObjectMeta.Annotations[key] = value
	}
}

func etcdVolumeMounts() []v1.VolumeMount {
	return []v1.VolumeMount{
		{Name: etcdVolumeName, MountPath: etcdVolumeMountDir},
	}
}

// mergeLabels merges l2 into l1. Conflicting label will be skipped.
func mergeLabels(l1, l2 map[string]string) {
	for k, v := range l2 {
		if _, ok := l1[k]; ok {
			continue
		}
		l1[k] = v
	}
}

func podSecurityContext(podPolicy *etcdv1alpha1.PodPolicy) *v1.PodSecurityContext {
	if podPolicy == nil {
		return nil
	}
	return podPolicy.SecurityContext
}

func newEtcdProbe(isSecure bool) *v1.Probe {
	// etcd pod is healthy only if it can participate in consensus
	cmd := "ETCDCTL_API=3 etcdctl endpoint status"
	if isSecure {
		tlsFlags := fmt.Sprintf("--cert=%[1]s/%[2]s --key=%[1]s/%[3]s --cacert=%[1]s/%[4]s", operatorEtcdTLSDir, CliCertFile, CliKeyFile, CliCAFile)
		cmd = fmt.Sprintf("ETCDCTL_API=3 etcdctl --endpoints=https://localhost:%d %s endpoint status", ClientPort, tlsFlags)
	}
	return &v1.Probe{
		Handler: v1.Handler{
			Exec: &v1.ExecAction{
				Command: []string{"/bin/sh", "-ec", cmd},
			},
		},
		InitialDelaySeconds: 10,
		TimeoutSeconds:      10,
		PeriodSeconds:       60,
		FailureThreshold:    3,
	}
}

func containerWithProbes(c v1.Container, lp *v1.Probe, rp *v1.Probe) v1.Container {
	c.LivenessProbe = lp
	c.ReadinessProbe = rp
	return c
}

func containerWithRequirements(c v1.Container, r v1.ResourceRequirements) v1.Container {
	c.Resources = r
	return c
}

// imageNameBusybox returns the default image for busybox init container, or the image specified in the PodPolicy
func imageNameBusybox(policy *etcdv1alpha1.PodPolicy) string {
	if policy != nil && len(policy.BusyboxImage) > 0 {
		return policy.BusyboxImage
	}
	return defaultBusyboxImage
}

func IsRunningAndReady(pod *v1.Pod) bool {
	return pod.Status.Phase == v1.PodRunning && IsPodReady(pod)
}

// IsPodReady returns true if a pod is ready; false otherwise.
func IsPodReady(pod *v1.Pod) bool {
	return IsPodReadyConditionTrue(pod.Status)
}

// IsPodReadyConditionTrue returns true if a pod is ready; false otherwise.
func IsPodReadyConditionTrue(status v1.PodStatus) bool {
	condition := GetPodReadyCondition(status)
	return condition != nil && condition.Status == v1.ConditionTrue
}

// GetPodReadyCondition extracts the pod ready condition from the given status and returns that.
// Returns nil if the condition is not present.
func GetPodReadyCondition(status v1.PodStatus) *v1.PodCondition {
	_, condition := GetPodCondition(&status, v1.PodReady)
	return condition
}

// GetPodCondition extracts the provided condition from the given status and returns that.
// Returns nil and -1 if the condition is not present, and the index of the located condition.
func GetPodCondition(status *v1.PodStatus, conditionType v1.PodConditionType) (int, *v1.PodCondition) {
	if status == nil {
		return -1, nil
	}
	return GetPodConditionFromList(status.Conditions, conditionType)
}

// GetPodConditionFromList extracts the provided condition from the given list of condition and
// returns the index of the condition and the condition. Returns -1 and nil if the condition is not present.
func GetPodConditionFromList(conditions []v1.PodCondition, conditionType v1.PodConditionType) (int, *v1.PodCondition) {
	if conditions == nil {
		return -1, nil
	}
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return i, &conditions[i]
		}
	}
	return -1, nil
}
