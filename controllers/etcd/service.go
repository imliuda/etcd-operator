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
	etcdv1alpha1 "github.com/imliuda/etcd-operator/apis/etcd/v1alpha1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// ClientPort is the client port on client service and etcd nodes.
	ClientPort = 2379
	PeerPort   = 2380
)

func NewEtcdService(cluster *etcdv1alpha1.EtcdCluster, svcName, clusterIP string, ports []v1.ServicePort) *v1.Service {
	labels_ := LabelsForCluster(cluster)
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:   svcName,
			Labels: labels_,
		},
		Spec: v1.ServiceSpec{
			Ports:                    ports,
			Selector:                 labels_,
			ClusterIP:                clusterIP,
			PublishNotReadyAddresses: true,
		},
	}
	return service
}

func ClientServiceName(cluster *etcdv1alpha1.EtcdCluster) string {
	return cluster.Name + "-peer"
}

func NewClientService(cluster *etcdv1alpha1.EtcdCluster) *v1.Service {
	ports := []v1.ServicePort{{
		Name:       "client",
		Port:       ClientPort,
		TargetPort: intstr.FromInt(ClientPort),
		Protocol:   v1.ProtocolTCP,
	}}
	service := NewEtcdService(cluster, ClientServiceName(cluster), "", ports)
	return service
}

func PeerServiceName(cluster *etcdv1alpha1.EtcdCluster) string {
	return cluster.Name + "-peer"
}

func NewPeerService(cluster *etcdv1alpha1.EtcdCluster) *v1.Service {
	ports := []v1.ServicePort{{
		Name:       "client",
		Port:       ClientPort,
		TargetPort: intstr.FromInt(ClientPort),
		Protocol:   v1.ProtocolTCP,
	}, {
		Name:       "peer",
		Port:       PeerPort,
		TargetPort: intstr.FromInt(PeerPort),
		Protocol:   v1.ProtocolTCP,
	}}
	service := NewEtcdService(cluster, PeerServiceName(cluster), "", ports)
	return service
}
