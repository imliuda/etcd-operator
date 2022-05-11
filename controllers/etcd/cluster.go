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
	"github.com/coreos/etcd/clientv3"
	etcdv1alpha1 "github.com/imliuda/etcd-operator/apis/etcd/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (r *EtcdClusterReconciler) getEtcdClient(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, members MemberSet) (*clientv3.Client, error) {
	// etcdctl member add
	var err error
	cfg := clientv3.Config{
		Endpoints:   members.ClientURLs(),
		DialTimeout: DefaultDialTimeout,
	}

	if cluster.Spec.TLS.IsSecureClient() {
		secret := &v1.Secret{}
		if err := r.Client.Get(ctx, types.NamespacedName{Namespace: cluster.Name, Name: cluster.Spec.TLS.Static.OperatorSecret}, secret); err != nil {
			return nil, err
		}
		cfg.TLS, err = NewTLSConfig(secret.Data[CliCertFile], secret.Data[CliKeyFile], secret.Data[CliCAFile])
		if err != nil {
			return nil, err
		}
	}

	logger.V(5).Info("Etcd client config", "config", cfg)

	etcdcli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return etcdcli, nil
}

func (r *EtcdClusterReconciler) addMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, ms MemberSet, m *Member) error {
	logger.Info("Starting add new member to cluster", "cluster", cluster.Name)
	defer logger.Info("End add new member to cluster", "cluster", cluster.Name)

	cli, err := r.getEtcdClient(ctx, cluster, ms)
	if err != nil {
		return err
	}
	defer cli.Close()

	etcdctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	defer cancel()

	// Get existing members
	list, err := cli.MemberList(etcdctx)
	if err != nil {
		return err
	}

	// Already exists
	for _, e := range list.Members {
		if e.Name == m.Name {
			return nil
		}
	}

	if _, err = cli.MemberAdd(etcdctx, []string{m.PeerURL()}); err != nil {
		return err
	}

	return nil
}

// Check error first
func (r *EtcdClusterReconciler) memberExists(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, ms MemberSet, m *Member) (bool, error) {
	cli, err := r.getEtcdClient(ctx, cluster, ms)
	if err != nil {
		return false, err
	}
	defer cli.Close()

	etcdctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	defer cancel()

	// Get existing members
	list, err := cli.MemberList(etcdctx)
	if err != nil {
		return false, err
	}

	for _, e := range list.Members {
		if e.Name == m.Name {
			return true, nil
		}
	}

	return false, nil
}

func (r *EtcdClusterReconciler) removeMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, ms MemberSet, m *Member) error {
	cli, err := r.getEtcdClient(ctx, cluster, ms)
	if err != nil {
		return err
	}
	defer cli.Close()

	etcdctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	defer cancel()

	// Get existing members
	list, err := cli.MemberList(etcdctx)
	if err != nil {
		return err
	}

	// Remove
	for _, e := range list.Members {
		if e.Name == m.Name {
			if _, err = cli.MemberRemove(etcdctx, e.ID); err != nil {
				return err
			}
		}
	}

	return nil
}
