package etcd

import (
	"context"
	"fmt"
	etcdv1alpha1 "github.com/imliuda/etcd-operator/apis/etcd/v1alpha1"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"time"
)

// See sigs.k8s.io/controller-runtime/pkg/cache/cache.go
// Options.Resync is apply to all informers
//
// If no event occurred to EtcdCluster resources, we have chance to know what
// the current cluster's real status, there may be some problem has happened in members.

func (r *EtcdClusterReconciler) Start(ctx context.Context) error {
	// TODO Make resync period is configurable
	period, _ := time.ParseDuration("120s")
	select {
	case <-time.After(period):
		r.clusters.Range(func(key, value interface{}) bool {
			go func() {
				realSleep := rand.Int63nRange(int64(0), int64(period))
				time.Sleep(time.Duration(realSleep))
				cluster := value.(*etcdv1alpha1.EtcdCluster)
				r.resyncCh <- event.GenericEvent{Object: cluster}
			}()
			return true
		})
	case <-ctx.Done():
		return nil
	}
	return nil
}

func (r *EtcdClusterReconciler) getNamespacedName(c *etcdv1alpha1.EtcdCluster) string {
	return fmt.Sprintf("%s/%s", c.Namespace, c.Name)
}

// NeedLeaderElection will ensure Start() will be called after cache is synced
func (r *EtcdClusterReconciler) NeedLeaderElection() bool {
	return true
}
