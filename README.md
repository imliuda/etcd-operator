# etcd-operator

本项目主要用于研究用途，探索如何在kubernetes中全自动托管有状态服务，包括集群部署，（自动）扩缩容，升级与回滚，自动化配置，服务管理，故障自愈，
备份与恢复，监控与告警等。本项目主要借鉴了 [官方版本](https://github.com/coreos/etcd-operator) 的operator的实现。 本项目在官方版本的
功能基础上，计划增加自动化证书配置功能(cert-manager)，HPA配置，监控与告警功能（prometheus）。

## 快速开始

下载源码，运行下列命令将operator部署到集群中：

```shell
# 制作镜像，通过${IMAGE_AND_TAG}指定镜像的名称和版本
make docker-build IMG=${IMAGE_AND_TAG}
# 导入镜像，传到指定仓库
docker save ${IMAGE_AND_TAG} -o controller.tar
docker push ${IMAGE_AND_TAG}
# 部署到集群
make deploy IMG=${IMAGE_AND_TAG}
```

部署完毕后，就可以创建集群了。etcd需要使用持久化存储，所以需要确保集群中已部署pvc控制器，以便动态供给存储。

通过下列命令创建一个新的集群：

```shell
cat <<EOF > cluster.yaml
apiVersion: etcd.imliuda.github.io/v1alpha1
kind: EtcdCluster
metadata:
  name: etcd-cluster
spec:
  replicas: 3
  version: "3.2.13"
  pod:
    persistentVolumeClaimSpec:
      accessModes: 
      - ReadWriteOnce
      volumeMode: Filesystem
      resources:
        requests:
          storage: 1Gi
      storageClassName: nfs-client
EOF
kubectl apply -f cluster.yaml
```

## 文档参考

本项目在实现时，采用了一种不同于原项目的控制器模型，通过使用event和reconcile互相协作的方式，促使集群达到期望的状态。控制器本身只关心spec描述，
不使用status存储状态信息。status仅仅用来描述当前这个资源的具体状态，如集群状态，如副本数，成员信息，事件等。在实现控制器具体功能时，充分考虑了
资源在本地缓存更新不及时的情况，接口幂等性等问题。具体参考[reconcile](docs/reconcile.md)文档。

本项目所使用的的相关如下相关技术模块，三方控制器，各功能可参考官方文档或源码：

- kube-builder 项目的脚手架工具
- controller-runtime 自定义控制器框架
- client-go kubernetes客户端工具
- apimachinery 常用api工具
- cert-manager 自动化证书配置
- prometheus 集群监控
- openebs 使用本地高性能磁盘

针对本项目的具体开发文档可参考：

- [开发说明](docs/development.md)
- [控制器设计](docs/reconcile.md)

## 后续功能

- [ ] 添加component config实现自定义配置
- [ ] 添加webhook做接口校验等工作
- [ ] 自动化证书配置功能
- [ ] PodMonitor监控配置
- [ ] 备份与恢复功能
- [ ] 替换节点功能

## 联系

通过本项目，期望能够总结一些自定义控制器开发的常规套路和方法论，尤其是针对对于有状态服务的自动化管理功能。如果您对本项目感兴趣，可通过下列方式加
入群组讨论：微信群加`Blue_L`，QQ群搜`450203387`。