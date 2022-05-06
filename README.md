# etcd-operator

本项目主要用于研究用途，探索如何在kubernetes中全自动托管有状态服务，包括集群部署，（自动）扩缩容，升级与回滚，自动化配置，服务管理，故障自愈，
备份与恢复，监控与告警等。本项目主要借鉴了 [官方版本](https://github.com/coreos/etcd-operator) 的operator的实现。 本项目在官方版本的
功能基础上，计划增加自动化证书配置功能(cert-manager)，HPA配置，监控与告警功能（prometheus）。

本项目在实现时，采用了一种不同于原项目的控制器模型，通过使用event和reconcile互相协作的方式，促使集群达到期望的状态。控制器本身只关心spec描述，
不使用status存储状态信息。status仅仅用来描述当前这个资源的具体状态，如集群状态，如副本数，成员信息，事件等。在实现控制器具体功能时，充分考虑了
资源在本地缓存更新不及时的情况，接口幂等性等问题。reconcile本身是一个状态机，每次执行的时候只做一个具体的特定功能，如创建资源，修改资源状态等，
期望下一个状态或需要异步等待时，通过requeue重新再次执行reconcile实现。

本项目所使用的的相关技术框架有kube-builder, controller-runtime, client-go, apimachinery等，可能使用的第三方控制器有cert-manager，
prometheus，openebs等。

通过本项目，期望能够总结一些自定义控制器开发的常规套路和方法论，尤其是针对对于有状态服务的自动化管理功能。如果您对本项目感兴趣，可通过下列方式加
入群组讨论：微信群加`Blue_L`，QQ群搜`450203387`。