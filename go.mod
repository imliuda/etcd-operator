module github.com/imliuda/etcd-operator

go 1.16

require (
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/google/uuid v1.1.2
	github.com/onsi/ginkgo v1.16.4
	github.com/onsi/gomega v1.15.0
	google.golang.org/grpc v1.46.0 // indirect
	k8s.io/api v0.22.1
	k8s.io/apimachinery v0.22.1
	k8s.io/client-go v0.22.1
	sigs.k8s.io/controller-runtime v0.10.0
)

replace google.golang.org/grpc v1.46.0 => google.golang.org/grpc v1.26.0
