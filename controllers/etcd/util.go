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
	"crypto/tls"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"io/ioutil"
	"path/filepath"
	"time"
)

const (
	DefaultDialTimeout    = 10 * time.Second
	DefaultRequestTimeout = 10 * time.Second
)

func ListMembers(clientURLs []string, tc *tls.Config) (*clientv3.MemberListResponse, error) {
	cfg := clientv3.Config{
		Endpoints:   clientURLs,
		DialTimeout: DefaultDialTimeout,
		TLS:         tc,
	}
	etcdcli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("list members failed: creating etcd client failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	resp, err := etcdcli.MemberList(ctx)
	cancel()
	etcdcli.Close()
	return resp, err
}

func RemoveMember(clientURLs []string, tc *tls.Config, id uint64) error {
	cfg := clientv3.Config{
		Endpoints:   clientURLs,
		DialTimeout: DefaultDialTimeout,
		TLS:         tc,
	}
	etcdcli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer etcdcli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), DefaultRequestTimeout)
	_, err = etcdcli.Cluster.MemberRemove(ctx, id)
	cancel()
	return err
}

func NewTLSConfig(certData, keyData, caData []byte) (*tls.Config, error) {
	// TODO: Need cleanup these temp dirs
	dir, err := ioutil.TempDir("", "etcd-operator-cluster-tls")
	if err != nil {
		return nil, err
	}

	certFile, err := writeFile(dir, CliCertFile, certData)
	if err != nil {
		return nil, err
	}
	keyFile, err := writeFile(dir, CliKeyFile, keyData)
	if err != nil {
		return nil, err
	}
	caFile, err := writeFile(dir, CliCAFile, caData)
	if err != nil {
		return nil, err
	}

	tlsInfo := transport.TLSInfo{
		CertFile:      certFile,
		KeyFile:       keyFile,
		TrustedCAFile: caFile,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, err
	}
	return tlsConfig, nil
}

func writeFile(dir, file string, data []byte) (string, error) {
	p := filepath.Join(dir, file)
	return p, ioutil.WriteFile(p, data, 0600)
}
