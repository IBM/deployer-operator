// Copyright 2019 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deployer

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sigappv1beta1 "github.com/kubernetes-sigs/application/pkg/apis/app/v1beta1"

	apis "github.com/IBM/deployer-operator/pkg/apis"
	dplappv1alpha1 "github.com/IBM/multicloud-operators-deployable/pkg/apis"
)

const (
	clusterNameOnHub      = "reave"
	clusterNamespaceOnHub = "reave"
)

var (
	managedClusterConfig *rest.Config
	hubClusterConfig     *rest.Config

	managedClusterClient client.Client
	hubClusterClient     client.Client

	mgr      manager.Manager
	eh       ExplorerHandler
	requests chan reconcile.Request
	recFn    reconcile.Reconciler

	// managed cluster namespace on hub
	clusterOnHub = types.NamespacedName{
		Name:      clusterNameOnHub,
		Namespace: clusterNamespaceOnHub,
	}
)

func TestMain(m *testing.M) {
	// setup the managed cluster environment
	managedCluster := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "deploy", "crds"),
			filepath.Join("..", "..", "..", "hack", "test"),
		},
	}

	// add eployer-operator scheme
	err := apis.AddToScheme(scheme.Scheme)
	if err != nil {
		log.Fatal(err)
	}

	// add multicloud-operators-deployable scheme
	err = dplappv1alpha1.AddToScheme(scheme.Scheme)
	if err != nil {
		log.Fatal(err)
	}

	// add application scheme
	err = sigappv1beta1.AddToScheme(scheme.Scheme)
	if err != nil {
		log.Fatal(err)
	}

	if managedClusterConfig, err = managedCluster.Start(); err != nil {
		log.Fatal(err)
	}

	// setup the hub cluster environment
	hubCluster := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "deploy", "crds"),
			filepath.Join("..", "..", "..", "hack", "test"),
		},
	}

	if hubClusterConfig, err = hubCluster.Start(); err != nil {
		log.Fatal(err)
	}

	mgr, err = manager.New(managedClusterConfig, manager.Options{
		MetricsBindAddress: "0",
		Scheme:             scheme.Scheme,
	})
	if err != nil {
		log.Fatal(err)
	}

	managedClusterClient = mgr.GetClient()
	innerHubClient, err := client.New(hubClusterConfig, client.Options{})
	if err != nil {
		log.Fatal(err)
	}
	hubClusterClient = SetupHubClient(innerHubClient)

	stopMgr, mgrStopped := StartTestManager(mgr)
	defer func() {
		close(stopMgr)
		mgrStopped.Wait()
	}()

	eh.initExplorerHandler(hubClusterConfig, mgr.GetConfig(), clusterOnHub)

	rec := newReconciler(mgr, hubClusterClient, clusterOnHub, &eh)
	recFn, requests = SetupTestReconcile(rec)

	if err = add(mgr, recFn); err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	managedCluster.Stop()
	hubCluster.Stop()
	os.Exit(code)
}

const waitgroupDelta = 1

type HubClient struct {
	client.Client
	createCh chan runtime.Object
	deleteCh chan runtime.Object
	updateCh chan runtime.Object
}

func (hubClient HubClient) Create(ctx context.Context, obj runtime.Object, opts ...client.CreateOption) error {
	err := hubClient.Client.Create(ctx, obj, opts...)
	// non-blocking operation
	select {
	case hubClient.createCh <- obj:
	default:
	}
	return err
}

func (hubClient HubClient) Delete(ctx context.Context, obj runtime.Object, opts ...client.DeleteOption) error {
	err := hubClient.Client.Delete(ctx, obj, opts...)
	// non-blocking operation
	select {
	case hubClient.deleteCh <- obj:
	default:
	}
	return err
}

func (hubClient HubClient) Update(ctx context.Context, obj runtime.Object, opts ...client.UpdateOption) error {
	err := hubClient.Client.Update(ctx, obj, opts...)
	// non-blocking operation
	select {
	case hubClient.updateCh <- obj:
	default:
	}
	return err
}

func SetupHubClient(innerClient client.Client) client.Client {
	cCh := make(chan runtime.Object)
	dCh := make(chan runtime.Object)
	uCh := make(chan runtime.Object)

	hubClient := HubClient{
		Client:   innerClient,
		createCh: cCh,
		deleteCh: dCh,
		updateCh: uCh,
	}
	return hubClient
}

func SetupTestReconcile(inner reconcile.Reconciler) (reconcile.Reconciler, chan reconcile.Request) {
	requests := make(chan reconcile.Request)
	fn := reconcile.Func(func(req reconcile.Request) (reconcile.Result, error) {
		result, err := inner.Reconcile(req)
		requests <- req
		return result, err
	})

	return fn, requests
}

func StartTestManager(mgr manager.Manager) (chan struct{}, *sync.WaitGroup) {
	stop := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(waitgroupDelta)

	go func() {
		defer wg.Done()
		err := mgr.Start(stop)
		if err != nil {
			log.Fatal(err)
		}
	}()

	return stop, wg
}
