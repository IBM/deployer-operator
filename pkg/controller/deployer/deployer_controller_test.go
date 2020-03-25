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
	"encoding/json"
	"testing"
	"time"

	sigappv1beta1 "github.com/kubernetes-sigs/application/pkg/apis/app/v1beta1"
	"github.com/onsi/gomega"

	appv1alpha1 "github.com/IBM/deployer-operator/pkg/apis/app/v1alpha1"
	dplappv1alpha1 "github.com/IBM/multicloud-operators-deployable/pkg/apis/app/v1alpha1"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	cloudformType = "cloudform"

	deployerName      = "test-deployer-operator"
	deployerNamespace = "default"

	webServiceName   = "wordpress-webserver-svc"
	webSTSName       = "wordpress-webserver"
	applicationName  = "wordpress-01"
	appLabelSelector = "app.kubernetes.io/name"
)

var (
	// A Deployer resource with metadata and spec.
	managedClusterDeployer = &appv1alpha1.Deployer{
		TypeMeta: metav1.TypeMeta{
			Kind: "Deployer",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        deployerName,
			Namespace:   deployerNamespace,
			Annotations: map[string]string{appv1alpha1.IsDefaultDeployer: "true"},
		},
		Spec: appv1alpha1.DeployerSpec{
			Type: cloudformType,
		},
	}

	webServicePort = v1.ServicePort{
		Port: 3306,
	}

	webService = &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind: "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      webServiceName,
			Namespace: deployerNamespace,
			Labels:    map[string]string{appLabelSelector: applicationName},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{webServicePort},
		},
	}

	webSTS = &apps.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind: "StatefulSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      webSTSName,
			Namespace: deployerNamespace,
			Labels:    map[string]string{appLabelSelector: applicationName},
		},
		Spec: apps.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{appLabelSelector: applicationName},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{appLabelSelector: applicationName},
				},
			},
		},
	}

	wordpressAppGK1 = metav1.GroupKind{
		Group: "v1",
		Kind:  "Service",
	}
	wordpressAppGK2 = metav1.GroupKind{
		Group: "apps",
		Kind:  "StatefulSet",
	}

	wordpressApp = &sigappv1beta1.Application{
		TypeMeta: metav1.TypeMeta{
			Kind: "Application",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      applicationName,
			Namespace: deployerNamespace,
			Labels:    map[string]string{appLabelSelector: applicationName},
		},
		Spec: sigappv1beta1.ApplicationSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{appLabelSelector: applicationName},
			},
			ComponentGroupKinds: []metav1.GroupKind{
				wordpressAppGK1,
				wordpressAppGK2,
			},
		},
	}

	// deployer object
	key = types.NamespacedName{
		Name:      deployerName,
		Namespace: deployerNamespace,
	}

	expectedRequest = reconcile.Request{NamespacedName: key}

	timeout = time.Second * 2

	exp Explorer
)

func TestReconcile(t *testing.T) {
	g := gomega.NewWithT(t)

	// Create the Deployer object and expect the Reconcile and Deployment to be created
	dep := managedClusterDeployer.DeepCopy()
	g.Expect(managedClusterClient.Create(context.TODO(), dep)).NotTo(gomega.HaveOccurred())

	// reconcile.Request
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))

	// deployer in managed cluster
	deployerResource := &appv1alpha1.Deployer{}
	g.Expect(managedClusterClient.Get(context.TODO(), expectedRequest.NamespacedName, deployerResource)).NotTo(gomega.HaveOccurred())

	// don't use dep at this point for assertion, as the client.Create nulled out the TypeMeta.
	g.Expect(deployerResource.TypeMeta.Kind).To(gomega.Equal(managedClusterDeployer.TypeMeta.Kind))
	g.Expect(deployerResource.ObjectMeta.Name).To(gomega.Equal(managedClusterDeployer.ObjectMeta.Name))
	g.Expect(deployerResource.ObjectMeta.Namespace).To(gomega.Equal(managedClusterDeployer.ObjectMeta.Namespace))
	g.Expect(deployerResource.Spec).To(gomega.Equal(managedClusterDeployer.Spec))

	// don't use defer for cleanup, as we weant to go through reconcile logic to make sure hubclient
	// cache stays consistent
	managedClusterClient.Delete(context.TODO(), dep)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))
}

func TestDeployersetCreatedOnHub(t *testing.T) {
	g := gomega.NewWithT(t)

	dep := managedClusterDeployer.DeepCopy()
	g.Expect(managedClusterClient.Create(context.TODO(), dep)).NotTo(gomega.HaveOccurred())

	// reconcile.Request
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))

	// deployerset in hub cluster
	deployersetResource := &appv1alpha1.DeployerSet{}
	g.Expect(hubClusterClient.Get(context.TODO(), clusterOnHub, deployersetResource)).NotTo(gomega.HaveOccurred())
	g.Expect(deployersetResource.ObjectMeta.Name).To(gomega.Equal(clusterNameOnHub))
	g.Expect(deployersetResource.ObjectMeta.Namespace).To(gomega.Equal(clusterNamespaceOnHub))
	g.Expect(deployersetResource.Spec.DefaultDeployer).To(gomega.Equal(deployerNamespace + "/" + deployerName))
	g.Expect(deployersetResource.Spec.Deployers).To(gomega.HaveLen(1))
	g.Expect(deployersetResource.Spec.Deployers[0].Spec).To(gomega.Equal(managedClusterDeployer.Spec))

	// don't use defer for cleanup, as we weant to go through reconcile logic to make sure hubclient
	// cache stays consistent
	managedClusterClient.Delete(context.TODO(), dep)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))
}

func TestDeployersetRemovedFromHub(t *testing.T) {
	g := gomega.NewWithT(t)

	dep := managedClusterDeployer.DeepCopy()
	g.Expect(managedClusterClient.Create(context.TODO(), dep)).NotTo(gomega.HaveOccurred())

	// reconcile.Request
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))

	managedClusterClient.Delete(context.TODO(), dep)

	// reconcile.Request
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))

	// deployerset in hub cluster
	deployersetResource := &appv1alpha1.DeployerSet{}
	err := hubClusterClient.Get(context.TODO(), clusterOnHub, deployersetResource)
	g.Expect(errors.IsNotFound(err)).To(gomega.BeTrue())
}

func TestApplicationDiscovery(t *testing.T) {
	g := gomega.NewWithT(t)

	dep := managedClusterDeployer.DeepCopy()
	g.Expect(managedClusterClient.Create(context.TODO(), dep)).NotTo(gomega.HaveOccurred())

	// reconcile.Request
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))

	exp = eh.generalExplorer
	exp.(*generalExplorer).hubClient = hubClusterClient

	service := webService.DeepCopy()
	g.Expect(managedClusterClient.Create(context.TODO(), service)).NotTo(gomega.HaveOccurred())
	defer managedClusterClient.Delete(context.TODO(), service)

	sts := webSTS.DeepCopy()
	g.Expect(managedClusterClient.Create(context.TODO(), sts)).NotTo(gomega.HaveOccurred())
	defer managedClusterClient.Delete(context.TODO(), sts)

	app := wordpressApp.DeepCopy()
	g.Expect(managedClusterClient.Create(context.TODO(), app)).NotTo(gomega.HaveOccurred())
	defer managedClusterClient.Delete(context.TODO(), app)

	// wait on the create channel for request to come through on the hub
	<-hubClusterClient.(HubClient).createCh
	<-hubClusterClient.(HubClient).createCh
	// retrieve deployablelist on hub
	deployableList := &dplappv1alpha1.DeployableList{}
	g.Expect(hubClusterClient.List(context.TODO(), deployableList, &client.ListOptions{
		Namespace: clusterNamespaceOnHub,
	})).NotTo(gomega.HaveOccurred())
	g.Expect(deployableList.Items).To(gomega.HaveLen(2))
	kinds := [2]string{"Service", "StatefulSet"}

	// validate deployables on hub
	for _, deployable := range deployableList.Items {
		// spec containing the template
		deployableSpec := &dplappv1alpha1.DeployableSpec{}
		deployable.Spec.DeepCopyInto(deployableSpec)

		obj := &unstructured.Unstructured{}
		g.Expect(json.Unmarshal(deployableSpec.Template.Raw, obj)).NotTo(gomega.HaveOccurred())

		g.Expect(obj.GetKind()).To(gomega.BeElementOf(kinds))
		g.Expect(deployable.GetLabels()[appLabelSelector]).To(gomega.Equal(applicationName))
		g.Expect(deployable.GetAnnotations()["app.ibm.com/hybrid-discovered"]).To(gomega.Equal("true"))
	}

	// validate annotations created on app resources on managed cluster
	serviceResource := &v1.Service{}
	serviceOnManagedCluster := types.NamespacedName{
		Name:      webServiceName,
		Namespace: deployerNamespace,
	}

	g.Expect(managedClusterClient.Get(context.TODO(), serviceOnManagedCluster, serviceResource)).NotTo(gomega.HaveOccurred())
	g.Expect(serviceResource.GetAnnotations()["app.ibm.com/hybrid-discovered"]).To(gomega.Equal("true"))

	stsResource := &apps.StatefulSet{}
	stsOnManagedCluster := types.NamespacedName{
		Name:      webSTSName,
		Namespace: deployerNamespace,
	}

	g.Expect(managedClusterClient.Get(context.TODO(), stsOnManagedCluster, stsResource)).NotTo(gomega.HaveOccurred())
	g.Expect(stsResource.GetAnnotations()["app.ibm.com/hybrid-discovered"]).To(gomega.Equal("true"))

	// don't use defer for cleanup, as we weant to go through reconcile logic to make sure hubclient
	// cache stays consistent
	managedClusterClient.Delete(context.TODO(), dep)
	g.Eventually(requests, timeout).Should(gomega.Receive(gomega.Equal(expectedRequest)))

}
