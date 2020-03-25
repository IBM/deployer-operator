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
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dplv1alpha1 "github.com/IBM/multicloud-operators-deployable/pkg/apis/app/v1alpha1"
	subv1alpha1 "github.com/IBM/multicloud-operators-subscription/pkg/apis/app/v1alpha1"

	appv1alpha1 "github.com/IBM/deployer-operator/pkg/apis/app/v1alpha1"
)

var (
	resync            = 20 * time.Minute
	resourcePredicate = discovery.SupportsAllVerbs{Verbs: []string{"create", "update", "delete", "list", "watch"}}
)

var (
	ignoreAnnotation = []string{
		"app.ibm.com/hosting-hybriddeployable",
	}
)

type generalExplorer struct {
	cluster          types.NamespacedName
	deployer         *appv1alpha1.Deployer
	hubClient        client.Client
	mcDynamiCclient  dynamic.Interface
	mcDynamicFactory dynamicinformer.DynamicSharedInformerFactory
	gvkGVRMap        map[schema.GroupVersionKind]schema.GroupVersionResource
	stopCh           chan struct{}
}

// Explorer defines the interface to explore 1 deployer
type Explorer interface {
	configure(deployer *appv1alpha1.Deployer, localconfig, hubconfig *rest.Config, cluster types.NamespacedName) error
	start()
	stop()
}

func (e *generalExplorer) configure(deployer *appv1alpha1.Deployer, mcconfig, hubconfig *rest.Config, cluster types.NamespacedName) error {
	var err error

	e.deployer = deployer
	e.cluster = cluster

	e.hubClient, err = client.New(hubconfig, client.Options{})
	if err != nil {
		klog.Error("Failed to create hub client for explorer", err)
		return err
	}

	e.mcDynamiCclient, err = dynamic.NewForConfig(mcconfig)
	if err != nil {
		klog.Error("Failed to create dynamic client for explorer", err)
		return err
	}

	e.mcDynamicFactory = dynamicinformer.NewDynamicSharedInformerFactory(e.mcDynamiCclient, resync)

	resources, err := discovery.NewDiscoveryClientForConfigOrDie(mcconfig).ServerPreferredResources()
	if err != nil {
		klog.Error("Failed to discover all server resources, continuing with err:", err)
		return err
	}

	filteredResources := discovery.FilteredBy(resourcePredicate, resources)

	klog.V(packageDetailLogLevel).Info("Discovered resources: ", filteredResources)

	e.gvkGVRMap = make(map[schema.GroupVersionKind]schema.GroupVersionResource)

	for _, rl := range filteredResources {
		e.buildGVKGVRMap(rl)
	}

	return nil
}

func (e *generalExplorer) buildGVKGVRMap(rl *metav1.APIResourceList) {
	for _, res := range rl.APIResources {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			klog.V(packageDetailLogLevel).Info("Skipping ", rl.GroupVersion, " with error:", err)
			continue
		}

		gvk := schema.GroupVersionKind{
			Kind:    res.Kind,
			Group:   gv.Group,
			Version: gv.Version,
		}
		gvr := schema.GroupVersionResource{
			Group:    gv.Group,
			Version:  gv.Version,
			Resource: res.Name,
		}

		e.gvkGVRMap[gvk] = gvr
	}
}

// PoC implementation without work queue
func (e *generalExplorer) start() {
	e.stop()

	if e.deployer == nil || e.deployer.Spec.Discovery == nil || e.mcDynamicFactory == nil {
		return
	}

	e.stopCh = make(chan struct{})

	for _, gvr := range e.deployer.Spec.Discovery.GVRs {
		e.mcDynamicFactory.ForResource(schema.GroupVersionResource{
			Group:    gvr.Group,
			Version:  gvr.Version,
			Resource: gvr.Resource,
		}).Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(new interface{}) {
				e.syncDeployable(new)
			},
			UpdateFunc: func(old, new interface{}) {
				e.syncDeployable(new)
			},
			DeleteFunc: func(old interface{}) {
				e.removeDeployable(old)
			},
		})
	}

	e.mcDynamicFactory.Start(e.stopCh)
}

func (e *generalExplorer) stop() {
	if e.stopCh != nil {
		e.mcDynamicFactory.WaitForCacheSync(e.stopCh)
		close(e.stopCh)
	}

	e.stopCh = nil
}

func (e *generalExplorer) syncDeployable(obj interface{}) {
	metaobj, err := meta.Accessor(obj)
	if err != nil {
		klog.Error("Failed to access object metadata for sync with error: ", err)
		return
	}

	annotations := metaobj.GetAnnotations()
	if annotations != nil {
		matched := false
		for _, key := range ignoreAnnotation {
			if _, matched = annotations[key]; matched {
				break
			}
		}

		if matched {
			klog.Info("Ignore object:", metaobj.GetNamespace(), "/", metaobj.GetName())
			return
		}
	}

	dpl := e.locateDeployableForObject(metaobj)
	if dpl == nil {
		dpl = &dplv1alpha1.Deployable{}
		dpl.GenerateName = e.genDeployableGenerateName(metaobj)
		dpl.Namespace = e.cluster.Namespace
	}

	e.prepareDeployable(dpl, metaobj)
	e.prepareTemplate(metaobj)

	dpl.Spec.Template = &runtime.RawExtension{
		Object: metaobj.(runtime.Object),
	}

	if dpl.UID == "" {
		err = e.hubClient.Create(context.TODO(), dpl)
	} else {
		err = e.hubClient.Update(context.TODO(), dpl)
	}

	if err != nil {
		klog.Error("Failed to sync object deployable with error: ", err)
	}

	err = e.patchObject(dpl, metaobj)
	if err != nil {
		klog.Error("Failed to patch object with error: ", err)
	}

	klog.V(packageInfoLogLevel).Info("Successfully synced deployable for object ", metaobj.GetNamespace()+"/"+metaobj.GetName())
}

func (e *generalExplorer) patchObject(dpl *dplv1alpha1.Deployable, metaobj metav1.Object) error {
	rtobj := metaobj.(runtime.Object)
	klog.V(packageDetailLogLevel).Info("Patching meta object:", metaobj, " covert to runtime object:", rtobj)

	objgvr := e.gvkGVRMap[rtobj.GetObjectKind().GroupVersionKind()]

	ucobj, err := e.mcDynamiCclient.Resource(objgvr).Namespace(metaobj.GetNamespace()).Get(metaobj.GetName(), metav1.GetOptions{})
	if err != nil {
		klog.Error("Failed to patch managed cluster object with error:", err)
		return err
	}

	annotations := ucobj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[subv1alpha1.AnnotationHosting] = "/"
	annotations[subv1alpha1.AnnotationSyncSource] = "subnsdpl-/"
	annotations[dplv1alpha1.AnnotationHosting] = types.NamespacedName{Namespace: dpl.GetNamespace(), Name: dpl.GetName()}.String()
	ucobj.SetAnnotations(annotations)

	_, err = e.mcDynamiCclient.Resource(objgvr).Namespace(metaobj.GetNamespace()).Update(ucobj, metav1.UpdateOptions{})

	return err
}

func (e *generalExplorer) removeDeployable(obj interface{}) {
	metaobj, err := meta.Accessor(obj)
	if err != nil {
		klog.Error("Failed to access object metadata for removal with error: ", err)
		return
	}

	dpl := e.locateDeployableForObject(metaobj)
	if dpl != nil {
		err = e.hubClient.Delete(context.TODO(), dpl)
	}

	if err != nil {
		klog.Error("Failed to delete object deployable with error:", err)
	}

	klog.V(packageInfoLogLevel).Info("Successfully deleted deployable for object ", metaobj.GetNamespace()+"/"+metaobj.GetName())
}

func (e *generalExplorer) genDeployableGenerateName(obj metav1.Object) string {
	return e.deployer.Spec.Type + "-" + obj.GetNamespace() + "-" + obj.GetName() + "-"
}

func (e *generalExplorer) locateDeployableForObject(metaobj metav1.Object) *dplv1alpha1.Deployable {
	labelmap := map[string]string{
		appv1alpha1.HostingDeployer: e.deployer.Name,
	}
	dpllist := &dplv1alpha1.DeployableList{}

	err := e.hubClient.List(context.TODO(), dpllist, &client.ListOptions{
		Namespace:     e.cluster.Namespace,
		LabelSelector: labels.Set(labelmap).AsSelector(),
	})
	if err != nil {
		klog.Error("Failed to list deployable objects from hub cluster namespace with error:", err)
		return nil
	}

	hdply := types.NamespacedName{Namespace: e.deployer.Namespace, Name: e.deployer.Name}.String()

	var objdpl *dplv1alpha1.Deployable

	for _, dpl := range dpllist.Items {
		annotations := dpl.GetAnnotations()
		if annotations == nil {
			continue
		}

		dply, ok := annotations[appv1alpha1.HostingDeployer]
		if !ok || dply != hdply {
			continue
		}

		key := types.NamespacedName{
			Namespace: metaobj.GetNamespace(),
			Name:      metaobj.GetName(),
		}.String()

		srcobj, ok := annotations[appv1alpha1.SourceObject]
		if ok && srcobj == key {
			objdpl = &dpl
			break
		}
	}

	return objdpl
}

var (
	obsoleteAnnotations = []string{
		"kubectl.kubernetes.io/last-applied-configuration",
	}
)

func (e *generalExplorer) prepareTemplate(metaobj metav1.Object) {
	var emptyuid types.UID

	metaobj.SetUID(emptyuid)
	metaobj.SetSelfLink("")
	metaobj.SetResourceVersion("")
	metaobj.SetGeneration(0)
	metaobj.SetCreationTimestamp(metav1.Time{})

	annotations := metaobj.GetAnnotations()
	if annotations != nil {
		for _, k := range obsoleteAnnotations {
			delete(annotations, k)
		}

		metaobj.SetAnnotations(annotations)
	}
}

func (e *generalExplorer) prepareDeployable(deployable *dplv1alpha1.Deployable, metaobj metav1.Object) {
	labels := deployable.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[appv1alpha1.HostingDeployer] = e.deployer.Name
	deployable.SetLabels(labels)

	annotations := deployable.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[appv1alpha1.HostingDeployer] = types.NamespacedName{Namespace: e.deployer.Namespace, Name: e.deployer.Name}.String()
	annotations[appv1alpha1.DeployerType] = e.deployer.Spec.Type
	annotations[appv1alpha1.SourceObject] = types.NamespacedName{Namespace: metaobj.GetNamespace(), Name: metaobj.GetName()}.String()
	annotations[dplv1alpha1.AnnotationManagedCluster] = e.cluster.String()
	annotations[dplv1alpha1.AnnotationLocal] = "true"
	deployable.SetAnnotations(annotations)
}

// ExplorerHandler defines the struct to handle all explorers
type ExplorerHandler struct {
	hubconfig   *rest.Config
	mcconfig    *rest.Config
	cluster     types.NamespacedName
	explorerMap map[types.NamespacedName]Explorer
}

func (eh *ExplorerHandler) initExplorerHandler(hubconfig, mcconfig *rest.Config, cluster types.NamespacedName) {
	eh.hubconfig = hubconfig
	eh.mcconfig = mcconfig
	eh.cluster = cluster
	eh.explorerMap = make(map[types.NamespacedName]Explorer)
}

func (eh *ExplorerHandler) removeExplorer(key types.NamespacedName) {
	klog.V(packageInfoLogLevel).Info("deleting explorer ", key, " from map ", eh.explorerMap)

	if eh.explorerMap != nil {
		if explorer, ok := eh.explorerMap[key]; ok {
			explorer.stop()
			klog.V(packageDetailLogLevel).Info("stopped explorer ", explorer)
		}

		delete(eh.explorerMap, key)
	}
}

func (eh *ExplorerHandler) updateExplorerForDeployer(deployer *appv1alpha1.Deployer) {
	if deployer.Spec.Discovery == nil || deployer.Spec.Discovery.GVRs == nil {
		eh.removeExplorer(types.NamespacedName{Name: deployer.Name, Namespace: deployer.Namespace})
		return
	}

	var explorer Explorer

	var ok bool

	if explorer, ok = eh.explorerMap[types.NamespacedName{Namespace: deployer.Namespace, Name: deployer.Name}]; ok {
		explorer.stop()
	} else {
		explorer = &generalExplorer{}
	}

	err := explorer.configure(deployer, eh.hubconfig, eh.mcconfig, eh.cluster)
	if err != nil {
		klog.Error("Failed to configure explorer with error: ", err)
	}

	eh.explorerMap[types.NamespacedName{Name: deployer.Name, Namespace: deployer.Namespace}] = explorer
	explorer.start()
}
