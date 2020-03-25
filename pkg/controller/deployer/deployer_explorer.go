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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

	sigappv1beta1 "github.com/kubernetes-sigs/application/pkg/apis/app/v1beta1"

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
		dplv1alpha1.AnnotationHosting,
		subv1alpha1.AnnotationHosting,
	}
)

var (
	applicationGVK = schema.GroupVersionKind{
		Group:   "app.k8s.io",
		Version: "v1beta1",
		Kind:    "Application",
	}
)

type generalExplorer struct {
	cluster          types.NamespacedName
	deployer         *appv1alpha1.Deployer
	hubClient        client.Client
	mcClient         dynamic.Interface
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

	e.mcClient, err = dynamic.NewForConfig(mcconfig)
	if err != nil {
		klog.Error("Failed to create dynamic client for explorer", err)
		return err
	}
	e.mcDynamicFactory = dynamicinformer.NewDynamicSharedInformerFactory(e.mcClient, resync)

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

	if e.deployer != nil || e.mcDynamicFactory == nil {
		return
	}
	// generic explorer
	e.stopCh = make(chan struct{})

	//handle Application.app.k8s.io
	if _, ok := e.gvkGVRMap[applicationGVK]; !ok {
		klog.Error("Failed to obtain gvr for application gvk:", applicationGVK.String())
		return
	}

	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(new interface{}) {
			e.syncDeployablesForApplication(new)
		},
		UpdateFunc: func(old, new interface{}) {
			e.syncDeployablesForApplication(new)
		},
		DeleteFunc: func(old interface{}) {
			e.removeDeployablesForApplication(old)
		},
	}

	e.mcDynamicFactory.ForResource(e.gvkGVRMap[applicationGVK]).Informer().AddEventHandler(handler)

	e.stopCh = make(chan struct{})
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

	klog.V(packageInfoLogLevel).Info("Successfully synched deployable for object ", metaobj.GetNamespace()+"/"+metaobj.GetName())
}

func (e *generalExplorer) syncDeployablesForApplication(obj interface{}) {
	metaobj, err := meta.Accessor(obj)
	if err != nil {
		klog.Error("Failed to access object metadata for sync with error: ", err)
		return
	}

	uc, err := runtime.DefaultUnstructuredConverter.ToUnstructured(metaobj)
	if err != nil {
		klog.Error("Failed to convert object to unstructured with error:", err)
		return
	}

	// convert obj to Application
	app := &sigappv1beta1.Application{}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(uc, app)

	if err != nil {
		klog.Error("Failed to convert unstructured to application with error: ", err)
		return
	}
	var appComponents map[metav1.GroupKind]*unstructured.UnstructuredList = make(map[metav1.GroupKind]*unstructured.UnstructuredList)

	for _, componentKind := range app.Spec.ComponentGroupKinds {
		klog.Info("Processing application GK ", componentKind.String())
		for gvk, gvr := range e.gvkGVRMap {

			if gvk.Kind == componentKind.Kind {
				// for v1 core group (which is the empty name group), application label selectors use v1 as group name
				if (gvk.Group == "" && gvk.Version == componentKind.Group) || (gvk.Group != "" && gvk.Group == componentKind.Group) {
					klog.V(packageInfoLogLevel).Info("Successfully found GVR ", gvr.String())

					var objlist *unstructured.UnstructuredList
					if _, ok := app.GetAnnotations()[appv1alpha1.AnnotationClusterScope]; ok {
						// retrieve all components, cluster wide
						objlist, err = e.mcClient.Resource(gvr).List(metav1.ListOptions{LabelSelector: labels.Set(app.Spec.Selector.MatchLabels).String()})
					} else {
						// retrieve only namespaced components
						objlist, err = e.mcClient.Resource(gvr).Namespace(metaobj.GetNamespace()).List(
							metav1.ListOptions{LabelSelector: labels.Set(app.Spec.Selector.MatchLabels).String()})
					}
					if err != nil {
						klog.Error("Failed to retrieve the list of components based on selector ")
						return
					}
					if len(objlist.Items) == 0 {
						// we still want to create the deployables for the resources we find on managed cluster ,
						// even though some kinds defined in the app may not have corresponding (satisfying the selector)
						// resources on managed cluster
						klog.Info("Could not find a managed cluster resource for application component with kind ", componentKind.String())
					}
					appComponents[componentKind] = objlist
					break
				}
			}
		}
	}

	// process the components on managed cluster and creates deployables on hub for them
	for _, objlist := range appComponents {
		for _, item := range objlist.Items {
			klog.Info("Processing object ", item.GetName(), " in namespace ", item.GetNamespace(), " with kind ", item.GetKind())
			e.syncDeployable(&item)
		}
	}
}

func (e *generalExplorer) removeDeployablesForApplication(obj interface{}) {
}

func (e *generalExplorer) patchObject(dpl *dplv1alpha1.Deployable, metaobj metav1.Object) error {

	rtobj := metaobj.(runtime.Object)
	klog.V(5).Info("Patching meta object:", metaobj, " covert to runtime object:", rtobj)

	objgvr := e.gvkGVRMap[rtobj.GetObjectKind().GroupVersionKind()]

	ucobj, err := e.mcClient.Resource(objgvr).Namespace(metaobj.GetNamespace()).Get(metaobj.GetName(), metav1.GetOptions{})
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
	annotations[appv1alpha1.AnnotationDiscovered] = trueCondition
	ucobj.SetAnnotations(annotations)

	_, err = e.mcClient.Resource(objgvr).Namespace(metaobj.GetNamespace()).Update(ucobj, metav1.UpdateOptions{})

	return err
}

func (e *generalExplorer) genDeployableGenerateName(obj metav1.Object) string {
	return obj.GetName() + "-"
}

func (e *generalExplorer) locateDeployableForObject(metaobj metav1.Object) *dplv1alpha1.Deployable {
	dpllist := &dplv1alpha1.DeployableList{}

	err := e.hubClient.List(context.TODO(), dpllist, &client.ListOptions{
		Namespace: e.cluster.Namespace,
	})
	if err != nil {
		klog.Error("Failed to list deployable objects from hub cluster namespace with error:", err)
		return nil
	}

	var objdpl *dplv1alpha1.Deployable

	for _, dpl := range dpllist.Items {
		annotations := dpl.GetAnnotations()
		if annotations == nil {
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

	for key, value := range metaobj.GetLabels() {
		labels[key] = value
	}

	deployable.SetLabels(labels)

	annotations := deployable.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[appv1alpha1.SourceObject] = types.NamespacedName{Namespace: metaobj.GetNamespace(), Name: metaobj.GetName()}.String()
	annotations[dplv1alpha1.AnnotationManagedCluster] = e.cluster.String()
	annotations[dplv1alpha1.AnnotationLocal] = trueCondition
	annotations[appv1alpha1.AnnotationDiscovered] = trueCondition
	deployable.SetAnnotations(annotations)
}

// ExplorerHandler defines the struct to handle all explorers
type ExplorerHandler struct {
	hubconfig       *rest.Config
	mcconfig        *rest.Config
	cluster         types.NamespacedName
	explorerMap     map[types.NamespacedName]Explorer
	generalExplorer Explorer
}

func (eh *ExplorerHandler) initExplorerHandler(hubconfig, mcconfig *rest.Config, cluster types.NamespacedName) {

	eh.hubconfig = hubconfig
	eh.mcconfig = mcconfig
	eh.cluster = cluster
	eh.explorerMap = make(map[types.NamespacedName]Explorer)

	eh.generalExplorer = eh.createGeneralExplorer()
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
	var explorer Explorer

	var ok bool

	if explorer, ok = eh.explorerMap[types.NamespacedName{Namespace: deployer.Namespace, Name: deployer.Name}]; ok {
		explorer.stop()
	} else {
		explorer = &generalExplorer{}
	}

	err := explorer.configure(deployer, eh.mcconfig, eh.hubconfig, eh.cluster)
	if err != nil {
		klog.Error("Failed to configure explorer for update with error: ", err)
		return
	}

	eh.explorerMap[types.NamespacedName{Name: deployer.Name, Namespace: deployer.Namespace}] = explorer
	explorer.start()
}

func (eh *ExplorerHandler) createGeneralExplorer() Explorer {
	var explorer Explorer = &generalExplorer{}
	err := explorer.configure(nil, eh.mcconfig, eh.hubconfig, eh.cluster)

	if err != nil {
		klog.Error("Failed to configure general explorer for create with error: ", err)
		return nil
	}

	explorer.start()

	return explorer
}
