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

package deployable

import (
	"reflect"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"

	appv1alpha1 "github.com/IBM/deployer-operator/pkg/apis/app/v1alpha1"
	"github.com/IBM/deployer-operator/pkg/utils"
	dplv1alpha1 "github.com/IBM/multicloud-operators-deployable/pkg/apis/app/v1alpha1"
	subv1alpha1 "github.com/IBM/multicloud-operators-subscription/pkg/apis/app/v1alpha1"
)

const (
	trueCondition       = "true"
	packageInfoLogLevel = 3
)

var (
	ignoreAnnotation = []string{
		dplv1alpha1.AnnotationHosting,
		subv1alpha1.AnnotationHosting,
	}
)

func SyncDeployable(obj interface{}, explorer *utils.Explorer) {
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

	dpl, err := locateDeployableForObject(metaobj, explorer)
	if err != nil {
		klog.Error("Failed to locate deployable ", metaobj.GetNamespace()+"/"+metaobj.GetName(), " with error: ", err)
		return
	}
	if dpl == nil {
		dpl = &dplv1alpha1.Deployable{}
		dpl.GenerateName = genDeployableGenerateName(metaobj)
		dpl.Namespace = explorer.Cluster.Namespace
	}

	if err = updateDeployableAndObject(dpl, metaobj, explorer); err != nil {
		klog.Error("Failed to update deployable :", metaobj.GetNamespace(), "/", metaobj.GetName()+" with error: ", err)
	}
}

func updateDeployableAndObject(dpl *dplv1alpha1.Deployable, metaobj metav1.Object, explorer *utils.Explorer) error {
	refreshedDpl := prepareDeployable(dpl, metaobj, explorer)

	var err error
	refreshedObject, err := patchObject(refreshedDpl, metaobj, explorer)
	prepareTemplate(refreshedObject)
	refreshedDpl.Spec.Template = &runtime.RawExtension{
		Object: refreshedObject,
	}

	if refreshedObject == nil {
		return nil
	}
	if err != nil {
		klog.Error("Failed to patch object with error: ", err)
		return err
	}

	if refreshedDpl.UID == "" {
		ucContent, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(refreshedDpl)
		uc := &unstructured.Unstructured{}
		uc.SetUnstructuredContent(ucContent)
		uc.SetGroupVersionKind(deployableGVK)
		_, err = explorer.DynamicHubClient.Resource(explorer.GVKGVRMap[deployableGVK]).Namespace(refreshedDpl.Namespace).Create(uc, metav1.CreateOptions{})
	} else if !reflect.DeepEqual(refreshedDpl, dpl) {
		// avoid expensive reconciliation logic if no changes in dpl structure
		ucContent, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(refreshedDpl)
		uc := &unstructured.Unstructured{}
		uc.SetUnstructuredContent(ucContent)
		uc.SetGroupVersionKind(deployableGVK)
		_, err = explorer.DynamicHubClient.Resource(explorer.GVKGVRMap[deployableGVK]).Namespace(refreshedDpl.Namespace).Update(uc, metav1.UpdateOptions{})
	}

	if err != nil {
		klog.Error("Failed to sync object deployable with error: ", err)
		return err
	}

	klog.V(packageInfoLogLevel).Info("Successfully synched deployable for object ", metaobj.GetNamespace()+"/"+metaobj.GetName())
	return nil
}

func patchObject(dpl *dplv1alpha1.Deployable, metaobj metav1.Object, explorer *utils.Explorer) (*unstructured.Unstructured, error) {

	rtobj := metaobj.(runtime.Object)
	klog.V(5).Info("Patching meta object:", metaobj, " covert to runtime object:", rtobj)

	objgvr := explorer.GVKGVRMap[rtobj.GetObjectKind().GroupVersionKind()]

	ucobj, err := explorer.DynamicMCClient.Resource(objgvr).Namespace(metaobj.GetNamespace()).Get(metaobj.GetName(), metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		klog.Error("Failed to patch managed cluster object with error:", err)
		return nil, err
	}

	annotations := ucobj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[subv1alpha1.AnnotationHosting] = "/"
	annotations[subv1alpha1.AnnotationSyncSource] = "subnsdpl-/"
	annotations[dplv1alpha1.AnnotationHosting] = types.NamespacedName{Namespace: dpl.GetNamespace(), Name: dpl.GetName()}.String()
	if _, ok := dpl.Annotations[appv1alpha1.AnnotationDiscovered]; ok {
		annotations[appv1alpha1.AnnotationDiscovered] = dpl.Annotations[appv1alpha1.AnnotationDiscovered]
	}

	ucobj.SetAnnotations(annotations)
	_, err = explorer.DynamicMCClient.Resource(objgvr).Namespace(metaobj.GetNamespace()).Update(ucobj, metav1.UpdateOptions{})

	return ucobj, err
}

func genDeployableGenerateName(obj metav1.Object) string {
	return obj.GetName() + "-"
}

func locateDeployableForObject(metaobj metav1.Object, explorer *utils.Explorer) (*dplv1alpha1.Deployable, error) {
	gvr := explorer.GVKGVRMap[deployableGVK]
	dpllist, err := explorer.DynamicHubClient.Resource(gvr).Namespace(explorer.Cluster.Namespace).List(metav1.ListOptions{})
	if err != nil {
		klog.Error("Failed to list deployable objects from hub cluster namespace with error:", err)
		return nil, err
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
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(dpl.Object, objdpl)
			if err != nil {
				klog.Error("Failed to convert unstructured to deployable for ", dpl.GetNamespace()+"/"+dpl.GetName())
				return nil, err
			}
			break
		}
	}

	return objdpl, nil
}

func locateObjectForDeployable(dpl metav1.Object, explorer *utils.Explorer) (*unstructured.Unstructured, error) {

	uc, err := runtime.DefaultUnstructuredConverter.ToUnstructured(dpl)
	if err != nil {
		klog.Error("Failed to convert object to unstructured with error:", err)
		return nil, err
	}

	kind, found, err := unstructured.NestedString(uc, "spec", "template", "kind")
	if !found || err != nil {
		klog.Error("Cannot get the wrapped object kind for deployable ", dpl.GetNamespace()+"/"+dpl.GetName())
		return nil, err
	}

	gv, found, err := unstructured.NestedString(uc, "spec", "template", "apiVersion")
	if !found || err != nil {
		klog.Error("Cannot get the wrapped object apiversion for deployable ", dpl.GetNamespace()+"/"+dpl.GetName())
		return nil, err
	}

	name, found, err := unstructured.NestedString(uc, "spec", "template", "metadata", "name")
	if !found || err != nil {
		klog.Error("Cannot get the wrapped object name for deployable ", dpl.GetNamespace()+"/"+dpl.GetName())
		return nil, err
	}

	namespace, found, err := unstructured.NestedString(uc, "spec", "template", "metadata", "namespace")
	if !found || err != nil {
		klog.Error("Cannot get the wrapped object namespace for deployable ", dpl.GetNamespace()+"/"+dpl.GetName())
		return nil, err
	}

	gvk := schema.GroupVersionKind{
		Group:   utils.StripVersion(gv),
		Version: utils.StripGroup(gv),
		Kind:    kind,
	}

	gvr := explorer.GVKGVRMap[gvk]
	if _, ok := explorer.GVKGVRMap[gvk]; !ok {
		klog.Error("Cannot get GVR for GVK ", gvk.String()+" for deployable "+dpl.GetNamespace()+"/"+dpl.GetName())
		return nil, err
	}
	obj, err := explorer.DynamicMCClient.Resource(gvr).Namespace(namespace).Get(name, metav1.GetOptions{})
	if obj == nil || err != nil {
		if errors.IsNotFound(err) {
			klog.Error("Cannot find the wrapped object for deployable ", dpl.GetNamespace()+"/"+dpl.GetName())
			return nil, nil
		}
		return nil, err
	}
	return obj, nil
}

var (
	obsoleteAnnotations = []string{
		"kubectl.kubernetes.io/last-applied-configuration",
	}
)

func prepareTemplate(metaobj metav1.Object) {
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

func prepareDeployable(deployable *dplv1alpha1.Deployable, metaobj metav1.Object, explorer *utils.Explorer) *dplv1alpha1.Deployable {
	dpl := deployable.DeepCopy()
	labels := dpl.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	for key, value := range metaobj.GetLabels() {
		labels[key] = value
	}

	dpl.SetLabels(labels)

	annotations := dpl.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	annotations[appv1alpha1.SourceObject] = types.NamespacedName{Namespace: metaobj.GetNamespace(), Name: metaobj.GetName()}.String()
	annotations[dplv1alpha1.AnnotationManagedCluster] = explorer.Cluster.String()
	annotations[dplv1alpha1.AnnotationLocal] = trueCondition
	annotations[appv1alpha1.AnnotationDiscovered] = trueCondition
	dpl.SetAnnotations(annotations)
	return dpl
}
