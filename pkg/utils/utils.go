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

package utils

import (
	appv1alpha1 "github.com/IBM/deployer-operator/pkg/apis/app/v1alpha1"
)

func IsInClusterDeployer(deployer *appv1alpha1.Deployer) bool {
	incluster := true

	annotations := deployer.GetAnnotations()
	if annotations != nil {
		if in, ok := annotations[appv1alpha1.DeployerInCluster]; ok && in == "false" {
			incluster = false
		}
	}

	return incluster
}

func SetInClusterDeployer(deployer *appv1alpha1.Deployer) {
	annotations := deployer.GetAnnotations()
	annotations[appv1alpha1.DeployerInCluster] = "true"
	deployer.SetAnnotations(annotations)
}

func SetRemoteDeployer(deployer *appv1alpha1.Deployer) {
	annotations := deployer.GetAnnotations()
	annotations[appv1alpha1.DeployerInCluster] = "false"
	deployer.SetAnnotations(annotations)
}
