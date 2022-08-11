/*
Copyright 2022 The Kubernetes Authors.

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

package shell

import (
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
)

const (
	// ProviderName is the cloud provider name for shell
	ProviderName = "shell"
)

var (
	// GPULabel is the label added to nodes with GPU resource.
	GPULabel = "nvidia.com/gpu"

	availableGPUTypes = map[string]struct{}{
		"nvidia-tesla-k80":  {},
		"nvidia-tesla-p100": {},
		"nvidia-tesla-v100": {},
	}
)

// ShellCloudProvider implements CloudProvider interface for kubemark
type ShellCloudProvider struct {
	nodeGroups      []*NodeGroup
	resourceLimiter *cloudprovider.ResourceLimiter
}

// Name returns name of the cloud provider.
func (shell *ShellCloudProvider) Name() string {
	return ProviderName
}

// GPULabel returns the label added to nodes with GPU resource.
func (shell *ShellCloudProvider) GPULabel() string {
	return GPULabel
}

// GetAvailableGPUTypes return all available GPU types cloud provider supports
func (shell *ShellCloudProvider) GetAvailableGPUTypes() map[string]struct{} {
	return availableGPUTypes
}

// NodeGroups returns all node groups configured for this cloud provider.
func (shell *ShellCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	result := make([]cloudprovider.NodeGroup, 0, len(shell.nodeGroups))
	for _, nodegroup := range shell.nodeGroups {
		result = append(result, nodegroup)
	}
	return result
}

// Pricing returns pricing model for this cloud provider or error if not available.
func (shell *ShellCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

// NodeGroupForNode returns the node group for the given node.
func (shell *ShellCloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	// Skip nodes that are not managed by Kubemark Cloud Provider.
	if !strings.HasPrefix(node.Spec.ProviderID, ProviderName) || node.Annotations == nil {
		return nil, nil
	}

	nodeGroupName, found := node.Annotations[NodeGroupNameKey]
	if !found {
		return nil, nil
	}

	for _, nodeGroup := range shell.nodeGroups {
		if nodeGroup.Name == nodeGroupName {
			return nodeGroup, nil
		}
	}
	return nil, nil
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
// Implementation optional.
func (shell *ShellCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	// TODO (@k82cn): get partition from slurm
	return []string{}, cloudprovider.ErrNotImplemented
}

// NewNodeGroup builds a theoretical node group based on the node definition provided.
func (shell *ShellCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string,
	taints []apiv1.Taint,
	extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetResourceLimiter returns struct containing limits (max, min) for resources (cores, memory etc.).
func (shell *ShellCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return shell.resourceLimiter, nil
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (shell *ShellCloudProvider) Refresh() error {
	return nil
}

// Cleanup cleans up all resources before the cloud provider is removed
func (shell *ShellCloudProvider) Cleanup() error {
	return nil
}
