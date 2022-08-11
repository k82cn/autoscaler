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
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/config/dynamic"
	"k8s.io/klog/v2"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// NodeGroup implements NodeGroup interface.
type NodeGroup struct {
	Name    string
	minSize int
	maxSize int
}

func buildNodeGroup(value string) (*NodeGroup, error) {
	spec, err := dynamic.SpecFromString(value, true)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node group spec: %v", err)
	}

	nodeGroup := &NodeGroup{
		Name:    spec.Name,
		minSize: spec.MinSize,
		maxSize: spec.MaxSize,
	}

	return nodeGroup, nil
}

// Id returns nodegroup name.
func (nodeGroup *NodeGroup) Id() string {
	return nodeGroup.Name
}

// MinSize returns minimum size of the node group.
func (nodeGroup *NodeGroup) MinSize() int {
	return nodeGroup.minSize
}

// MaxSize returns maximum size of the node group.
func (nodeGroup *NodeGroup) MaxSize() int {
	return nodeGroup.maxSize
}

// Debug returns a debug string for the nodegroup.
func (nodeGroup *NodeGroup) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", nodeGroup.Id(), nodeGroup.MinSize(), nodeGroup.MaxSize())
}

// Nodes returns a list of all nodes that belong to this node group.
func (nodeGroup *NodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	instances := make([]cloudprovider.Instance, 0)

	cmd := exec.Command(ShellCmdPath+"getnodes", "-g", nodeGroup.Name)
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	nodes := strings.Split(string(output), "\n")

	for _, node := range nodes {
		instances = append(instances, cloudprovider.Instance{Id: "shell://" + node})
	}
	return instances, nil
}

// DeleteNodes deletes the specified nodes from the node group.
func (nodeGroup *NodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	// TODO(@k82cn): release the job
	var nns []string
	for _, n := range nodes {
		nns = append(nns, n.Name)
	}

	if len(nns) == 0 {
		return fmt.Errorf("node list is empty")
	}

	cmd := exec.Command(ShellCmdPath+"delnodes", "-l", strings.Join(nns, ","))
	output, err := cmd.Output()
	if err != nil {
		return err
	}

	klog.Infof("<%s> nodes was deleted from <%s>.",
		strings.TrimSpace(string(output)), nodeGroup.Name)

	return nil
}

// IncreaseSize increases NodeGroup size.
func (nodeGroup *NodeGroup) IncreaseSize(delta int) error {
	// TODO(@k82cn): submit a job
	cmd := exec.Command(ShellCmdPath+"addnode", "-n", strconv.Itoa(delta))
	output, err := cmd.Output()
	if err != nil {
		return err
	}

	klog.Infof("<%s> nodes was added to <%s>.",
		strings.TrimSpace(string(output)), nodeGroup.Name)

	return nil
}

// TargetSize returns the current TARGET size of the node group. It is possible that the
// number is different from the number of nodes registered in Kubernetes.
func (nodeGroup *NodeGroup) TargetSize() (int, error) {
	// TODO(@k82cn): get jobs from slurm, including pending jobs
	cmd := exec.Command(ShellCmdPath+"getnodes", "-g", nodeGroup.Name)
	output, err := cmd.Output()
	if err != nil {
		return -1, err
	}

	nodes := strings.Split(string(output), "\n")

	return len(nodes), nil
}

// DecreaseTargetSize decreases the target size of the node group. This function
// doesn't permit to delete any existing node and can be used only to reduce the
// request for new nodes that have not been yet fulfilled. Delta should be negative.
func (nodeGroup *NodeGroup) DecreaseTargetSize(delta int) error {
	// TODO(@k82cn): delete jobs, from pending to running jobs
	cmd := exec.Command(ShellCmdPath+"delnodes", "-n", strconv.Itoa(delta))
	output, err := cmd.Output()
	if err != nil {
		return err
	}

	klog.Infof("<%s> nodes was deleted from <%s>.",
		strings.TrimSpace(string(output)), nodeGroup.Name)

	return nil
}

// TemplateNodeInfo returns a node template for this node group.
func (nodeGroup *NodeGroup) TemplateNodeInfo() (*schedulerframework.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Exist checks if the node group really exists on the cloud provider side.
func (nodeGroup *NodeGroup) Exist() bool {
	return false
}

// Create creates the node group on the cloud provider side.
func (nodeGroup *NodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Delete deletes the node group on the cloud provider side.
func (nodeGroup *NodeGroup) Delete() error {
	return cloudprovider.ErrNotImplemented
}

// Autoprovisioned returns true if the node group is autoprovisioned.
func (nodeGroup *NodeGroup) Autoprovisioned() bool {
	return false
}

// GetOptions returns NodeGroupAutoscalingOptions that should be used for this particular
// NodeGroup. Returning a nil will result in using default options.
func (nodeGroup *NodeGroup) GetOptions(defaults config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	return nil, cloudprovider.ErrNotImplemented
}
