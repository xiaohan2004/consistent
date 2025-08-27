package main

import (
	"fmt"

	"github.com/buraksezer/consistent"
)

// Member 扩展原有的Member接口，添加权重支持
type Member interface {
	consistent.Member
	Weight() int
}

// Consistent 带权重的一致性哈希，组合原有的实现
type Consistent struct {
	*consistent.Consistent
	weights     map[string]int
	totalWeight int
}

type Config consistent.Config;

// New 创建一个带权重的一致性哈希实例
func New(members []Member, config Config) *Consistent {
	// 将带权重的Member转换为原始Member列表
	var baseMembers []consistent.Member
	weights := make(map[string]int)
	totalWeight := 0

	for _, member := range members {
		weight := member.Weight()
		if weight <= 0 {
			weight = 1 // 默认权重为1
		}

		// 根据权重创建多个虚拟节点
		for i := 0; i < weight; i++ {
			virtualMember := &weightedMember{
				name:   member.String(),
				index:  i,
				weight: weight,
			}
			baseMembers = append(baseMembers, virtualMember)
		}

		weights[member.String()] = weight
		totalWeight += weight
	}

	// 创建原始的一致性哈希
	baseConsistent := consistent.New(baseMembers, consistent.Config(config))

	return &Consistent{
		Consistent:  baseConsistent,
		weights:     weights,
		totalWeight: totalWeight,
	}
}

// Add 添加一个带权重的成员（如果成员已存在，会覆盖添加）
func (c *Consistent) Add(member Member) {
	weight := member.Weight()
	if weight <= 0 {
		weight = 1
	}

	// 如果成员已存在，先移除
	if _, exists := c.weights[member.String()]; exists {
		c.Remove(member.String())
	}

	// 根据权重添加虚拟节点
	for i := 0; i < weight; i++ {
		virtualMember := &weightedMember{
			name:   member.String(),
			index:  i,
			weight: weight,
		}
		c.Consistent.Add(virtualMember)
	}

	c.weights[member.String()] = weight
	c.totalWeight += weight
}

// Remove 移除一个带权重的成员
func (c *Consistent) Remove(name string) {
	weight, exists := c.weights[name]
	if !exists {
		return
	}

	// 移除所有虚拟节点
	for i := 0; i < weight; i++ {
		virtualName := name
		if weight > 1 {
			virtualName = fmt.Sprintf("%s#%d", name, i)
		}
		c.Consistent.Remove(virtualName)
	}

	delete(c.weights, name)
	c.totalWeight -= weight
}

// LocateKey 定位key并返回原始成员名称（去除虚拟节点后缀）
func (c *Consistent) LocateKey(key []byte) consistent.Member {
	member := c.Consistent.LocateKey(key)
	if member == nil {
		return nil
	}

	// 如果是虚拟节点，返回原始成员
	if vm, ok := member.(*weightedMember); ok {
		return &originalMember{name: vm.name}
	}

	return member
}

// GetWeights 获取所有成员的权重信息
func (c *Consistent) GetWeights() map[string]int {
	result := make(map[string]int)
	for name, weight := range c.weights {
		result[name] = weight
	}
	return result
}

// GetTotalWeight 获取总权重
func (c *Consistent) GetTotalWeight() int {
	return c.totalWeight
}

// weightedMember 内部使用的带权重的成员实现
type weightedMember struct {
	name   string
	index  int
	weight int
}

func (w *weightedMember) String() string {
	if w.weight == 1 {
		return w.name
	}
	// 对于权重大于1的节点，创建虚拟节点名称
	return fmt.Sprintf("%s#%d", w.name, w.index)
}

// originalMember 用于返回原始成员名称
type originalMember struct {
	name string
}

func (o *originalMember) String() string {
	return o.name
}
