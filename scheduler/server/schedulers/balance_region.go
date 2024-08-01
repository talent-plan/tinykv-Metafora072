// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type suitableStoreSlice []*core.StoreInfo
func (s suitableStoreSlice) Len() int { return len(s) }
func (s suitableStoreSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i]}
func (s suitableStoreSlice) Less(i, j int) bool { return s[i].GetRegionSize() < s[j].GetRegionSize() }

// Schedule 实现 region 调度，用来让集群中的 stores 所负载的 region 趋于平衡，避免一个 store 中含有很多 region 而另一个 store 中含有很少 region 的情况。
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	// 选出 DownTime() < MaxStoreDownTime 的 store 作为 suitableStores，并按照 regionSize 降序排列；
	suitableStores := make(suitableStoreSlice, 0)
	for _, store := range cluster.GetStores() {
		// 根据参考文档
		// In short, a suitable store should be up and the down time cannot be longer than MaxStoreDownTime of the cluster,
		// which you can get through cluster.GetMaxStoreDownTime().
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}

	// 获取 regionSize 最大的 suitableStore，作为源 store，
	// 然后依次调用 GetPendingRegionsWithLock()、GetFollowersWithLock()、GetLeadersWithLock()，
	// 如果找到了一个待转移 region，执行下面的步骤，否则尝试下一个 suitableStore；
	sort.Sort(suitableStores)
	var fromStore, toStore *core.StoreInfo
	var selectedRegion *core.RegionInfo
	for i := len(suitableStores) - 1; i >= 0; i-- {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		selectedRegion = regions.RandomRegion(nil, nil)
		if selectedRegion != nil {
			fromStore = suitableStores[i]
			break
		}
		cluster.GetFollowersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		selectedRegion = regions.RandomRegion(nil, nil)
		if selectedRegion != nil {
			fromStore = suitableStores[i]
			break
		}
		cluster.GetLeadersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		selectedRegion = regions.RandomRegion(nil, nil)
		if selectedRegion != nil {
			fromStore = suitableStores[i]
			break
		}
	}
	if selectedRegion == nil {
		return nil
	}

	// 判断待转移 region 的 store 数量，如果小于 cluster.GetMaxReplicas()，放弃转移；
	if len(selectedRegion.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}

	// 取出 regionSize 最小的 suitableStore 作为目标 store (toStore)，并且该 store 不能在待转移 region 中，如果在，尝试次小的 suitableStore，以此类推；
	for i := 0 ; i < len(suitableStores); i++ {
		_, ok := selectedRegion.GetStoreIds()[suitableStores[i].GetID()]
		if ok == false {
			toStore = suitableStores[i]
			break
		}
	}
	if toStore == nil {
		return nil
	}

	// 判断两 store 的 regionSize 差别是否过小，如果是小于2*ApproximateSize，放弃转移。
	// 因为如果此时接着转移，很有可能过不了久就重新转了回来；
	if fromStore.GetRegionSize() - toStore.GetRegionSize() < 2 * selectedRegion.GetApproximateSize() {
		log.Infof("fromStore.GetRegionSize() - toStore.GetRegionSize() < 2 * selectedRegion.GetApproximateSize()")
		return nil
	}

	// 在目标 store 上创建一个 peer，然后调用 CreateMovePeerOperator 生成转移请求；
	// 根据参考文档，使用 scheduler/server/schedule/operator 包中的CreateMovePeerOperator 函数来创建一个 MovePeer 操作。
	newPeer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		log.Panic(err)
	}
	moveInfo := fmt.Sprintf("move-from-%d-to-%d", fromStore.GetID(), toStore.GetID())
	operatorResult,err := operator.CreateMovePeerOperator(moveInfo,cluster,selectedRegion,operator.OpBalance,fromStore.GetID(),toStore.GetID(),newPeer.GetId())
	if err != nil {
		log.Panic(err)
	}
	return operatorResult
}
