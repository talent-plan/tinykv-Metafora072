package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// HandleRaftReady 处理 rawNode 传递来的 Ready,对这些 entries 进行 apply
// 每执行完一次 apply，都需要对 proposals 中的相应 Index 的 proposal 进行 callback 回应（调用 cb.Done()),然后从中删除这个 proposal
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).

	// 判断是否 Ready
	if d.RaftGroup.HasReady() == false {
		return
	}

	ready := d.RaftGroup.Ready()

	// 调用 SaveReadyState 将 Ready 中需要持久化的内容保存到 badger
	applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		log.Panic(err)
	}

	if applySnapResult != nil { // 存在快照，则应用这个快照
		if reflect.DeepEqual(applySnapResult.Region,applySnapResult.PrevRegion) == false {
			d.peerStorage.SetRegion(applySnapResult.Region)

			storeMeta := d.ctx.storeMeta

			storeMeta.Lock()
			storeMeta.regions[applySnapResult.Region.Id] = applySnapResult.Region
			storeMeta.regionRanges.Delete(&regionItem{
				region: applySnapResult.PrevRegion,
			})
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{
				region: applySnapResult.Region,
			})
			storeMeta.Unlock()
		}
	}

	// 调用 d.Send() 方法将 Ready 中的 Msg 发送出去
	d.Send(d.ctx.trans, ready.Messages)

	// 处理待 apply 的日志
	if len(ready.CommittedEntries) > 0 {
		entries := ready.CommittedEntries

		writeBatch := &engine_util.WriteBatch{}
		for _, entry := range entries {
			writeBatch = d.processCommittedEntry(&entry,writeBatch)
			// 节点有可能在 processCommittedEntry 返回之后就销毁了,如果销毁了需要直接返回，保证对这个节点而言不会再 DB 中写入数据
			if d.stopped {
				return
			}
		}

		/*
		type RaftApplyState struct {
			// Record the applied index of the state machine to make sure
			// not apply any index twice after restart.
			AppliedIndex uint64 `protobuf:"varint,1,opt,name=applied_index,json=appliedIndex,proto3" json:"applied_index,omitempty"`
			// Record the index and term of the last raft log that have been truncated. (Used in 2C)
			TruncatedState       *RaftTruncatedState `protobuf:"bytes,2,opt,name=truncated_state,json=truncatedState" json:"truncated_state,omitempty"`
		}
		 */
		// 更新 peer_storage.applyState
		d.peerStorage.applyState.AppliedIndex = entries[len(entries)-1].Index

		// ??
		err := writeBatch.SetMeta(meta.ApplyStateKey(d.regionId),d.peerStorage.applyState)
		if err != nil {
			log.Panic(err)
		}

		// 一次性执行所有的 Command 操作和 ApplyState 更新操作
		writeBatch.MustWriteToDB(d.peerStorage.Engines.Kv)

	}

	// 调用 d.RaftGroup.Advance() 推进 RawNode,更新 raft 状态
	d.RaftGroup.Advance(ready)
}


func (d *peerMsgHandler) processCommittedEntry(entry *pb.Entry,writeBatch *engine_util.WriteBatch) *engine_util.WriteBatch {
	//fmt.Println("processCommittedEntry called!")
	//EntryType_EntryNormal (值为 0): 这种类型表示普通的日志条目，通常用于存储客户端的请求或命令。这些命令会被应用到状态机中，以保持集群的一致性。
	//EntryType_EntryConfChange (值为 1): 这种类型表示配置变更日志条目，用于集群配置的更改，例如添加或删除节点。当这种类型的条目被提交时，Raft 节点会应用配置变更，以更新集群的成员信息。

	// Project3 TODO
	//if entry.EntryType == pb.EntryType_EntryConfChange { // 日志条目是配置变更条目
	//	confChange := &pb.ConfChange{}
	//
	//	err := confChange.Unmarshal(entry.Data)
	//	if err != nil {
	//		log.Panic(err)
	//	}
	//	log.Infof("EntryType_EntryConfChange")
	//
	//	return d.processConfChange(entry,confChange,writeBatch)
	//}

	// 反序列化 entry.Data 中的数据
	request := &raft_cmdpb.RaftCmdRequest{}

	err := request.Unmarshal(entry.Data)
	if err != nil {
		log.Panic(err)
	}

	if request.AdminRequest != nil {
		return d.processAdminRequest(entry, request, writeBatch)
	} else {
		return d.processRequest(entry, request, writeBatch)
	}


}



// processConfChange 处理配置变更类型的日志条目
func (d *peerMsgHandler) processConfChange(entry *pb.Entry,confChange *pb.ConfChange,writeBatch *engine_util.WriteBatch) *engine_util.WriteBatch {
	// 反序列化 entry.Data 中的数据
	request := &raft_cmdpb.RaftCmdRequest{}
	err := request.Unmarshal(entry.Data)
	if err != nil {
		log.Panic(err)
	}
	// ------------------------------------------------------------------------------- ?
	// 检查 Command Request 中的 RegionEpoch 是否是过期的，以此判定是不是一个重复的请求
	// 实验指导书中提到，测试程序可能会多次提交同一个 ConfChange 直到 ConfChange 被应用
	// CheckRegionEpoch 检查 RaftCmdRequest 头部携带的 RegionEpoch 是不是和 currentRegionEpoch 匹配
	if err, ok := util.CheckRegionEpoch(request, d.Region(), true).(*util.ErrEpochNotMatch); ok {
		log.Infof("[processConfChange] %v RegionEpoch not match", d.PeerId())
		d.processProposal(entry, ErrResp(err))
		return writeBatch
	}
	// ---------------------------------------------------------------------------------

	/*
	type ConfChangeType int32
	const (
		ConfChangeType_AddNode    ConfChangeType = 0
		ConfChangeType_RemoveNode ConfChangeType = 1
	)
	ConfChangeType_AddNode (值为 0): 表示添加节点操作，用于将一个新节点添加到 Raft 集群中。这通常用于扩展集群的规模，提高系统的容错能力和可用性。
	ConfChangeType_RemoveNode (值为 1): 表示移除节点操作，用于从 Raft 集群中删除一个节点。这通常用于缩减集群规模或移除故障节点，以保持集群的健康状态.
	 */
	switch confChange.ChangeType {
	case pb.ConfChangeType_AddNode: // 添加节点操作，用于将一个新节点添加到 Raft 集群中。
		log.Infof("[AddNode] %v add %v", d.PeerId(), confChange.NodeId)
		// 待添加的节点必须原先在 Region 中不存在
		if d.getPeerIndex(confChange.NodeId) == uint64(len(d.Region().Peers)) { // 不存在
			d.Region().Peers = append(d.Region().Peers,)
		}
	case pb.ConfChangeType_RemoveNode: // 移除节点操作，用于从 Raft 集群中删除一个节点。
		// TODO
	}

	return writeBatch

}

// getPeerIndex 根据需要添加或者删除的 Peer id，找到 region 中是否已经存在这个 Peer,不存在则返回Peer数组的len
func (d *peerMsgHandler) getPeerIndex(nodeId uint64) uint64 {
	for idx,peer := range d.peerStorage.region.Peers {
		if peer.GetId() == nodeId {
			return uint64(idx)
		}
	}
	return uint64(len(d.peerStorage.region.Peers))
}

// processAdminRequest 处理 commit 的 Admin Request 类型 command
func (d *peerMsgHandler) processAdminRequest(entry *pb.Entry,request *raft_cmdpb.RaftCmdRequest,writeBatch *engine_util.WriteBatch) *engine_util.WriteBatch {
	switch request.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		break
	case raft_cmdpb.AdminCmdType_CompactLog: // CompactLog 类型请求不需要将执行结果存储到 proposal 回调????
		if request.AdminRequest.CompactLog.CompactIndex > d.peerStorage.applyState.TruncatedState.Index {
			d.peerStorage.applyState.TruncatedState.Index = request.AdminRequest.CompactLog.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = request.AdminRequest.CompactLog.CompactTerm
			// 调度日志截断任务
			d.ScheduleCompactLog(request.AdminRequest.CompactLog.CompactIndex)
		}
	// TODO other cases: AdminCmdType_ChangePeer、AdminCmdType_TransferLeader、AdminCmdType_Split
	}

	return writeBatch
}
// processRequest 处理 commit 的 Put/Get/Delete/Snap 类型 command
func (d *peerMsgHandler) processRequest(entry *pb.Entry,request *raft_cmdpb.RaftCmdRequest,writeBatch *engine_util.WriteBatch) *engine_util.WriteBatch {
	raftCmdResponse := &raft_cmdpb.RaftCmdResponse{
		Header:		&raft_cmdpb.RaftResponseHeader{},
		Responses:  make([]*raft_cmdpb.Response, 0),
	}

	// **`CmdType_Invalid` (值为 0)**: 表示无效的命令类型，通常用于初始化或错误处理。
	// **`CmdType_Get` (值为 1)**: 表示获取操作，用于从存储中读取数据。
	// **`CmdType_Put` (值为 3)**: 表示存储操作，用于将数据写入存储。
	// **`CmdType_Delete` (值为 4)**: 表示删除操作，用于从存储中删除数据。
	// **`CmdType_Snap` (值为 5)**: 表示快照操作，用于创建存储的快照，以便进行备份或恢复。
	for _, curRequest := range request.Requests {
		switch curRequest.CmdType {
		case raft_cmdpb.CmdType_Invalid:
			continue
		case raft_cmdpb.CmdType_Get: // 表示获取操作，用于从存储中读取数据。
			err := util.CheckKeyInRegion(curRequest.Get.Key,d.Region())
			if err != nil {
				//These errors are mainly related to Region. So it is also a member of RaftResponseHeader of RaftCmdResponse.
				//When proposing a request or applying a command, there may be some errors.
				//If that, you should return the raft command response with the error, then the error will be further passed to gRPC response.
				//You can use BindRespError provided in kv/raftstore/cmd_resp.go to convert these errors to errors defined in errorpb.proto when returning the response with an error.
				BindRespError(raftCmdResponse, err)
				continue
			}

			// Get 和 Snap 请求需要先将之前的结果写到 DB ??
			writeBatch.MustWriteToDB(d.peerStorage.Engines.Kv)

			writeBatch = &engine_util.WriteBatch{}

			value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv,curRequest.Get.Cf,curRequest.Get.Key)
			/*
			type GetResponse struct {
				Value                []byte   `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
				XXX_NoUnkeyedLiteral struct{} `json:"-"`
				XXX_unrecognized     []byte   `json:"-"`
				XXX_sizecache        int32    `json:"-"`
			}
			 */
			raftCmdResponse.Responses = append(raftCmdResponse.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:	 &raft_cmdpb.GetResponse{Value: value},
			})
		case raft_cmdpb.CmdType_Put: // 表示存储操作，用于将数据写入存储。
			err := util.CheckKeyInRegion(curRequest.Put.Key,d.Region())
			if err != nil {
				//These errors are mainly related to Region. So it is also a member of RaftResponseHeader of RaftCmdResponse.
				//When proposing a request or applying a command, there may be some errors.
				//If that, you should return the raft command response with the error, then the error will be further passed to gRPC response.
				//You can use BindRespError provided in kv/raftstore/cmd_resp.go to convert these errors to errors defined in errorpb.proto when returning the response with an error.
				BindRespError(raftCmdResponse, err)
				continue
			}

			writeBatch.SetCF(curRequest.Put.Cf,curRequest.Put.Key,curRequest.Put.Value)
			/*
			type PutResponse struct {
				XXX_NoUnkeyedLiteral struct{} `json:"-"`
				XXX_unrecognized     []byte   `json:"-"`
				XXX_sizecache        int32    `json:"-"`
			}
			 */
			raftCmdResponse.Responses = append(raftCmdResponse.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:	 &raft_cmdpb.PutResponse{},
			})
		case raft_cmdpb.CmdType_Delete: // 表示删除操作，用于从存储中删除数据。
			err := util.CheckKeyInRegion(curRequest.Delete.Key,d.Region())
			if err != nil {
				//These errors are mainly related to Region. So it is also a member of RaftResponseHeader of RaftCmdResponse.
				//When proposing a request or applying a command, there may be some errors.
				//If that, you should return the raft command response with the error, then the error will be further passed to gRPC response.
				//You can use BindRespError provided in kv/raftstore/cmd_resp.go to convert these errors to errors defined in errorpb.proto when returning the response with an error.
				BindRespError(raftCmdResponse, err)
				continue
			}
			writeBatch.DeleteCF(curRequest.Delete.Cf,curRequest.Delete.Key)
			/*
			type DeleteResponse struct {
				XXX_NoUnkeyedLiteral struct{} `json:"-"`
				XXX_unrecognized     []byte   `json:"-"`
				XXX_sizecache        int32    `json:"-"`
			}
			 */
			raftCmdResponse.Responses = append(raftCmdResponse.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})
		case raft_cmdpb.CmdType_Snap: // 表示快照操作，用于创建存储的快照，以便进行备份或恢复。
			if request.Header.GetRegionEpoch().GetVersion() != d.Region().GetRegionEpoch().GetVersion() {
				err := &util.ErrEpochNotMatch{}
				BindRespError(raftCmdResponse,err)
				continue
			}
			// Get 和 Snap 请求需要先将结果写到 DB，否则的话如果有多个 entry 同时被 apply，客户端无法及时看到写入的结果 ??
			writeBatch.MustWriteToDB(d.peerStorage.Engines.Kv)
			writeBatch = &engine_util.WriteBatch{}
			/*
			type SnapResponse struct {
				Region               *metapb.Region `protobuf:"bytes,1,opt,name=region" json:"region,omitempty"`
				XXX_NoUnkeyedLiteral struct{}       `json:"-"`
				XXX_unrecognized     []byte         `json:"-"`
				XXX_sizecache        int32          `json:"-"`
			}
			 */
			raftCmdResponse.Responses = append(raftCmdResponse.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
			})
		}
	}

	d.processProposal(entry, raftCmdResponse)
	return writeBatch
}


// processProposal 找到等待 entry 的回调（proposal），存入操作的执行结果（raftCmdResponse）
func (d *peerMsgHandler) processProposal(entry *pb.Entry, raftCmdResponse *raft_cmdpb.RaftCmdResponse) {
	for len(d.proposals) > 0 {
		curProposal := d.proposals[0]
		if curProposal.term < entry.Term || curProposal.index < entry.Index {
			NotifyStaleReq(curProposal.term,curProposal.cb)
			d.proposals = d.proposals[1:]
			continue
		}

		if curProposal.term == entry.Term && curProposal.index == entry.Index {
			if curProposal.cb != nil {
				curProposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
			curProposal.cb.Done(raftCmdResponse)
			d.proposals = d.proposals[1:]

		}

		return
	}
	//plen := len(d.proposals)
	//for idx := 0; idx < plen; idx++ {
	//	curProposal := d.proposals[idx]
	//	if curProposal.term == entry.Term && curProposal.index == entry.Index {
	//		if curProposal.cb != nil {
	//			curProposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	//		}
	//		curProposal.cb.Done(raftCmdResponse)
	//		return
	//	}
	//}
}


func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}




//将 client 的请求包装成 entry 传递给 raft 层
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.Requests != nil { // 普通请求
		// 封装回调函数 callback
		curProposal := &proposal{
			index: d.RaftGroup.Raft.RaftLog.LastIndex() + 1,
			term:  d.RaftGroup.Raft.Term,
			cb:	   cb,
		}

		d.proposals = append(d.proposals,curProposal)

		// 将 RaftCmdRequest 序列化为字节流
		marshalRes, err := msg.Marshal()
		if err != nil {
			log.Panic(err)
		}

		// 将序列化的字节流包装成 entry 传递给 raft 层的 MessageType_MsgPropose
		err = d.RaftGroup.Propose(marshalRes)
		if err != nil {
			log.Panic(err)
		}
	} else if msg.AdminRequest != nil { // 管理员请求
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog: // 日志压缩需要提交到 raft 同步
			marshalRes, err := msg.Marshal()
			if err != nil {
				log.Panic(err)
			}
			err = d.RaftGroup.Propose(marshalRes)
			if err != nil {
				log.Panic(err)
			}
		// TODO other cases
		}
	}

}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}