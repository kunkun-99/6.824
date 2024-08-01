package kvraft

import (
	"bytes"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandType CommandType
	Key         string
	Value       string
	ClientId    int
	SeqId       int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientReply map[int64]CommandReply
	replyCh     map[int64]chan CommandReply
	kvData      map[string]string
	lastApplied int
}

type CommandReply struct {
	SeqId int
	Err   Err
	Value string
	Term  int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if commandReply, ok := kv.clientReply[int64(args.ClientId)]; ok {
		if args.SeqId <= commandReply.SeqId {
			reply.Err = commandReply.Err
			reply.Value = commandReply.Value
			kv.mu.Unlock()
			return
		}
	}
	op := Op{
		CommandType: GetMethod,
		Key:         args.Key,
		ClientId:    args.ClientId,
		SeqId:       args.SeqId,
	}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("KVServer %d Get start,ClientId:%d,SeqId:%dIndex:%d\n", kv.me, args.ClientId, args.SeqId, index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("Server %d Error wrong leader\n", kv.me)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	replyCh := kv.getIndexReplyCh(index)
	select {
	case replyMsg := <-replyCh:
		if replyMsg.Term == term {
			DPrintf("replyMsg.Seq:%d,Err:%s,Term:%d,index:%d", replyMsg.SeqId, replyMsg.Err, replyMsg.Term, index)
			reply.Value = replyMsg.Value
			reply.Err = replyMsg.Err
		} else {
			DPrintf("Server %d Error wrong leader because of term\n", kv.me)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("KVServer %d time out!\n", kv.me)
		reply.Err = ErrTimeOut
	}
	kv.mu.Lock()
	_, ok := kv.replyCh[int64(index)]
	if ok {
		delete(kv.replyCh, int64(index))
		DPrintf("index %d chan closed!\n", index)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if commandReply, ok := kv.clientReply[int64(args.ClientId)]; ok {
		if args.SeqId == commandReply.SeqId {
			reply.Err = commandReply.Err
			kv.mu.Unlock()
			return
		}
	}
	op := Op{
		CommandType: CommandType(args.Op),
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SeqId:       args.SeqId,
	}
	index, term, isLeader := kv.rf.Start(op)
	DPrintf("KVServer %d PutAppend start,ClientId:%d,SeqId:%dIndex:%d\n", kv.me, args.ClientId, args.SeqId, index)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("Server %d Error wrong leader\n", kv.me)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	replyCh := kv.getIndexReplyCh(index)
	select {
	case replyMsg := <-replyCh:
		if replyMsg.Term == term {
			DPrintf("replyMsg.Err:%s,Term:%d,index:%d", replyMsg.Err, replyMsg.Term, index)
			reply.Err = replyMsg.Err
		} else {
			DPrintf("Server %d Error wrong leader because of term\n", kv.me)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("KVServer %d time out!\n", kv.me)
		reply.Err = ErrTimeOut
	}
	kv.mu.Lock()
	_, ok := kv.replyCh[int64(index)]
	if ok {
		delete(kv.replyCh, int64(index))
		DPrintf("index %d chan closed!\n", index)
	}
	kv.mu.Unlock()

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ReceiveApplyMsg() {
	for kv.killed() == false {
		select {
		case applyMsg := <-kv.applyCh:
			index := applyMsg.CommandIndex
			DPrintf("kv %d ReceiveApplyMsg,index:%d", kv.me, index)
			if applyMsg.CommandValid {
				command := applyMsg.Command.(Op)
				commandReply := CommandReply{}
				kv.mu.Lock()
				// replyCh, ok := kv.replyCh[int64(index)]
				if commandContext, ok := kv.clientReply[int64(command.ClientId)]; ok && command.SeqId <= commandContext.SeqId {
					DPrintf("包含在map里了,SeqId:%d,index:%d\n", commandContext.SeqId, index)
					kv.mu.Unlock()
					continue
				}
				if command.CommandType == GetMethod {
					if value, ok := kv.kvData[command.Key]; ok {
						commandReply.Err = OK
						commandReply.Term = applyMsg.CommandTerm
						commandReply.Value = value
					} else {
						commandReply.Err = ErrNoKey
						commandReply.Term = applyMsg.CommandTerm
					}
				} else if command.CommandType == PutMethod {
					commandReply.Err = OK
					commandReply.Term = applyMsg.CommandTerm
					kv.kvData[command.Key] = command.Value
				} else {
					commandReply.Err = OK
					commandReply.Term = applyMsg.CommandTerm
					if _, ok := kv.kvData[command.Key]; ok {
						kv.kvData[command.Key] += command.Value
					} else {
						kv.kvData[command.Key] = command.Value
					}
				}
				commandReply.SeqId = command.SeqId
				kv.clientReply[int64(command.ClientId)] = commandReply
				replyCh, ok := kv.replyCh[int64(index)]
				if index > kv.lastApplied {
					kv.lastApplied = index
				}
				if kv.rf.GetRaftStateSize() > kv.maxraftstate {
					kv.StartSnapShot()
				}
				kv.mu.Unlock()
				if ok {
					DPrintf("------SendingApplyMsg ClientId:%d SeqId:%d Index:%d-------\n", command.ClientId, command.SeqId, index)
					replyCh <- commandReply
				}

			} else if applyMsg.SnapshotValid {
				kv.mu.Lock()
				if applyMsg.SnapshotIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				} else {
					kv.lastApplied = applyMsg.SnapshotTerm
					kv.ReadSnapShot(applyMsg.Snapshot)
					kv.mu.Unlock()
				}
			} else {
				DPrintf("error:invalid applyMsg!\n")
			}
		}
	}
}

func (kv *KVServer) getIndexReplyCh(index int) chan CommandReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if replyCh, ok := kv.replyCh[int64(index)]; ok {
		return replyCh
	} else {
		replyCh := make(chan CommandReply)
		kv.replyCh[int64(index)] = replyCh
		return replyCh
	}
}

func (kv *KVServer) CreateSnapShotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvData)
	e.Encode(kv.clientReply)
	data := w.Bytes()
	return data
}

func (kv *KVServer) StartSnapShot() {
	data := kv.CreateSnapShotData()
	go kv.rf.Snapshot(kv.lastApplied, data)
}

func (kv *KVServer) ReadSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData map[string]string
	var clientReply map[int64]CommandReply
	if d.Decode(&kvData) != nil || d.Decode(&clientReply) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		kv.kvData = kvData
		kv.clientReply = clientReply
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvData = make(map[string]string)
	kv.clientReply = make(map[int64]CommandReply)
	kv.replyCh = make(map[int64]chan CommandReply)
	kv.ReadSnapShot(kv.rf.GetSnapShot())
	// You may need initialization code here.
	go kv.ReceiveApplyMsg()
	// go runNumGoroutineMonitor()
	return kv
}

func runNumGoroutineMonitor() {
	log.Printf("协程数量->%d\n", runtime.NumGoroutine())

	for {
		select {
		case <-time.After(time.Second):
			log.Printf("协程数量->%d\n", runtime.NumGoroutine())
		}
	}
}
