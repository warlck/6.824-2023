package shardkv

import (
	"fmt"
	"time"

	"6.824/shardctrler"
)

type shardConfigState struct {
	loggedConfigNum     int
	currentConfig       shardctrler.Config
	shardIsReadyToServe map[int]map[int]bool
}

func (kv *ShardKV) fetchLatestShardConfig() {
	for {
		if kv.killed() {
			return
		}

		// ask controler for the latest configuration.
		kv.mu.Lock()
		currentConfig := kv.scs.currentConfig
		kv.mu.Unlock()

		newConfig := kv.sm.Query(-1)
		// Debug(dLog, "S%d-%d pulled latest config currentConfig: %#v, newConfig: %#v ", kv.me, kv.gid, currentConfig, newConfig)
		if newConfig.Num > currentConfig.Num {
			Debug(dLog, "S%d-%d pulled latest config currentConfig: %#v, newConfig: %#v ", kv.me, kv.gid, currentConfig, newConfig)
			// Agree on new config with the group using Raft consensus
			// Need to ensure this line eventually runs by Raft leader,
			// since it may happen that leadership is currently lost by KV server
			kv.agreeOnNewConfigNum(newConfig)

			kv.mu.Lock()
			kv.installShardConfig(newConfig.Num)
			kv.mu.Unlock()

		}

		time.Sleep(100 * time.Millisecond)
	}

}

// Install the ShardConfig state corresponding to  passed configNum value
// This state is required for correct operation of GetShard RPC
func (kv *ShardKV) applyConfigNumL(configNum int) {
	if configNum > kv.scs.loggedConfigNum {
		kv.scs.loggedConfigNum = configNum
	}
	if configNum > kv.scs.currentConfig.Num {
		kv.installShardConfig(configNum)
	}
}

func (kv *ShardKV) applyShardDataL(op Op) {
	Debug(dCommit, "S%d-%d received ShardData message, appliedConfig: %#v, opConfigNum: %d, before duplicate check", kv.me, kv.gid, kv.scs.currentConfig, op.ConfigNum)
	shardKVs := op.ShardKVs
	configNum := op.ConfigNum
	var shard int
	for key, value := range shardKVs {
		kv.stateMachine[key] = value

		// This is will keep resetting the shard value
		// but since the operation is not expensive, we keep it here for clarity
		shard = key2shard(key)
	}

	kv.scs.shardIsReadyToServe[configNum][shard] = true
}

func (kv *ShardKV) getShardKVs(gid, shard int, currentConfig, newConfig shardctrler.Config) map[string]string {
	args := GetShardArgs{
		Server:    fmt.Sprintf("S%d-%d", kv.me, kv.gid),
		ShardID:   shard,
		ConfigNum: newConfig.Num,
	}
	Debug(dInfo, "S%d-%d making GetShard request before loop for shard:%d  args:%+v, servers: %#v", kv.me, kv.gid, shard, args, currentConfig.Groups[gid])

	for {
		if servers, ok := currentConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply GetShardReply
				ok := srv.Call("ShardKV.GetShard", &args, &reply)
				if ok && (reply.Err == OK) {
					Debug(dInfo, "S%d-%d successfully completed GetShard request  for shard:%d  args:%+v, server: %#v", kv.me, kv.gid, shard, args, servers[si])
					return reply.ShardKVs
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Fetches the shards data required to serve the latest shardconfig from the previous
// shard owner group and starts the Raft agreement of the new shard data to ensure that
// KV statemachine changes are captured in the Raft log.
func (kv *ShardKV) applyNewConfig(currentConfig, newConfig shardctrler.Config) {
	shardsToServe := []int{}
	for i := range newConfig.Shards {
		if newConfig.Shards[i] == kv.gid {
			shardsToServe = append(shardsToServe, i)
		}
	}

	Debug(dInfo, "S%d-%d applying new config. newShardsToServe:%+v", kv.me, kv.gid, shardsToServe)
	for _, shard := range shardsToServe {
		go func(shard int) {
			gid := currentConfig.Shards[shard]
			// already have the necessary shard data
			if gid == kv.gid {
				return
			}

			shardKVs := kv.getShardKVs(gid, shard, currentConfig, newConfig)
			if len(shardKVs) == 0 {
				return
			}

			kv.agreeOnShardKVs(shard, shardKVs, currentConfig, newConfig)
		}(shard)
	}

}

func (kv *ShardKV) agreeOnNewConfigNum(newConfig shardctrler.Config) {
	op := Op{
		Server:    fmt.Sprintf("S%d-%d", kv.me, kv.gid),
		Op:        Config,
		ConfigNum: newConfig.Num,
	}

	go func() {
		for {
			kv.mu.Lock()
			loggedConfigNum := kv.scs.loggedConfigNum
			kv.mu.Unlock()

			if loggedConfigNum >= newConfig.Num {
				return
			}

			// Fire and forget the Raft aggrement for the latest config.
			// The agreement is required to ensure that there will be
			// no more in flight operations  belonging to older config shards
			// that group  needs to serve, before current group can correctly
			// reply to GetShard RPC
			_, _, isLeader := kv.rf.Start(op)
			if isLeader {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

}

func (kv *ShardKV) agreeOnShardKVs(shard int, shardKVs map[string]string, currentConfig, newConfig shardctrler.Config) {

	op := Op{
		Server:    fmt.Sprintf("S%d-%d", kv.me, kv.gid),
		Op:        ShardData,
		ShardKVs:  shardKVs,
		ConfigNum: newConfig.Num,
	}

	for {
		kv.mu.Lock()
		shardIsReadyToServe := kv.scs.shardIsReadyToServe[newConfig.Num][shard]
		kv.mu.Unlock()

		// When shard data has been successfully shared mong replica group,
		// we can terminate the loop
		if shardIsReadyToServe {
			return
		}

		_, _, isLeader := kv.rf.Start(op)
		if isLeader {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

}

func (kv *ShardKV) installShardConfig(configNum int) {
	oldConfig := kv.scs.currentConfig
	newConfig := kv.sm.Query(configNum)
	newConfigNum := newConfig.Num
	kv.scs.currentConfig = newConfig

	kv.scs.shardIsReadyToServe[newConfigNum] = make(map[int]bool)
	for shard := 0; shard < shardctrler.NShards; shard++ {
		// New config does not require shard movement for 'shard' value
		if (oldConfig.Shards[shard] == kv.gid || oldConfig.Shards[shard] == 0) && newConfig.Shards[shard] == kv.gid {
			kv.scs.shardIsReadyToServe[newConfigNum][shard] = true
		}
	}
	Debug(dInfo, "S%d-%d is installing shard config:%d, oldConfig:%+v, newConfig:%+v, shardsReadyToServe:%+v", kv.me, kv.gid, configNum, oldConfig, newConfig, kv.scs.shardIsReadyToServe[newConfigNum])
	// Fetch shards from previous shard owners,
	// Get ready to serve new shards
	go kv.applyNewConfig(oldConfig, newConfig)
}
