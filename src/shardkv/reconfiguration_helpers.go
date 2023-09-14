package shardkv

import (
	"fmt"
	"time"

	"6.824/shardctrler"
)

func (kv *ShardKV) fetchLatestShardConfig() {
	for {
		if kv.killed() {
			return
		}

		// ask controler for the latest configuration.
		kv.mu.Lock()
		currentConfig := kv.shardConfig
		kv.mu.Unlock()

		newConfig := kv.sm.Query(-1)
		// Debug(dLog, "S%d-%d pulled latest config currentConfig: %#v, newConfig: %#v ", kv.me, kv.gid, currentConfig, newConfig)
		if newConfig.Num > currentConfig.Num {
			Debug(dLog, "S%d-%d pulled latest config currentConfig: %#v, newConfig: %#v ", kv.me, kv.gid, currentConfig, newConfig)
			// Agree on new config with the group
			// Use Raft
			kv.mu.Lock()
			kv.shardConfig = newConfig
			kv.shardIsReadyToServe[newConfig.Num] = make(map[int]bool)

			for shard := 0; shard < shardctrler.NShards; shard++ {
				// New config does not require shard movement for 'shard' value
				if (currentConfig.Shards[shard] == kv.gid || currentConfig.Shards[shard] == 0) && newConfig.Shards[shard] == kv.gid {
					kv.shardIsReadyToServe[newConfig.Num][shard] = true
				}
			}

			kv.mu.Unlock()

			// Need to ensure this line eventually runs by Raft leader,
			// since it may happen that leadership is currently lost by KV server
			kv.agreeOnNewConfigNum(newConfig)
			// Fetch shards from previous shard owners,
			// Get ready to serve new shards
			kv.applyNewConfig(currentConfig, newConfig)
		}

		time.Sleep(100 * time.Millisecond)
	}

}

// Changed applied configNum state to passed configNum value
// This state is required for correct operation of GetShard RPC
func (kv *ShardKV) applyConfigNumL(configNum int) {
	kv.appliedConfigNum = configNum
}

func (kv *ShardKV) applyShardDataL(op Op) {
	Debug(dCommit, "S%d-%d received ShardData message, appliedConfig: %#v, opConfigNum: %d, before duplicate check", kv.me, kv.gid, kv.shardConfig, op.ConfigNum)
	shardKVs := op.ShardKVs
	configNum := op.ConfigNum
	var shard int
	for key, value := range shardKVs {
		kv.stateMachine[key] = value
		shard = key2shard(key)
	}

	kv.shardIsReadyToServe[configNum][shard] = true
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

func (kv *ShardKV) applyNewConfig(currentConfig, newConfig shardctrler.Config) {

	shardsToServe := []int{}
	for i := range newConfig.Shards {
		if newConfig.Shards[i] == kv.gid {
			shardsToServe = append(shardsToServe, i)
		}
	}

	Debug(dInfo, "S%d-%d applying new config. new shardsToServe:%+v", kv.me, kv.gid, shardsToServe)
	for _, shard := range shardsToServe {
		go kv.fetchAndApplyShardKVs(shard, currentConfig, newConfig)

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
			appliedConfigNum := kv.appliedConfigNum
			kv.mu.Unlock()

			if appliedConfigNum >= newConfig.Num {
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

func (kv *ShardKV) fetchAndApplyShardKVs(shard int, currentConfig, newConfig shardctrler.Config) {
	gid := currentConfig.Shards[shard]
	// already have the necessary shard data
	if gid == kv.gid {
		return
	}

	shardKVs := kv.getShardKVs(gid, shard, currentConfig, newConfig)
	if len(shardKVs) == 0 {
		return
	}

	op := Op{
		Server:    fmt.Sprintf("S%d-%d", kv.me, kv.gid),
		Op:        ShardData,
		ShardKVs:  shardKVs,
		ConfigNum: newConfig.Num,
	}

	for {
		kv.mu.Lock()
		shardIsReadyToServe := kv.shardIsReadyToServe[newConfig.Num][shard]
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
