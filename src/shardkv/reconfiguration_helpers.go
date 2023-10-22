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

func (kv *ShardKV) fetchAndInstallShardConfig() {
	for {
		if kv.killed() {
			return
		}

		// ask controler for the latest configuration.
		kv.mu.Lock()
		currentConfig := kv.scs.currentConfig
		kv.mu.Unlock()

		configToFetch := currentConfig.Num + 1
		if currentConfig.Num == 0 {
			// Server just started; fetch the latest config
			configToFetch = -1
		}
		newConfig := kv.sm.Query(configToFetch)

		kv.mu.Lock()
		// Debug(dLog, "S%d-%d pulled latest config currentConfig: %#v, newConfig: %#v ", kv.me, kv.gid, currentConfig, newConfig)
		if newConfig.Num > kv.scs.currentConfig.Num {
			Debug(dLog, "S%d-%d pulled latest config currentConfig: %#v, newConfig: %#v ", kv.me, kv.gid, currentConfig, newConfig)
			// Agree on new config with the group using Raft consensus
			// Need to ensure this line eventually runs by Raft leader,
			// since it may happen that leadership is currently lost by KV server
			kv.agreeOnNewConfigNum(newConfig)
			kv.scs.shardIsReadyToServe[newConfig.Num] = make(map[int]bool)
			kv.installShardConfig(currentConfig, newConfig)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// Install the ShardConfig state corresponding to logged config value
// This state is required for correct operation of GetShard RPC
func (kv *ShardKV) processConfigOpL(config shardctrler.Config) {
	if config.Num > kv.scs.loggedConfigNum {
		kv.scs.loggedConfigNum = config.Num
	}
	if config.Num > kv.scs.currentConfig.Num {
		newConfig := config
		kv.scs.shardIsReadyToServe[newConfig.Num] = make(map[int]bool)
		kv.updateShardConfig(kv.scs.currentConfig, newConfig)
	}
}

func (kv *ShardKV) processShardDataOpL(op Op) {
	Debug(dCommit, "S%d-%d received ShardData message, currentConfig: %+v, opConfigNum: %d", kv.me, kv.gid, kv.scs.currentConfig, op.Config.Num)
	shardKVs := op.ShardKVs
	configNum := op.Config.Num
	shardID := op.ShardID

	if configNum > kv.scs.currentConfig.Num {
		newConfig := op.Config
		kv.scs.shardIsReadyToServe[newConfig.Num] = make(map[int]bool)
		kv.updateShardConfig(kv.scs.currentConfig, newConfig)
	}

	for key, value := range shardKVs {
		kv.stateMachine[key] = value
	}

	kv.scs.shardIsReadyToServe[configNum][shardID] = true
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
					Debug(dInfo, "S%d-%d successfully completed GetShard request  for shard:%d  args:%+v, server: %#v, shards: %+v", kv.me, kv.gid, shard, args, servers[si], reply.ShardKVs)
					return reply.ShardKVs
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Fetches the shards data required to serve the latest shardconfig from the previous
// shard owner group.
// If `withLogAgreement` is set to true -  starts the Raft agreement of the new shard data to ensure that
// KV statemachine changes are captured in the Raft log for recovery and logging.
// If `withLogAgreement` is set to false - apply the new shard data into state machine of KV server, and
// prepare to serve shard in current configuration.
func (kv *ShardKV) applyNewConfigShards(currentConfig, newConfig shardctrler.Config, withLogAgreement bool) {
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

			if withLogAgreement {
				kv.agreeOnShardKVs(shard, shardKVs, newConfig)
			} else {
				// apply the shardKVs to statemachine, update shardsReadyToServe state

			}
		}(shard)
	}
}

func (kv *ShardKV) agreeOnNewConfigNum(newConfig shardctrler.Config) {
	op := Op{
		Server: fmt.Sprintf("S%d-%d", kv.me, kv.gid),
		Op:     Config,
		Config: newConfig,
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

func (kv *ShardKV) agreeOnShardKVs(shard int, shardKVs map[string]string, newConfig shardctrler.Config) {

	op := Op{
		Server:   fmt.Sprintf("S%d-%d", kv.me, kv.gid),
		Op:       ShardData,
		ShardKVs: shardKVs,
		Config:   newConfig,
		ShardID:  shard,
	}

	for {
		kv.mu.Lock()
		shardIsReadyToServe := kv.scs.shardIsReadyToServe[newConfig.Num][shard]
		kv.mu.Unlock()

		// When shard data has been successfully shared among replica group,
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

func (kv *ShardKV) installShardConfig(oldConfig, newConfig shardctrler.Config, withLogAgreement bool) {
	kv.updateShardConfig(oldConfig, newConfig)
	Debug(dInfo, "S%d-%d is installing shard config:%d, oldConfig:%+v, newConfig:%+v, shardsReadyToServe:%+v", kv.me, kv.gid, newConfig.Num, oldConfig, newConfig, kv.scs.shardIsReadyToServe[newConfig.Num])
	// Fetch shards from previous shard owners,
	// Get ready to serve new shards
	kv.applyNewConfigShards(oldConfig, newConfig, withLogAgreement)
}

// Initialize the shard config state of the KV server belonging to group when it start
// There are 3 possible scenarios that decide how we need to proceed with state initialization
// Scenario I: The whole group has just started serving Client requests and  were not part of
// config changes.
// Init Process: Config starts and 0, increments monotonically
// #################
// Scenario II: The group did go through shard config changes, but crashed before snapshotting the
// last observed shard configuration.
// Init Process: Rely on the applied Raft log messages to setup the last observed configuration.
// #################
// Scenario III: Group's last observed config is part of snapshot
// Init Process: Initialize the  shard configuration by getting the shards that KV server needs to
// to serve the config.
// #################
func (kv *ShardKV) initShardConfig() {
	if kv.scs.currentConfig.Num > 0 {
		oldConfig := kv.sm.Query(kv.scs.currentConfig.Num - 1)
		newConfig := kv.scs.currentConfig
		kv.installShardConfig(oldConfig, newConfig)
		return
	}
}

func (kv *ShardKV) updateShardConfig(oldConfig, newConfig shardctrler.Config) {
	newConfigNum := newConfig.Num
	kv.scs.currentConfig = newConfig
	for shard := 0; shard < shardctrler.NShards; shard++ {
		// New config does not require shard movement for 'shard' value
		if (oldConfig.Shards[shard] == kv.gid || oldConfig.Shards[shard] == 0) && newConfig.Shards[shard] == kv.gid {
			kv.scs.shardIsReadyToServe[newConfigNum][shard] = true
		}
	}
}
