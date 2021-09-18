package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func IsLocalMsg(msgt MessageType) bool {
	return msgt == MsgHup || msgt == MsgBeat || msgt == MsgUnreachable ||
		msgt == MsgSnapStatus || msgt == MsgCheckQuorum
}

func IsResponseMsg(msgt MessageType) bool {
	return msgt == MsgAppResp || msgt == MsgVoteResp || msgt == MsgHeartbeatResp || msgt == MsgUnreachable || msgt == MsgPreVoteResp
}
