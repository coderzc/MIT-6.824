package raft

import (
	"log"
	"os"
)

// Logs
const Debug = true

var DebugLogger = log.New(os.Stdout, "DEBUG: ", log.LstdFlags)
var InfoLogger = log.New(os.Stdout, "INFO: ", log.LstdFlags)
var WarnLogger = log.New(os.Stdout, "WARN: ", log.LstdFlags)
var ErrorLogger = log.New(os.Stdout, "ERROR: ", log.LstdFlags)

func LogDebug(format string, a ...interface{}) {
	if Debug {
		DebugLogger.Printf(format, a...)
	}
}

func LogInfo(format string, a ...interface{}) {
	InfoLogger.Printf(format, a...)
}

func LogWarning(format string, a ...interface{}) {
	WarnLogger.Printf(format, a...)
}

func LogError(format string, a ...interface{}) {
	ErrorLogger.Printf(format, a...)
}

//func IsLocalMsg(msgt MessageType) bool {
//	return msgt == MsgHup || msgt == MsgBeat || msgt == MsgUnreachable ||
//		msgt == MsgSnapStatus || msgt == MsgCheckQuorum
//}
//
//func IsResponseMsg(msgt MessageType) bool {
//	return msgt == MsgAppResp || msgt == MsgVoteResp || msgt == MsgHeartbeatResp || msgt == MsgUnreachable || msgt == MsgPreVoteResp
//}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
