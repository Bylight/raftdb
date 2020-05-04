package raft

type State uint8

const (
    Follower State = iota
    Candidate
    Leader
)

const (
    NullVotedFor = "_:_"
    HeartbeatRPCTimeout = 1000
)

const Debug bool = true // for debug

func (state State) String() string {
    switch state {
    case Follower:
        return "Follower"
    case Candidate:
        return "Candidate"
    case Leader:
        return "Leader"
    default:
        return "ErrorState"
    }
}
