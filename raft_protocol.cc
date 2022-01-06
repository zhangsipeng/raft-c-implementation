#include "raft_protocol.h"

marshall& operator<<(marshall &m, const request_vote_args& args) {
    // Your code here
    m<<args.candidateidx;
    m<<args.candidateTermNum;
    m<<args.lastlogIndex;
    m<<args.lastlogTerm;

    return m;

}
unmarshall& operator>>(unmarshall &u, request_vote_args& args) {
    // Your code here
    u>>args.candidateidx;
    u>>args.candidateTermNum;
    u>>args.lastlogIndex;
    u>>args.lastlogTerm;

    return u;
}

marshall& operator<<(marshall &m, const request_vote_reply& reply) {
    // Your code here
    m<<reply.voteridx;
    m<<reply.vote;
    m<<reply.voterTerm;
    return m;
}

unmarshall& operator>>(unmarshall &u, request_vote_reply& reply) {
    // Your code here
    u>>reply.voteridx;
    u>>reply.vote;
    u>>reply.voterTerm;

    return u;
}

marshall& operator<<(marshall &m, const append_entries_reply& reply) {
    // Your code here
    m<<reply.term;
    m<<reply.success;
    m<<reply.heartbeat;
    m<<reply.matchIndex;
    m<<reply.receiverConflictIdx;
    m<<reply.receiverConflictTerm;

    return m;
}
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply) {
    // Your code here
    m>>reply.term;
    m>>reply.success;
    m>>reply.heartbeat;
    m>>reply.matchIndex;

    m>>reply.receiverConflictIdx;
    m>>reply.receiverConflictTerm;

    return m;
}

// template<typename command>
// marshall& operator<<(marshall &m, const install_snapshot_args& args) {
//     // Your code here
//     m<<args.term;
//     m<<args.leaderId;
//     m<<args.lastIncludedIndex;
//     m<<args.lastIncludedTerm;
//     m<<args.stm_snapshot.size();
//     for (int i=0;i<args.stm_snapshot.size();i++)
//     {
//         m<<args.stm_snapshot[i];
//     }
//     m<<args.entries.size();
//     for (int i=0;i<args.entries.size();i++)
//     {
//         m<<args.entries[i];
//     }

//     return m;
// }

// template<typename command>
// unmarshall& operator>>(unmarshall &u, install_snapshot_args& args) {
//     // Your code here
//     u>>args.term;
//     u>>args.leaderId;
//     u>>args.lastIncludedIndex;
//     u>>args.lastIncludedTerm;
//     int size1;
//     u>>size1;
//     for (int i=0;i<size1;i++)
//     {
//         u>>args.stm_snapshot[i];
//     }
//     int size2;
//     u>>size2;
//     for (int i=0;i<size2;i++)
//     {
//         u>>args.entries[i];
//     }


//     return u; 
// }

marshall& operator<<(marshall &m, const install_snapshot_reply& reply) {
    // Your code here
    m<<reply.term;
    m<<reply.matchidx;

    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply) {
    // Your code here
    u>>reply.term;
    u>>reply.matchidx;

    return u;
}