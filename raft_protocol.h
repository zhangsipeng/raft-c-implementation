#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    // Your code here
    int candidateidx;
    int candidateTermNum;
    int lastlogIndex;
    int lastlogTerm;
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    // Your code here
    int voteridx;
    bool vote;
    int voterTerm;
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    // Your code here
    int idx;
    int term;
    // add snapshot
    int logicidx;
    //
    command  cmd;

};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    m<<entry.idx;
    m<<entry.term;
    // add snapshot
    m<<entry.logicidx;
    // 
    m<<entry.cmd;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    u>>entry.idx;
    u>>entry.term;
    // add snapshot
    u>>entry.logicidx;
    //
    u>>entry.cmd;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int term;
    int idx; //leader index
    int prevLogidx;
    int prevLogterm;
    int leaderCommit;
    std::vector<log_entry<command>> entries;
    int entrysize;
    bool heartbeat;
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m<<args.term;
    m<<args.idx;
    m<<args.prevLogidx;
    m<<args.prevLogterm;
    m<<args.leaderCommit;
    m<<args.entrysize;
    for (int i=0;i<args.entrysize;i++)
    {
        m<<args.entries[i];
    }
    
    m<<args.heartbeat;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    int tmp;
    u>>args.term;
    u>>args.idx;
    u>>args.prevLogidx;
    u>>args.prevLogterm;
    u>>args.leaderCommit;
    u>>tmp;
    args.entrysize=tmp;
    for (int i=0;i<tmp;i++)
    {
        log_entry<command> log;
        u>>log;
        args.entries.push_back(log);
    }
    
    u>>args.heartbeat;
    return u;
}

class append_entries_reply {
public:
    // Your code here
    int term;
    bool success;
    bool heartbeat;

    int matchIndex;
    // add snapshot
    int receiverConflictIdx;
    int receiverConflictTerm;
    //
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply);

template<typename command>
class install_snapshot_args {
public:
    // Your code here
    int term;
    int leaderId;
    int lastIncludedIndex;
    int lastIncludedTerm;
    // int stm_snapshot_size;
    std::vector<char> stm_snapshot;
    // int entrysize;
    std::vector<log_entry<command>> entries;
    

};

template<typename command>
marshall& operator<<(marshall &m, const install_snapshot_args<command>& args){
    m<<args.term;
    m<<args.leaderId;
    m<<args.lastIncludedIndex;
    m<<args.lastIncludedTerm;
    m<<int(args.stm_snapshot.size());
    for (int i=0;i<args.stm_snapshot.size();i++)
    {
        m<<args.stm_snapshot[i];
    }
    m<<int(args.entries.size());
    for (int i=0;i<args.entries.size();i++)
    {
        m<<args.entries[i];
    }

    return m;
}
template<typename command>
unmarshall& operator>>(unmarshall &m, install_snapshot_args<command>& args){
    //  Your code here
    m>>args.term;
    m>>args.leaderId;
    m>>args.lastIncludedIndex;
    m>>args.lastIncludedTerm;
    int size1;
    m>>size1;
    args.stm_snapshot.resize(size1);
    for (int i=0;i<size1;i++)
    {
        m>>args.stm_snapshot[i];
    }
    int size2;
    m>>size2;
    args.entries.resize(size2);
    for (int i=0;i<size2;i++)
    {
        m>>args.entries[i];
    }


    return m; 
}


class install_snapshot_reply {
public:
    // Your code here
    int term;
    int matchidx;
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h