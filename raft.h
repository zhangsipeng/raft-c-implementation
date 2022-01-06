#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

class ballotCounter{
    public:
    int Term;
    int ballots;
};



template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:
    int voteFor;
    int commit_index;
    int last_applied;
    std::vector<log_entry<command>> loglist;
    
    ballotCounter counter;
    unsigned long last_received_RPC_time;
    unsigned long getTime();

    int snapshot_idx;

    int * nextIndex;
    int * matchIndex;

    //add snapshot
    std::vector<char> last_snapshot;


private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args<command> arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args<command> arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args<command>& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:


};
template<typename state_machine, typename command>
unsigned long raft<state_machine, command>::getTime()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
}

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    mtx.lock();
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    voteFor=-1;
    last_received_RPC_time=getTime();
    last_applied=0;
    commit_index=0;
    snapshot_idx=0;
    log_entry<command> tmp;
    tmp.term=0;
    tmp.idx=0;
    // add snapshot
    tmp.logicidx=0;
    //
    loglist.push_back(tmp);
    nextIndex=new int [rpc_clients.size()];
    matchIndex=new int [rpc_clients.size()];
    unsigned long a=getTime();
    storage->recover();
    unsigned long b=getTime();
    RAFT_LOG("recover time:%d",b-a);
    if (storage->recovered)
    {
        current_term=storage->cterm;
        voteFor=storage->vFor;
        //注意recover得到的logicidx都是未定义的
        storage->llist[0].logicidx=0;
        loglist.assign(storage->llist.begin(),storage->llist.end());
        //add snapshot
        if (storage->has_snapshot)
        {
            RAFT_LOG("recover success with snapshot!");
        state->apply_snapshot(storage->stm_snapshot);
        last_snapshot.assign(storage->stm_snapshot.begin(),storage->stm_snapshot.end());
        loglist[0].logicidx=storage->lastIncludedIndex;
        loglist[0].term=storage->lastIncludedTerm;
         snapshot_idx=storage->lastIncludedIndex;
          last_applied=storage->lastIncludedIndex;
          commit_index=storage->lastIncludedIndex;
        }
        // fix bug?
        // for (int i=1;i<loglist.size();i++)
        // {
        //     loglist[i].logicidx=loglist[0].logicidx+i;
        // }
        //
        // RAFT_LOG("%d log0logicidx %d",my_id,loglist[0].logicidx);
        // assert(loglist[0].logicidx==0);
        RAFT_LOG("recover success!");
        
    }
    mtx.unlock();
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    // unsigned long a= std::chrono::duration_cast<std::chrono::milliseconds>(
    //         std::chrono::system_clock::now().time_since_epoch()
    //     ).count();
    //     printf("restart s1:%d",a);
    stopped.store(true);
    //  a= std::chrono::duration_cast<std::chrono::milliseconds>(
    //         std::chrono::system_clock::now().time_since_epoch()
    //     ).count();
    //     printf("restart s2:%d",a);
    background_ping->join();
    //  a= std::chrono::duration_cast<std::chrono::milliseconds>(
    //         std::chrono::system_clock::now().time_since_epoch()
    //     ).count();
    //     printf("restart s3:%d",a);
    background_election->join();
    // a= std::chrono::duration_cast<std::chrono::milliseconds>(
    //         std::chrono::system_clock::now().time_since_epoch()
    //     ).count();
    //     printf("restart s4:%d",a);
    background_commit->join();
    //  a= std::chrono::duration_cast<std::chrono::milliseconds>(
    //         std::chrono::system_clock::now().time_since_epoch()
    //     ).count();
    //     printf("restart s5:%d",a);
    background_apply->join();
    //  a= std::chrono::duration_cast<std::chrono::milliseconds>(
    //         std::chrono::system_clock::now().time_since_epoch()
    //     ).count();
    //     printf("restart s6:%d",a);
    thread_pool->destroy();
    // a= std::chrono::duration_cast<std::chrono::milliseconds>(
    //         std::chrono::system_clock::now().time_since_epoch()
    //     ).count();
    //     printf("restart s7:%d",a);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    
    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    
    bool logchanged=0;
    RAFT_LOG("here i am 1");
    if (role==leader)
    {
        RAFT_LOG("leader %d new cmd:%d %d",my_id,current_term,loglist.size());
        log_entry<command> tmp;
        tmp.cmd=cmd;
        tmp.idx=loglist.size();
        //add snapshot
        tmp.logicidx=tmp.idx+loglist[0].logicidx;
        //
        tmp.term=current_term;
        loglist.push_back(tmp);
        logchanged=1;
        // storage->persistlog(loglist);
    }
    else{
        // forward the cmd
        return false;

    }
    //add snapshot
    index=loglist.size()-1+loglist[0].logicidx;
    //
    // index=loglist.size()-1;
    term = current_term;
    if (logchanged)
    {
        storage->persistlog(loglist,loglist.size()-2);
    }
    RAFT_LOG("here i am");
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    std::unique_lock<std::mutex> lock(mtx);
    auto snapshot=state->snapshot();
    last_snapshot=snapshot;
    std::vector<log_entry<command>> tmp;
    log_entry<command> log0;
    log0.idx=0;
    log0.logicidx=loglist[last_applied-loglist[0].logicidx].logicidx;
    log0.term=loglist[last_applied-loglist[0].logicidx].term;
    tmp.push_back(log0);

    for (int i=last_applied+1;i<loglist.size();i++)
    {
        loglist[i].idx=i-last_applied;
        tmp.push_back(loglist[i]);
    }
    // loglist=tmp;
    //add latest fix
    //
    loglist.assign(tmp.begin(),tmp.end());
    storage->persistsnapshot(snapshot,log0.logicidx,log0.term);
    storage->persistlog(loglist,0);

    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    bool metachanged=0;
    bool logchanged=0;
    RAFT_LOG("received vote request %d",my_id);
    std::unique_lock<std::mutex> lock(mtx);
    // last_received_RPC_time=getTime();
    if (voteFor!=-1&&args.candidateTermNum==current_term)
    {
        reply.voteridx=my_id;
        reply.vote=false;
        reply.voterTerm=current_term;
        last_received_RPC_time=getTime();
        
        return 0;
    }
     // add snapshot
     if (args.candidateTermNum<current_term||(args.lastlogTerm<loglist[loglist.size()-1].term)||(args.lastlogTerm==loglist[loglist.size()-1].term&&args.lastlogIndex<loglist.size()-1+loglist[0].logicidx))
     //
    // if (args.candidateTermNum<current_term||(args.lastlogTerm<loglist[loglist.size()-1].term)||(args.lastlogTerm==loglist[loglist.size()-1].term&&args.lastlogIndex<loglist.size()-1))
    {
        
        if (args.candidateTermNum>current_term)
        {
            current_term=args.candidateTermNum;
            voteFor=-1;
            role=follower;
            metachanged=1;
            // storage->persistmeta(current_term,voteFor);
        }
        reply.voteridx=my_id; 
        reply.vote=false;
        reply.voterTerm=current_term;
        if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
        return 0;
    }
    else{
        reply.voteridx=my_id;
        reply.vote=true;
        current_term=args.candidateTermNum;
        voteFor=args.candidateidx;
        
        role=follower;
        reply.voterTerm=current_term;
        metachanged=1;
        // storage->persistmeta(current_term,voteFor);
    }
    last_received_RPC_time=getTime();
    RAFT_LOG("%d vote given %d %d",my_id,args.lastlogIndex,loglist.size()-1);
    if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    bool metachanged=0;
    bool logchanged=0;
    RAFT_LOG("received vote reply %d",target);
    std::unique_lock<std::mutex> lock(mtx);
    if (role==leader||role==follower) 
    {
        if (reply.voterTerm>current_term)
        {
            current_term=reply.voterTerm;
            voteFor=-1;
            role=follower;
            metachanged=1;
            // storage->persistmeta(current_term,voteFor);
        }
        if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
        return;
    }
    last_received_RPC_time=getTime();
    if (reply.vote)
    {
        counter.ballots++;
        if (counter.ballots>=(rpc_clients.size()/2+1))
        {
            RAFT_LOG("new leader! %d",my_id);
            role=leader;
            for (int i=0;i<rpc_clients.size();i++)
            {
                // add snapshot
                //fix latest 加了一个-1
                nextIndex[i]=loglist.size()+loglist[0].logicidx;
                matchIndex[i]=-1;
                //
                // nextIndex[i]=loglist.size();
                // matchIndex[i]=0;
            }
        }
    }
    else {
        if (reply.voterTerm>current_term)
        {
            current_term=reply.voterTerm;
            voteFor=-1;
            role=follower;
            metachanged=1;
            // storage->persistmeta(current_term,voteFor);
        }
    }
    if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    bool metachanged=0;
    bool logchanged=0;
    std::unique_lock<std::mutex> lock(mtx);
    if (arg.heartbeat)
    {
        // RAFT_LOG("%d receive heartbeat",my_id);
        if (arg.term>=current_term)
    {
        if (arg.term==current_term&&arg.leaderCommit>commit_index&&arg.leaderCommit<loglist.size()+loglist[0].logicidx) 
        {
            commit_index=arg.leaderCommit;
            RAFT_LOG("%d commit idx %d",my_id,commit_index);
        }
        current_term=arg.term;
        
        role=follower;
        reply.term=current_term;
        reply.success=true;
        reply.heartbeat=true;
        last_received_RPC_time=getTime();
        metachanged=1;
        // storage->persistmeta(current_term,voteFor);
    }
    else {
        reply.success=false;
        reply.term=current_term;
        reply.heartbeat=true;
        last_received_RPC_time=getTime();
    }
    }
    else{
        // RAFT_LOG("%d receive log communication",my_id);
        reply.heartbeat=false;
        if (arg.term>=current_term)
        {
            current_term=arg.term;
            role=follower;//add new
            metachanged=1;
            // storage->persistmeta(current_term,voteFor);
            //add snapshot
            if (arg.prevLogidx<=loglist.size()+loglist[0].logicidx-1&&arg.prevLogterm==loglist[arg.prevLogidx-loglist[0].logicidx].term)
            {
                loglist.resize(arg.prevLogidx-loglist[0].logicidx+arg.entrysize+1);//add
                  for (int i=arg.prevLogidx+1-loglist[0].logicidx;i<=arg.prevLogidx+arg.entrysize-loglist[0].logicidx;i++)
            {
                
                if (i<loglist.size())
                {
                    loglist[i]=arg.entries[i-arg.prevLogidx-1+loglist[0].logicidx];

                }
                else{
                    loglist.push_back(arg.entries[i-arg.prevLogidx-1+loglist[0].logicidx]);
                }
            }
             logchanged=1;
                if (arg.leaderCommit>commit_index&&arg.leaderCommit<loglist.size()+loglist[0].logicidx) 
                {
                    commit_index=arg.leaderCommit;
                    RAFT_LOG("%d commit idx %d",my_id,commit_index);
                }
                
                reply.matchIndex=arg.prevLogidx+arg.entrysize;
                reply.term=current_term;
                reply.success=true;
                RAFT_LOG("%d receive log communication matchidx:%d",my_id,reply.matchIndex);
            }
            //
            // if (arg.prevLogidx<=loglist.size()-1&&arg.prevLogterm==loglist[arg.prevLogidx].term)
            // {
            //     loglist.resize(arg.prevLogidx+arg.entrysize+1);//add
            //       for (int i=arg.prevLogidx+1;i<=arg.prevLogidx+arg.entrysize;i++)
            // {
                
            //     if (i<loglist.size())
            //     {
            //         loglist[i]=arg.entries[i-arg.prevLogidx-1];

            //     }
            //     else{
            //         loglist.push_back(arg.entries[i-arg.prevLogidx-1]);
            //     }
            // }
            //  logchanged=1;
            //     if (arg.leaderCommit>commit_index&&arg.leaderCommit<loglist.size()) 
            //     {
            //         commit_index=arg.leaderCommit;
            //         RAFT_LOG("%d commit idx %d",my_id,commit_index);
            //     }
                
            //     reply.matchIndex=arg.prevLogidx+arg.entrysize;
            //     reply.term=current_term;
            //     reply.success=true;
            //     RAFT_LOG("%d receive log communication matchidx:%d",my_id,reply.matchIndex);
            // }
            else{
                ///add snapshot
                //没有加next优化的版本直接去掉下面这段即可，只有两处
                // int ridx=0;
                // for (int i=loglist.size()-1;i>=0;i--)
                // {
                //     if (loglist[i].term<=arg.prevLogterm)
                //     {
                //         ridx=i;
                //         break;
                //     }
                // }
                // reply.receiverConflictTerm=loglist[ridx].term;
                // reply.receiverConflictIdx=ridx;
                //
                // RAFT_LOG("%d communicate failed:%d %d %d %d \n",my_id,arg.prevLogidx,arg.prevLogterm,loglist.size()+loglist[0].logicidx-1,loglist[arg.prevLogidx-loglist[0].logicidx].term);
                reply.term=current_term;
                reply.success=false;
            }
        }
        else{
            reply.term=current_term;
            reply.success=false;
        }
      

    }
    last_received_RPC_time=getTime();
    if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
        if (logchanged)
        {
            int tm=arg.prevLogidx;
            if (tm<loglist[0].logicidx)
            {
                printf("tm:%d logic:%d",tm,loglist[0].logicidx);
                
            }
            storage->persistlog(loglist,tm-loglist[0].logicidx);
        }
    
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
     std::unique_lock<std::mutex> lock(mtx);
     bool metachanged=0;
    bool logchanged=0;
     last_received_RPC_time=getTime();
    if (role!=leader)
    {
        return;
    }
    if (reply.term>current_term)
    {
        voteFor=-1;
        current_term=reply.term;
        metachanged=1;
        // storage->persistmeta(current_term,voteFor);
        role=follower;
        if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
        return;
    }
    if (reply.heartbeat)
    {
        if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
    return;
    }
    else{
        if (reply.success)
        {
            // RAFT_LOG("replicate success!")
            //add snapshot
            //add fix part5 last
            if (reply.matchIndex>matchIndex[target])
                matchIndex[target]=reply.matchIndex;
            //
            if (nextIndex[target]<loglist.size()+loglist[0].logicidx)
            {
                matchIndex[target]=reply.matchIndex;//add new
                //增加的一行
                nextIndex[target]=matchIndex[target]+1;
                // nextIndex[target]++;
                //改成了小于等于而不是不等于
                if ( nextIndex[target]<=loglist.size()-1+loglist[0].logicidx)
                nextIndex[target]=loglist.size()-1+loglist[0].logicidx;
                else{
                    nextIndex[target]=loglist.size()+loglist[0].logicidx;
                }
            }
             RAFT_LOG("%d replicate success! %d %d %d",target,matchIndex[target],nextIndex[target],loglist.size()+loglist[0].logicidx);
             //add snapshot
            for (int i=commit_index+1-loglist[0].logicidx;i<loglist.size();i++)
            {
                int cnt=0;
                if (loglist[i].term!=current_term)
                {
                    continue;
                }
                for(int j=0;j<rpc_clients.size();j++)
                {
                    if (j==my_id)
                    {
                        cnt++;
                        continue;
                    }
                    //add snapshot
                    if (matchIndex[j]>=i+loglist[0].logicidx)
                    {
                        cnt++;
                    }
                }
                if (cnt>=(rpc_clients.size()/2)+1)
                {
                    RAFT_LOG("leader commit idx %d",commit_index);
                    //add snapshot
                    commit_index=i+loglist[0].logicidx;
                }
                else{
                    continue;
                }
            }
        }
        else{
            //add snapshot
            // for (int i=loglist.size()-1;i>=0;i--)
            // {
            //     if (loglist[i].term<=reply.receiverConflictTerm&&i<=reply.receiverConflictIdx)
            //        {
            //            nextIndex[target]=i+1;
            //            break;
            //        }
            // }
            //

            //没有加next优化的版本
            nextIndex[target]--;

        }
       
    }
    if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args<command> args, install_snapshot_reply& reply) {
    // Your code here:
     
    std::unique_lock<std::mutex> lock(mtx);
    bool metachanged=0;
    bool logchanged=0;
    last_received_RPC_time=getTime();
    if (current_term>args.term)
    {
        reply.term=current_term;
        return 0;
    }
    else{
        if (args.term>current_term)
        {
            current_term=args.term;
            role=follower;
            voteFor=-1;
            metachanged=1;
        }
        if (args.lastIncludedTerm==loglist[0].term&&args.lastIncludedIndex==loglist[0].logicidx&&loglist.size()>=args.entries.size())
        {
           
            reply.term=current_term;
            reply.matchidx=args.lastIncludedIndex+args.entries.size()-1;
              if (metachanged)
            {
                storage->persistmeta(current_term,voteFor);
            }
            return 0;
        }
        state->apply_snapshot(args.stm_snapshot);
        last_applied=args.lastIncludedIndex;
        commit_index=args.lastIncludedIndex;
        loglist[0].term=args.lastIncludedTerm;
        loglist[0].logicidx=args.lastIncludedIndex;
        last_snapshot.assign(args.stm_snapshot.begin(),args.stm_snapshot.end());
        loglist.resize(args.entries.size());
        assert(args.entries.size()!=0);
        for (int i=0;i<args.entries.size();i++)
        {
            loglist[i]=args.entries[i];
        }
          if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
        storage->persistsnapshot(args.stm_snapshot,args.lastIncludedIndex,args.lastIncludedTerm);
        storage->persistlog(loglist,0);
        reply.term=current_term;
        reply.matchidx=args.lastIncludedIndex+args.entries.size()-1;
        RAFT_LOG("%d install snapshot! matcheidx:%d,lastinclude:%d",my_id,reply.matchidx,args.lastIncludedIndex);
        // if ((args.lastIncludedTerm<loglist[0].term)||(args.lastIncludedTerm==loglist[0].term&&args.lastIncludedIndex<loglist[0].logicidx))
        // {
        //     for (int i=0;i<loglist.size();i++)
        //     {
        //         loglist[i].logicidx=args.lastIncludedIndex+i;
        //     }
        //     state->apply_snapshot(args.stm_snapshot);
        //     loglist[0].logicidx=args.lastIncludedIndex;
        //     loglist[0].term=args.lastIncludedTerm;
        //     reply.term=current_term;
        //     storage->persistsnapshot(args.stm_snapshot,args.lastIncludedIndex,args.lastIncludedTerm);
        // }
        // else{
        //     std::vector<log_entry<command>> tmp;
        //     log_entry<command> log0;
        //     int match=0;
        //     log0.idx=0;
        //     log0.logicidx=args.lastIncludedIndex;
        //     log0.term=args.lastIncludedTerm;
        //     tmp.push_back(log0);
        //     for (int i=0;i<loglist.size();i++)
        //     {

        //         if ((args.lastIncludedTerm<loglist[i].term)||(args.lastIncludedTerm==loglist[i].term&&args.lastIncludedIndex<loglist[i].idx))
        //         {
        //             match=i;
        //         }
        //     }
        //     for (int i=match;i<loglist.size();i++)
        //     {
        //         loglist[i].logicidx=i+args.lastIncludedIndex;
        //         tmp.push_back(loglist[i]);
        //     }
        //     loglist=tmp;
        //     state->apply_snapshot(args.stm_snapshot);
        //     // loglist[0].logicidx=args.lastIncludedIndex;
        //     // loglist[0].term=args.lastIncludedTerm;
        //     reply.term=current_term;
        //     storage->persistsnapshot(args.stm_snapshot,args.lastIncludedIndex,args.lastIncludedTerm);
        // }
    }
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args<command>& arg, const install_snapshot_reply& reply) {
    // Your code here:
     std::unique_lock<std::mutex> lock(mtx);
     bool metachanged=0;
     last_received_RPC_time=getTime();
    if (reply.term>current_term)
    {
        current_term=reply.term;
        role=follower;
        voteFor=-1;
        metachanged=1;
    }
    else{
        // RAFT_LOG("%d install success!",target);
        matchIndex[target]=reply.matchidx;
        nextIndex[target]=loglist.size()-1+loglist[0].logicidx;
        //add fix
        if (nextIndex[target]<=matchIndex[target])
        {
            nextIndex[target]=matchIndex[target]+1;
        }
        //
        // RAFT_LOG("commitidx:%d,size:%d,%d",commit_index,loglist.size(),loglist[0].logicidx);
        for (int i=commit_index+1-loglist[0].logicidx;i<loglist.size();i++)
            {
                int cnt=0;
                if (loglist[i].term!=current_term)
                {
                    continue;
                }
                for(int j=0;j<rpc_clients.size();j++)
                {
                    if (j==my_id)
                    {
                        cnt++;
                        continue;
                    }
                    //add snapshot
                    if (matchIndex[j]>=i+loglist[0].logicidx)
                    {
                        cnt++;
                    }
                }
                
                if (cnt>=(rpc_clients.size()/2)+1)
                {
                    RAFT_LOG("leader commit idx %d",commit_index);
                    //add snapshot
                    commit_index=i+loglist[0].logicidx;
                }
                else{
                    continue;
                }
            }
    }
     if (metachanged)
    {
        storage->persistmeta(current_term,voteFor);
    }

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    // RAFT_LOG("%d send request",my_id);
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
    
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args<command> arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).

    
    while (true) {
        
        if (is_stopped()) return;
        // Your code here:
        // RAFT_LOG("%d %d",clock(),last_received_RPC_time);
        // RAFT_LOG("enter here");
        
        // unsigned long now = getTime();
        // printf("%ld \n",now);
        
        mtx.lock();
        bool metachanged=0;
        bool logchanged=0;
        if (((role==follower)&&(getTime()-last_received_RPC_time)>(300+200*my_id/rpc_clients.size()))||(role==candidate&&(getTime()-last_received_RPC_time)>800+200*my_id/rpc_clients.size()))
        {
            RAFT_LOG("begin election");
            // RAFT_LOG("%ld %ld",getTime(),last_received_RPC_time);
            role=candidate;
            current_term++;
            counter.ballots=1;
            voteFor=my_id;
            metachanged=1;
            // storage->persistmeta(current_term,voteFor);
            counter.Term=current_term;
            last_received_RPC_time=getTime();
            for (int i=0;i<rpc_clients.size();i++)
            {
                if (i==my_id) continue;
                 request_vote_args tmp;
                 tmp.candidateidx=my_id;
                 tmp.candidateTermNum=current_term;
                //  add snapshot
                tmp.lastlogIndex=loglist.size()-1+loglist[0].logicidx;
                //
                //  tmp.lastlogIndex=loglist.size()-1;
                 tmp.lastlogTerm=loglist[loglist.size()-1].term;
                // add new
                 thread_pool->addObjJob(this, &raft::send_request_vote,i, tmp);
                
                 
            }
        
        }
        
        if (metachanged)
        {
            storage->persistmeta(current_term,voteFor);
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.   
       
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();
        if (role==leader)
        {
            
            for (int i=0;i<rpc_clients.size();i++)
            {
                if (i==my_id)
                continue;
                //add snapshot
                // printf("log info:%d %d %d %d\n",nextIndex[i],matchIndex[i],loglist.size(),loglist[0].logicidx);
                if ((loglist.size()+loglist[0].logicidx>nextIndex[i])||(matchIndex[i]<int(loglist.size()-1)+loglist[0].logicidx))
                //
                // if (loglist.size()>nextIndex[i])
                {   
                    
                     
                    
                    append_entries_args<command> tmp;
                    tmp.term=current_term;
                    tmp.idx=my_id;
                    // RAFT_LOG("nextidx %d",nextIndex[i])

                    //add snapshot
                    // RAFT_LOG("%d %d %d %d",i,loglist[0].logicidx,matchIndex[i],nextIndex[i]);
                    //删掉了&&matchIndex[i]==-1
                    if ((nextIndex[i]-loglist[0].logicidx<=1&&loglist[0].logicidx)||(matchIndex[i]==-1&&loglist[0].logicidx))
                    {
                        
                        install_snapshot_args<command> arg;
                        arg.term=current_term;
                        arg.leaderId=my_id;
                        arg.lastIncludedIndex=loglist[0].logicidx;
                        arg.lastIncludedTerm=loglist[0].term;
                        arg.stm_snapshot.assign(last_snapshot.begin(),last_snapshot.end());
                          for (int j=0;j<loglist.size();j++)
                        {
                            arg.entries.push_back(loglist[j]);
                        }
                        // send_install_snapshot(i,arg);
                        thread_pool->addObjJob(this, &raft::send_install_snapshot,i, arg);
                        // add availa
                        // maxsize[i]++;
                        //
                       
                       
                    }
                    else{
                       
                    tmp.prevLogidx=nextIndex[i]-1;
                    tmp.prevLogterm=loglist[nextIndex[i]-loglist[0].logicidx-1].term;
                    tmp.heartbeat=false;
                    tmp.leaderCommit=commit_index;
                    tmp.entrysize=loglist.size()+loglist[0].logicidx-nextIndex[i];
                    for (int j=0;j<tmp.entrysize;j++)
                    {
                        // printf("\n%d %d %d\n",nextIndex[i],loglist[0].logicidx,j);
                        // assert(nextIndex[i]-loglist[0].logicidx+j>=0);
                        tmp.entries.push_back(loglist[nextIndex[i]-loglist[0].logicidx+j]);
                        
                    }
                     thread_pool->addObjJob(this, &raft::send_append_entries,i, tmp);
                    

                    }
                    // old version
                    // tmp.prevLogidx=nextIndex[i]-1;
                    // tmp.prevLogterm=loglist[nextIndex[i]-1].term;
                    // tmp.heartbeat=false;
                    // tmp.leaderCommit=commit_index;
                    // tmp.entrysize=loglist.size()-nextIndex[i];
                    // for (int j=0;j<tmp.entrysize;j++)
                    // {
                    //     tmp.entries.push_back(loglist[nextIndex[i]+j]);
                        
                    // }
                    //  thread_pool->addObjJob(this, &raft::send_append_entries,i, tmp);
                }
                
            }
        }
        mtx.unlock();
        LABEL:
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();
        if (last_applied<commit_index)
        {
            
            for (int i=last_applied+1;i<=commit_index;i++)
            {
                //add snapshot
                // RAFT_LOG("i:%d,logicidx:%d",i,loglist[0].logicidx);
                assert(i-loglist[0].logicidx>=0);
                state->apply_log(loglist[i-loglist[0].logicidx].cmd);
            }
            RAFT_LOG("%d %d applied",last_applied+1,commit_index);
            last_applied=commit_index;
        }

        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
         mtx.lock();
         
         if (role==leader)
         {
            //  RAFT_LOG("begin send heartbeat");
             for (int i=0;i<rpc_clients.size();i++)
            {
                if (i==my_id) continue;
                append_entries_args<command> tmp;
                 tmp.term=current_term;
                 tmp.idx=my_id;
                 tmp.heartbeat=true;
                 tmp.entrysize=0;
                 //add snapshot
                 tmp.leaderCommit=0;
                 if (matchIndex[i]>=commit_index)
                 //
                 tmp.leaderCommit=commit_index;
                
                 thread_pool->addObjJob(this, &raft::send_append_entries,i, tmp);
               
                 
            }
            //
            // for (int i=0;i<loglist.size();i++)
            // {
            //     printf("logvalue %d ",loglist[i].cmd.value);
            // }
            //
            last_received_RPC_time=getTime();
            
         }


       
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/



#endif // raft_h