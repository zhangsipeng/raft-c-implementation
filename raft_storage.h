#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>

#include <mutex>


template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    int cterm;
    int vFor;
    std::vector<log_entry<command>> llist;
    int mylogsize;
    int metafd;
    int logfd;
    int snapshotfd;

    int lastIncludedIndex;
    int lastIncludedTerm;
    std::vector<char> stm_snapshot;

    int __lastlogsize;
    

    
    bool has_snapshot;
    bool recovered;


    // void persist(int &current_term,int &voteFor,vector<log_entry<command>> &loglist);
    void persistmeta(int &current_term,int &voteFor);
    void persistlog(std::vector<log_entry<command>> loglist,int mIndex);
    void persistsnapshot(std::vector<char> stm_snapshot,int  lastIncludedIndex,int  lastIncludedTerm);
    void recover();
private:
    std::mutex mtx;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
    metafd=open((dir+"/meta.txt").c_str(),O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
    logfd=open((dir+"/log.txt").c_str(),O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
    // add snapshot 
    snapshotfd=open((dir+"/snapshot.txt").c_str(),O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
    // 
    __lastlogsize=1;
    if (metafd==-1||logfd==-1)
    {
        printf("open failed! %s",strerror(errno));
        
        exit(0);
    }
   

}

// template<typename command>
// void raft_storage<command>::persist(int &current_term,int &voteFor,vector<log_entry<command>> &loglist)
// {
//     mtx.lock();
//     lseek(metafd,0,SEEK_SET);
//     lseek(logfd,0,SEEK_SET);
//     int tmp=loglist.size();
//     void * buf=&current_term;
//     write(metafd,buf,sizeof(int));
//     buf=&voteFor;
//     write(metafd,buf,sizeof(int));
//     buf=&tmp;
//     write(metafd,buf,sizeof(int));

//     for (int i=0;i<tmp;i++)
//     {
//         buf=&loglist[i].idx;
//         write(logfd,buf,sizeof(int));
//         buf=&loglist[i].term;
//         write(logfd,buf,sizeof(int));
//         buf=&loglist[i].cmd.size();
//         write(logfd,buf,sizeof(int));
//         loglist[i].cmd.serialize(buf,loglist[i].cmd.size());
//         write(logfd,buf,loglist[i].cmd.size());
//     }
//     mtx.unlock();
// }

template<typename command>
void raft_storage<command>::persistmeta(int &current_term,int &voteFor)
{
    mtx.lock();
    
    // printf("begin persist meta\n");
    lseek(metafd,0,SEEK_SET);
    void * buf=&current_term;
    write(metafd,(char *)buf,sizeof(int));
    buf=&voteFor;
    write(metafd,(char *)buf,sizeof(int));

    // lseek(metafd,0,SEEK_SET);
    // read(metafd,buf,sizeof(int));
    // int tmp1=*(int*)buf;
    // read(metafd,buf,sizeof(int));
    // int tmp2=*(int*)buf;
    mtx.unlock();
    // printf("end persist meta %d %d %d %d\n",current_term,voteFor,tmp1,tmp2);
    
   
    
}
template<typename command>
void raft_storage<command>::persistlog(std::vector<log_entry<command>> loglist,int mIndex)//引用loglist会有问题吗
{
    //dont persist in chdb
    return;
    //


    
    // mtx.lock();
    // lseek(logfd,0,SEEK_SET);
    // int tmp=loglist.size();
    // void * buf;
    // buf=&tmp;
    // write(logfd,(char *)buf,sizeof(int));
    // for (int i=0;i<tmp;i++)
    // {
    //     buf=&loglist[i].idx;
    //     write(logfd,(char *)buf,sizeof(int));
    //     buf=&loglist[i].term;
    //     write(logfd,(char *)buf,sizeof(int));
    //     int lsize=loglist[i].cmd.size();
    //     buf=&lsize;
    //     write(logfd,(char *)buf,sizeof(int));
    //     loglist[i].cmd.serialize((char *)buf,loglist[i].cmd.size());
    //     write(logfd,(char *)buf,loglist[i].cmd.size());
    // }
    // mtx.unlock();
    mtx.lock();
    if (mIndex==0||__lastlogsize==1||__lastlogsize<mIndex+1)
    {
        // printf("begin here %d %d",mIndex,loglist.size()-1);
        int totalsize=8;
        lseek(logfd,4,SEEK_SET);
        int tmp=loglist.size();
        __lastlogsize=tmp;
        void * buf;
        buf=&tmp;
        write(logfd,(char *)buf,sizeof(int));
        for (int i=0;i<tmp;i++)
        {   int one_log_size=16;
            buf=&loglist[i].idx;
            write(logfd,(char *)buf,sizeof(int));
            buf=&loglist[i].term;
            write(logfd,(char *)buf,sizeof(int));
            int lsize=loglist[i].cmd.size();
            buf=&lsize;
            write(logfd,(char *)buf,sizeof(int));
        //     if (lsize>0)
        // {
        //     printf("should be zeor lsize %d\n",lsize);
        //     exit(1);
        // }
            buf=new char [loglist[i].cmd.size()];
            loglist[i].cmd.serialize((char *)buf,loglist[i].cmd.size());
            write(logfd,(char *)buf,loglist[i].cmd.size());

            one_log_size+=loglist[i].cmd.size();
            buf=&one_log_size;
            write(logfd,(char *)buf,sizeof(int));

            totalsize+=one_log_size;

        }
        lseek(logfd,0,SEEK_SET);
        buf=&totalsize;
        write(logfd,(char *)buf,sizeof(int));
       
        // printf("end here");
        

    }
    else{
    
    
    int tmp=loglist.size();
    int nop;
    void * buf=&nop;
    lseek(logfd,0,SEEK_SET);
    int lastsize=0;
    read(logfd,buf,sizeof(int));
    lastsize=*(int *)buf;


    int lastlogsize=0;
    read(logfd,buf,sizeof(int));
    lastlogsize=*(int *)buf;
    assert(lastlogsize==__lastlogsize);
    // if (mIndex>=loglist.size())
    // {
    //     printf("mindex:%d size:%d\n",mIndex,loglist.size());
    // }
    assert(mIndex<loglist.size());

    


    lseek(logfd,lastsize-4,SEEK_SET);

    int lostsize=0;
    for (int i=lastlogsize-1;i>mIndex;i--)
    {
        int logsize=0;
        read(logfd,buf,sizeof(int));
        logsize=*(int *)buf;
        lostsize+=logsize;
        lseek(logfd,-(logsize+4),SEEK_CUR);
    }

    lastsize-=lostsize;
    lseek(logfd,4,SEEK_CUR);
    for (int i=mIndex+1;i<loglist.size();i++)
    {
        int one_log_size=16;
        buf=&loglist[i].idx;
        write(logfd,(char *)buf,sizeof(int));
        buf=&loglist[i].term;
        write(logfd,(char *)buf,sizeof(int));
        int lsize=loglist[i].cmd.size();
        buf=&lsize;
        write(logfd,(char *)buf,sizeof(int));
        //
        buf=new char [loglist[i].cmd.size()];
        //
        loglist[i].cmd.serialize((char *)buf,loglist[i].cmd.size());
        write(logfd,(char *)buf,loglist[i].cmd.size());
        one_log_size+=loglist[i].cmd.size();
        
        buf=&one_log_size;
        write(logfd,(char *)buf,sizeof(int));

        lastsize+=one_log_size;
    }

    lseek(logfd,0,SEEK_SET);
    buf=&lastsize;
    write(logfd,(char *)buf,sizeof(int));
    buf=&tmp;
    write(logfd,(char *)buf,sizeof(int));
    __lastlogsize=tmp;
    
    }


    
    // for (int i=0;i<tmp;i++)
    // {
    //     buf=&loglist[i].idx;
    //     write(logfd,(char *)buf,sizeof(int));
    //     buf=&loglist[i].term;
    //     write(logfd,(char *)buf,sizeof(int));
    //     int lsize=loglist[i].cmd.size();
    //     buf=&lsize;
    //     write(logfd,(char *)buf,sizeof(int));
    //     loglist[i].cmd.serialize((char *)buf,loglist[i].cmd.size());
    //     write(logfd,(char *)buf,loglist[i].cmd.size());
    // }
    mtx.unlock();
    
    
}

template<typename command>
void raft_storage<command>::persistsnapshot(std::vector<char> stm_snapshot,int  lastIncludedIndex,int  lastIncludedTerm)
{
    mtx.lock();
    int nop=0;
    void * buf=&nop;
    buf=&lastIncludedIndex;
    write(snapshotfd,(char*)buf,sizeof(int));
    buf=&lastIncludedTerm;
    write(snapshotfd,(char*)buf,sizeof(int));
    int snapshotsize=stm_snapshot.size();
    buf=&snapshotsize;
    write(snapshotfd,(char*)buf,sizeof(int));
    for (int i=0;i<snapshotsize;i++)
    {
        buf=&stm_snapshot[i];
        write(snapshotfd,(char *)buf,sizeof(char));
    }


    mtx.unlock();
}


template<typename command>
void raft_storage<command>::recover()
{
    
    // int tmp;
    // void * buf=&tmp;
    // lseek(metafd,0,SEEK_SET);
    // lseek(logfd,0,SEEK_SET);
    // if(!read(metafd,buf,sizeof(int)))
    // {
    //     recovered=false;
    //     return;
    // }
    // cterm=*((int*) buf);
    // read(metafd,buf,sizeof(int));
    // vFor=*((int *) buf);
    // read(logfd,buf,sizeof(int));
    // mylogsize=*((int *) buf);

    // for (int i=0;i<mylogsize;i++)
    // {
    //     read(logfd,buf,sizeof(int));
    //     log_entry<command> tmp;
    //     tmp.idx=*((int *) buf);
    //     read(logfd,buf,sizeof(int));
    //     tmp.term=*((int *) buf);
    //     read(logfd,buf,sizeof(int));
    //     int lsize=*((int *)  buf);
    //     read(logfd,buf,lsize);
    //     tmp.cmd.deserialize((char *)buf,lsize);
    //     llist.push_back(tmp);

    // }
    // recovered=true;


    int tmp;
    void * buf=&tmp;
    lseek(metafd,0,SEEK_SET);
    // lseek(logfd,0,SEEK_SET);
    // read(logfd,buf,sizeof(int));
    // printf("recover totalsize %d!\n",tmp);
    lseek(logfd,4,SEEK_SET);
    if(!read(metafd,buf,sizeof(int)))
    {
        recovered=false;
        return;
    }
    cterm=*((int*) buf);
    read(metafd,buf,sizeof(int));
    vFor=*((int *) buf);

    read(logfd,buf,sizeof(int));
    mylogsize=*((int *) buf);

    for (int i=0;i<mylogsize;i++)
    {
        read(logfd,buf,sizeof(int));
        log_entry<command> tmp;
        tmp.idx=*((int *) buf);
        read(logfd,buf,sizeof(int));
        tmp.term=*((int *) buf);
        read(logfd,buf,sizeof(int));
        int lsize=*((int *)  buf);
        //
        buf=new char [lsize];
        //
        read(logfd,buf,lsize);
        // if (lsize>0)
        // {
        // printf("lsize %d\n",lsize);
        tmp.cmd.deserialize((char *)buf,lsize);
        // }

        read(logfd,buf,sizeof(int));
        llist.push_back(tmp);

    }
    recovered=true;
    has_snapshot=true;
    ///add snapshot
    lseek(snapshotfd,0,SEEK_SET);
    
    if(!read(snapshotfd,(char *)buf,sizeof(int)))
    {
        has_snapshot=false;
        return;
    }
    
    lastIncludedIndex=*(int *)buf;
    read(snapshotfd,(char *)buf,sizeof(int));
    lastIncludedTerm=*(int *)buf;
    read(snapshotfd,(char *)buf,sizeof(int));
    int snapsize=0;
    snapsize=*(int *) buf;
    for (int i=0;i<snapsize;i++)
    {
        read(snapshotfd,(char *)buf,sizeof(char));
        char state_s=*(char *)buf;
        stm_snapshot.push_back(state_s);
    }

    
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
   close(metafd);
   close(logfd);
}

#endif // raft_storage_h