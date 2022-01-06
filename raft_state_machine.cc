#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return 1+key.length()+value.length()+8;
    // return 0;
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    switch(cmd_tp)
    {
        case CMD_NONE: buf[0]='a';break;
        case CMD_GET:  buf[0]='b';break;
        case CMD_PUT: buf[0]='c';break;
        case CMD_DEL: buf[0]='d';break;
    }
    int a=key.size();
    int b=value.size();
    memcpy(buf+1,&a,4);
    memcpy(buf+5,key.c_str(),key.size());
    memcpy(buf+5+key.size(),&b,4);
    memcpy(buf+9+key.size(),value.c_str(),value.size());
    // printf("\n serialize key:%s val:%s\n",key.c_str(),value.c_str());

    return;
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    switch(buf[0])
    {
        case 'a': cmd_tp=CMD_NONE;break;
        case 'b':  cmd_tp=CMD_GET;break;
        case 'c': cmd_tp=CMD_PUT;break;
        case 'd': cmd_tp=CMD_DEL;break;
    }
    int a;
    int b;
    memcpy(&a,buf+1,4);
    key.resize(a);
    memcpy(&key[0],buf+5,a);
    memcpy(&b,buf+5+a,4);
    value.resize(b);
    memcpy(&value[0],buf+9+a,b);
    // printf("\ndeserialize key:%s val:%s\n",key.c_str(),value.c_str());
    return;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    int tmp=cmd.cmd_tp;
    m<<tmp;
    m<<cmd.key;
    m<<cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    int tmp;
    
    u>>tmp;
    u>>cmd.key;
    u>>cmd.value;
    cmd.cmd_tp=(kv_command::command_type)tmp;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    
    switch(kv_cmd.cmd_tp)
    {
        case kv_command::CMD_NONE:
        {
            kv_cmd.res->succ=true;
            return;
        }
        break;
        case kv_command::CMD_GET:
        {
            auto iter=dataBase.find(kv_cmd.key);
            if (iter==dataBase.end())
            {
                kv_cmd.res->succ=false;
                kv_cmd.res->key=kv_cmd.key;
                kv_cmd.res->value="";
            }
            else{
                 kv_cmd.res->succ=true;
                kv_cmd.res->key=kv_cmd.key;
                kv_cmd.res->value=iter->second;
            }

        }
        break;
        case kv_command::CMD_PUT:
        {
            auto iter=dataBase.find(kv_cmd.key);
            if (iter==dataBase.end())
            {
                kv_cmd.res->succ=true;
                kv_cmd.res->key=kv_cmd.key;
                kv_cmd.res->value=kv_cmd.value;
                dataBase.insert(std::pair<std::string, std::string>(kv_cmd.key, kv_cmd.value));
            }
            else{
                kv_cmd.res->succ=false;
                kv_cmd.res->key=kv_cmd.key;
                kv_cmd.res->value=iter->second;
                dataBase.insert(std::pair<std::string, std::string>(kv_cmd.key, kv_cmd.value));
            }
        }
        break;
        case kv_command::CMD_DEL:
        {
             auto iter=dataBase.find(kv_cmd.key);
            if (iter==dataBase.end())
            {
                kv_cmd.res->succ=false;
                kv_cmd.res->key=kv_cmd.key;
                kv_cmd.res->value="";
                
            }
            else{
                kv_cmd.res->succ=true;
                kv_cmd.res->key=kv_cmd.key;
                kv_cmd.res->value=iter->second;
                dataBase.erase(kv_cmd.res->key=kv_cmd.key);
            }
        }
    }
    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    std::vector<char> tmp;
    
    for(auto item =dataBase.begin();item!=dataBase.end();item++)
{
     auto key= item->first;
     auto value= item->second;
     
     for (int i=0;i<key.size();i++)
     {
         tmp.push_back(key[i]);
     }
     tmp.push_back('%');
      for (int i=0;i<value.size();i++)
     {
         tmp.push_back(value[i]);
     }
     tmp.push_back('%');
     
     
}

    return tmp;
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
   
    dataBase.clear();
    int cnt=0;
    int idx=0;
    while (idx<snapshot.size())
    {
        std::string key;
        std::string value;
        while (snapshot[idx]!='%')
        {
            key+=snapshot[idx];
            idx++;
        }
        idx++;
         while (snapshot[idx]!='%')
        {
            value+=snapshot[idx];
            idx++;
        }
        idx++;
        dataBase.insert(std::pair<std::string, std::string>(key,value));


    }
    
    return;    
}
