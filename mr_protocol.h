#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab2: Your definition here.
		mr_tasktype tasktype;
		int index;
		string filenames;
	};

	struct AskTaskRequest {
		// Lab2: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab2: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab2: Your definition here.
	};

};
marshall &operator<<(marshall &m,mr_protocol::AskTaskResponse reply)
{
	if (reply.tasktype==mr_tasktype::NONE)
	{
		unsigned n=0;
		m<<n;
	}
	else if (reply.tasktype==mr_tasktype::MAP)
	{
		unsigned n=1;
		m<<n;
	}
	else if (reply.tasktype==mr_tasktype::REDUCE)
	{
		unsigned n=2;
		m<<n;
	}
	m<<reply.index;
	m<<reply.filenames;
	return m;
}
 unmarshall & operator>>(unmarshall &u,mr_protocol::AskTaskResponse &reply)
{
	unsigned t;
	u>>t;
	int index;
	u>>index;
	std::string filenames;
	u>>filenames;
	if (t==0)
	{
		reply.tasktype=mr_tasktype::NONE;
		}
		else if (t==1)
		{
			reply.tasktype=mr_tasktype::MAP;
		}
		else if (t==2)
		{
			reply.tasktype=mr_tasktype::REDUCE;
		}
	reply.index=index;
	reply.filenames=filenames;
	return u;
}

#endif

