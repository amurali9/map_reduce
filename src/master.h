#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include <grpc++/grpc++.h>
using namespace std;
#include "masterworker.grpc.pb.h"

//grpc client
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

//Masterclient
using masterworker::FileChunk;
using masterworker::MapStatus;
using masterworker::Empty;
using masterworker::WorkerStatus;
using masterworker::MasterWorker;


using namespace std;
/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		vector<string> shard_names;
		MapReduceSpec spec;
		map<string,bool> worker_info;	// Save the worker name and status (0: busy and 1: idle)
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	spec = mr_spec;
	for(FileShard fs : file_shards){
		shard_names.push_back(fs.sh_name);
  }

   // Get the fileshard names
   for(FileShard fs : file_shards){
	shard_names.push_back(fs.sh_name);
   }

   for(int j=0; j<mr_spec.worker_ipaddr_ports.size();++j){
  	worker_info[mr_spec.worker_ipaddr_ports[j]] = 0;	// Initialize the states of all workers as idle (0:idle 1:busy)
   }

}

class MasterClient {
 public:
  explicit MasterClient(std::shared_ptr<Channel> channel)
      : stub_(MasterWorker::NewStub(channel)) {}

  bool DoMap(string filename) {

    // Data we are sending to the server.
    FileChunk request;
    request.set_name(filename);
    MapStatus reply;
    ClientContext context;
    CompletionQueue cq;
    Status status;

    std::unique_ptr<ClientAsyncResponseReader<MapStatus> > rpc(
        stub_->AsyncDoMap(&context, request, &cq));
		cout<<"RPC Initiated"<<endl;
    rpc->Finish(&reply, &status, (void*)1);
    void* got_tag;
    bool ok = false;
    GPR_ASSERT(cq.Next(&got_tag, &ok));
    GPR_ASSERT(got_tag == (void*)1);
    GPR_ASSERT(ok);
    if (status.ok()) {
      cout<<"RPC Done"<<endl;
    } else {
      cout<<"RPC failed"<<endl;
    }
      return status.ok();
  }

	bool CheckWorkerStatus() {
    // Data we are sending to the server.
    Empty request;
    // request.set_name(filename);
    WorkerStatus reply;
    ClientContext context;
    CompletionQueue cq;
    Status status;

    std::unique_ptr<ClientAsyncResponseReader<WorkerStatus> > rpc(
        stub_->AsyncCheckStatus(&context, request, &cq));

    rpc->Finish(&reply, &status, (void*)1);
    void* got_tag;
    bool ok = false;
    GPR_ASSERT(cq.Next(&got_tag, &ok));
    GPR_ASSERT(got_tag == (void*)1);
    GPR_ASSERT(ok);
    if (status.ok()) {
      cout<<"Worker Status check RPC Done"<<endl;
			return reply.worker_status();
    } else {
			cout<<"Worker Status check RPC failed"<<endl;
			return false;
    }
  }
 private:
  std::unique_ptr<MasterWorker::Stub> stub_;
};


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

	//make rpc call to run the worker
	for(string worker_ipaddr_port : spec.worker_ipaddr_ports){
		cout<<"Port "<<worker_ipaddr_port<<endl;
		MasterClient masterClient(grpc::CreateChannel(worker_ipaddr_port, grpc::InsecureChannelCredentials()));
		cout<<"Requesting worker "<<worker_ipaddr_port<<" to domap"<<endl;
		bool reply = masterClient.DoMap(shard_names[0]);
	}

	//make rpc call to run the worker. Create a grpc channel between the master and all the workers
	vector<MasterClient> masterClient;
	for(auto const& entry : worker_info){
	   masterClient.emplace_back(grpc::CreateChannel(entry.first, grpc::InsecureChannelCredentials()));		// Key : entry.first  -- > Value : entry.second
	}

	//1. If DoMap stage, assign work to individual workers and set their flags appropriately
	//2. Record when all the workers are done
	//3. Move to Reduce stage
	//4. Repeat steps 1-2
	//5. Clean up (Remove temporary files)

	bool reply = masterClient[0].DoMap(shard_names[0]);


	return true;
}
