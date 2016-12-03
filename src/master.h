#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"
#include <unistd.h>
#include <vector>
#include <cmath>

//grpc client
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

//Masterclient
using masterworker::FileChunk;
using masterworker::MapStatus;
using masterworker::ReduceStatus;
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
	map<string,bool> worker_info;			// Save the worker name and status (0: busy and 1: idle)
	MapReduceSpec spec;				// Map-Reduce specifications
	int n_shards;					// No. of file shards created
	int n_workers;					// No. of active workers
	int n_int_files;				// No. of intermediate files (To be populated by workers)
};


/* CS6210_TASK: This is all the information your master will get from the framework.
You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	spec = mr_spec;

	for(FileShard fs : file_shards){
		shard_names.push_back(fs.sh_name);			 // Get the fileshard names
	}

	for(int j=0; j<mr_spec.worker_ipaddr_ports.size();++j){
		worker_info[mr_spec.worker_ipaddr_ports[j]] = 0;	// Initialize the states of all workers as idle (0:idle 1:busy)
	}

	n_shards    = shard_names.size();				// No. of file shards : M (No of files >= No.of active workers)
	n_workers   = spec.worker_ipaddr_ports.size();			// TODO: Number of workers (active). Verify this during execution
	n_int_files = spec.n_output_files;  				// This is no. of output files to be written by each mapper
}

class MasterClient {
public:
	explicit MasterClient(std::shared_ptr<Channel> channel) : stub_(MasterWorker::NewStub(channel)) {}

	MapStatus DoMap(string filename, int round_no, int n_int_files) {

		// Data we are sending to the server.
		FileChunk request;
		request.set_name(filename);
		request.set_round_no(round_no);
		request.set_n_int_files(n_int_files);
		MapStatus reply;
		ClientContext context;
		CompletionQueue cq;
		Status status;

		std::unique_ptr<ClientAsyncResponseReader<MapStatus> > rpc(stub_->AsyncDoMap(&context, request, &cq));
		cout<<"RPC Initiated ....";
		rpc->Finish(&reply, &status, (void*)1);
		void* got_tag;
		bool ok = false;
		GPR_ASSERT(cq.Next(&got_tag, &ok));
		GPR_ASSERT(got_tag == (void*)1);
		GPR_ASSERT(ok);
		if (status.ok()) {
			cout<<"RPC Done" << endl;
		} else {
			cout<<"RPC failed" << endl;
		}
		return reply;
	}


	bool DoReduce(vector<string> &mr_temp_files) {

		// Data we are sending to the server.
		FileChunk request;
		//request.set_name(filename);
		for(string mr_temp_file : mr_temp_files){
			request.add_temp_files(mr_temp_file);
		}
		ReduceStatus reply;
		ClientContext context;
		CompletionQueue cq;
		Status status;

		std::unique_ptr<ClientAsyncResponseReader<ReduceStatus> > rpc(stub_->AsyncDoReduce(&context, request, &cq));
		cout<<"RPC Initiated for DoReduce ....";
		rpc->Finish(&reply, &status, (void*)1);
		void* got_tag;
		bool ok = false;
		GPR_ASSERT(cq.Next(&got_tag, &ok));
		GPR_ASSERT(got_tag == (void*)1);
		GPR_ASSERT(ok);
		if (status.ok()) {
			cout<<"RPC Done" << endl;
		} else {
			cout<<"RPC failed" << endl;
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

	vector<string> mr_temp_files;					// Vector of intermediate file names. To be received from worker
       	int n_rounds	= (n_shards + n_workers - 1) / n_workers;	// Calculate the total rounds required

	/***********************************************************************************************************************************************************************/
	//MAP PHASE
	/***********************************************************************************************************************************************************************/
	cout << "Starting the Map Task.\nNo. of active workers->" << n_workers << "   |   Total Shards(M)->" << n_shards << "   |   No. of intermediate files per mapper(R)->" << n_int_files << endl;
	for(int round = 0; round < n_rounds ; round++){
		for(int m = 0; m< n_workers ; m++){
			if(round*n_workers+m < n_shards){
				MasterClient masterClient(grpc::CreateChannel(spec.worker_ipaddr_ports[m], grpc::InsecureChannelCredentials()));
				cout<<"Worker "<< spec.worker_ipaddr_ports[m] <<" : ";
				MapStatus reply = masterClient.DoMap(shard_names[round*n_workers + m], round, n_int_files);
				for(int i=0;i<reply.temp_files_size();i++){
					mr_temp_files.push_back(reply.temp_files(i));
				}
			}
		}
	}
	usleep(2*1000000);

	/***********************************************************************************************************************************************************************/
	//REDUCE PHASE
	/***********************************************************************************************************************************************************************/
	cout << "\nMap Task Done. Starting the Reduce Task.\nNo. of active workers->" << n_workers << "   |   No. of intermediate files (M*R)->" << mr_temp_files.size() << endl;
	// Assign R files for every reducer
	// TODO: Error Handling -> Stragglers case
	//n_rounds = (n_shards + n_workers - 1) / n_workers; 			// No. of shards = R output files i.e. No of rounds will be same if active workers are constant 
	vector<string> reduce_files(n_int_files);				// Vector of filenames to be passed to reducer
	int indx_int = 0;		

	for(int round = 0; round < n_rounds; round++){
	   for(int m=0;m<n_workers;m++){
		if(round*n_workers+m < n_shards){
		
			cout << "********************************************************************************************" << endl;
			//Create vector of intermediate filenames to be passed to reducer	
			for(int k=0;k<n_int_files;k++){
			   reduce_files[k] = mr_temp_files[indx_int];
			   cout << "Round:->" << round << "  |  Worker->" << m << "  |  File : " <<  mr_temp_files[indx_int] << endl;
			   indx_int++;		
			}
			MasterClient masterClient(grpc::CreateChannel(spec.worker_ipaddr_ports[m], grpc::InsecureChannelCredentials()));
			cout << "Worker "<< spec.worker_ipaddr_ports[m] <<" : ";
			bool reply = masterClient.DoReduce(reduce_files);
			fill(reduce_files.begin(), reduce_files.end(), 0);
		}
	   }
	}

	/***********************************************************************************************************************************************************************/

return true;
}
