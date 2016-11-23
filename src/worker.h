#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include <grpc++/grpc++.h>

#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using masterworker::FileChunk;
using masterworker::MapStatus;
using masterworker::MasterWorker;

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::string ip_addr_port;
};

class WorkerService final : public MasterWorker::Service {

  public:
    WorkerService(const std::string& server_address) : id_("Worker_" + server_address) {}

  private:
    Status DoMap(ServerContext* context, const FileChunk* request,
                    MapStatus* reply) override {
			std::cout<<"Doing map"<<std::endl;
			reply->set_map_status(true);
      reply->set_worker_id(id_);
      return Status::OK;
    }
    const std::string id_;
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	this->ip_addr_port = ip_addr_port;
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
	BaseReduer's member BaseReducerInternal impl_ directly,
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 4 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */

		//grpc server

		std::cout<<"Attempting to run WorkerServer"<<std::endl;
		ServerBuilder builder;
		WorkerService service(this->ip_addr_port);
		builder.AddListeningPort(this->ip_addr_port, grpc::InsecureServerCredentials());
  	builder.RegisterService(&service);

   	std::unique_ptr<Server> server(builder.BuildAndStart());
  	std::cout << "Server listening on " << this->ip_addr_port << std::endl;

  	server->Wait();


	std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	/*  Below 4 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */
	auto mapper = get_mapper_from_task_factory("cs6210");
	mapper->map("some_input_map");
	auto reducer = get_reducer_from_task_factory("cs6210");
	reducer->reduce("some_input_key_reduce", std::vector<std::string>({"some_input_vals_reduce"}));
	return true;
}
