#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
using namespace std;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
     int n_output_files;
     int n_workers;
     int map_kilobytes;

     string output_dir;
     string user_id;

     vector<string> worker_ipaddr_ports;
     vector<string> input_files;
};

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const string& config_filename, MapReduceSpec& mr_spec) {

/*Read the config file and extract the MR spec */
ifstream is_file;
is_file.open(config_filename);
string line;

while( getline(is_file, line) )
{
  istringstream is_line(line);
  string key;

  if(getline(is_line, key, '=') )
  {
    string value;
    if(getline(is_line, value) ){

	if(key.compare("n_output_files") == 0) mr_spec.n_output_files = stoi(value);
	if(key.compare("n_workers") == 0) mr_spec.n_workers = stoi(value);
	if(key.compare("map_kilobytes") == 0) mr_spec.map_kilobytes = (stoi(value)*1024);

	if(key.compare("output_dir") == 0) mr_spec.output_dir = value;
	if(key.compare("user_id") == 0) mr_spec.user_id = value;

	if(key.compare("worker_ipaddr_ports") == 0) {
	       stringstream ss1(value);
	       string tok1;

	   while(getline(ss1,tok1,',')){
		mr_spec.worker_ipaddr_ports.push_back(tok1);
	   }
	}

	if(key.compare("input_files") == 0){
	      stringstream ss(value);
  	      string tok;

	   while(getline(ss, tok, ',')) {
    		mr_spec.input_files.push_back(tok);
  	    }
	}


     }
  }

}
	return true;
}

inline bool file_exists (const std::string& name) {
  struct stat buffer;
  return (stat (name.c_str(), &buffer) == 0);
}

/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
  if(mr_spec.n_output_files <= 0 || mr_spec.n_workers <=0 || mr_spec.map_kilobytes <= 0){
    std::cout<<"Enter correct values for n_output_files, n_workers and map_kilobytes"<<std::endl;
    return false;
  }
  for(string filename : mr_spec.input_files){
    if(!file_exists(filename)){
      cout<<"Cannot access "<<filename<<endl;
      return false;
    }
  }
  struct stat validinfo;
  if(stat(mr_spec.output_dir.c_str(), &validinfo ) != 0 ){
    cout<<"cannot access output_dir"<<endl;
    return false;
  }
  if(!(validinfo.st_mode & S_IFDIR )){
    cout<<"Given output_dir is not a dir"<<endl;
    return false;
  }
	return true;
}
