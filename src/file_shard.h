#pragma once

#include <vector>
#include <set>
#include "mapreduce_spec.h"
#include <sys/stat.h>
#include <cmath>

using namespace std;

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

struct FileShard {				// Information about every file split
		 
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
      
 	// Collect information about the input files
	int n_ip_files = mr_spec.input_files.size();   
	struct stat st;
	double tot_size;

    	for(int i=0; i< n_ip_files; ++i){
	   stat(mr_spec.input_files[i].c_str(), &st);	// Get the individual file size
   	   tot_size +=  ((st.st_size)/1024.0);		// in kB
	}
	
        int nshards = ceil(tot_size/mr_spec.map_kilobytes);
        cout << "Total Size : " << tot_size << "   N_shards : " << nshards << endl;


	// Start splitting the files and save the offset information in the vector


	return true;
}

