#pragma once

#include <vector>
#include <map>
#include "mapreduce_spec.h"


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
   map<string,vector<int>> filemap;		//STL Map for File Split : filename + Offset(s)
   vector<string> ip_filenames;			//Input filenames
   int map_kB;
   int n_fileshard;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {

	return true;
}
