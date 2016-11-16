#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include <sys/stat.h>
#include <cmath>
#include <map>
#include <set>
#include <fstream>

using namespace std;

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {				// Information about every file split
     multiset<string, vector<int>> f_info;		 
};

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
      
 	// Collect information about the input files
	int n_ip_files = mr_spec.input_files.size();
        int maxSize = mr_spec.map_kilobytes; 
        int result = 0;
        FILE *fIn;
        FILE *fOut;   
        char buffer[200];
        
        size_t size;
        char ch;

    // File sharding	
    /********************************************************************************************************************/
        
for(int m =0 ; m<n_ip_files; ++m){
const char *fileIn = mr_spec.input_files[m].c_str();

    if ((fileIn != NULL) && (maxSize > 0))
    {
        fIn = fopen(fileIn, "r");
        if (fIn != NULL)
        {
            fOut = NULL;
            result = 1;   /* we have at least one part */

            while (!feof(fIn))
            {
		
                /* initialize (next) output file if no output file opened */
                if (fOut == NULL)
                {
                    sprintf(buffer, "%s.%03d", fileIn, result);
                    fOut = fopen(buffer, "w");
                    if (fOut == NULL)
                    {
                        result *= -1;
                        break;
                    }
                    size = 0;
                }

                /* calculate size of data to be read from input file in order to not exceed maxSize */
		ch = fgetc(fIn);
		if(ch == EOF){
		   break;	
		}else{
		if(size > maxSize && ch == '\n' && m != n_ip_files){
		   cout << size << "--" << endl;
		   size = 0;
		   fclose(fOut);
                   fOut = NULL;
                   result++;		
		}else{
		   fputc(ch,fOut);
		   size += 1;
		}
	     }

		
            }

            /* clean up */
            if (fOut != NULL)
            {
                fclose(fOut);
            }
            fclose(fIn);
        }
    }
}    

/********************************************************************************************************************/

	return true;
}

