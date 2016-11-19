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
struct FileShard {			
   string sh_name;   		 
};

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
      
 	// Collect information about the input files
	int n_ip_files = mr_spec.input_files.size();
        int maxSize    = mr_spec.map_kilobytes; 
        int result     = 0;
        FILE *fIn;
        FILE *fOut;   
        string buffer;
	size_t size;
        char ch;
        const char* bname = "input/tmp_shard_";	 

    // File sharding	
    /********************************************************************************************************************/
        
for(int m =0 ; m<n_ip_files; ++m){
const char *fileIn = mr_spec.input_files[m].c_str();

    if ((fileIn != NULL) && (maxSize > 0))
    {
        fIn = fopen(fileIn, "r");
        if (fIn != NULL)
        {
            if(m == 0){ 
		fOut = NULL;
            	result = 1;
	     }   /* we have at least one part. First run */

            while (!feof(fIn))
            {		
                /* initialize (next) output file if no output file opened */
                if (fOut == NULL)
                {   
		    buffer = bname + to_string(result) + ".txt";		// Dynamically generate the filenames
		    fileShards.push_back({buffer});
		    fOut = fopen(buffer.c_str(), "wa");
                    if (fOut == NULL)
                    {
                        result *= -1;
                        break;
                    }
                    size = 0;
                }

                /* calculate size of data to be read from input file in order to not exceed maxSize */
		ch = fgetc(fIn);
		if(ch == EOF){ 		// This is where the file ends and the write for new file starts. Do bookkeeping
		   break;	
		}else if(size > maxSize && ch == '\n'){
		   size = 0;
		   fclose(fOut);
		   fOut = NULL;
                   result++;		
		}else{
		   fputc(ch,fOut);
		   size += 1;
		}

            } // End of while loop
            fclose(fIn);
        }
    }
}    


/* clean up */
if (fOut != NULL)
{
   fclose(fOut);
}

/********************************************************************************************************************/

	return true;
}

