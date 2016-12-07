#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <map>
#include <fstream>
#include <cstdlib>
#include <functional>

using namespace std;

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		int f_indx;							// Partition function mod
		int n_int_files;						// No. of intermediate files ( = R )
		int num_shards;
		vector<string> intermediate_files;				// Path to intermediate files	
		
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
   f_indx 	= 0;								// TODO :Create new dir
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {

	/***************************************************************************************************************************/
	hash<string> hasher;
	f_indx = hasher(key) % (n_int_files*num_shards);
	ofstream myfile (intermediate_files[f_indx].c_str(), std::ofstream::app);
        if (myfile.is_open())
        {
    	  myfile << key << "," << val << endl;
	  //f_indx++;
  	}
	myfile.close();       
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string output_file;							// Path to output file
		
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
		 
}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
 
     ofstream myfile (output_file.c_str(), std::ofstream::app);			// One output file per reducer. 'path' will have the filename
     if (myfile.is_open())
      {
    	myfile << key << "," << val << endl;	
       }	
     myfile.close();	 	
     

}
