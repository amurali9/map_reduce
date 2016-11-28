#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <map>
#include <fstream>

using namespace std;

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		int n_int_files;						// = R(no. of output files)
		vector<string> int_files;					// List of intermediate files (for mapper)
		std::multimap<string,string> map_result; 			// Result of mapping task
		int f_indx;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
   f_indx = 0;				//TODO :Create new dir	    
   n_int_files = 8;			// Get value from worker	
   string i_name;

   for(int i=0;i<n_int_files;++i){
     i_name = "output/int_tmp_" + to_string(i) + ".txt"; 	// Generate intermediate filenames
     int_files.push_back(i_name);		
   } 				
	
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {

	//map_result.insert(std::pair<string,string>(key, val));			// All the values are stored in map_result. When writing to file, use hash for individual elements
	f_indx = f_indx % 8;
	
	ofstream myfile (int_files[f_indx].c_str(), std::ofstream::app);
        if (myfile.is_open())
        {
    	  myfile << key << "," << val << endl;
	  myfile.close();
  	}
       else cout << "Unable to open file";
       f_indx++;
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
		int n_op_files;
		vector<string> output_files;					// List of final output files (for reducer)
		
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
    n_op_files = 1;			// This has to be one for every reducer worker	
    string o_name;	

   for(int i=0;i<n_op_files;++i){
     o_name = "output/final_" + to_string(i) + ".txt";	// Generate final output names	
     output_files.push_back(o_name);		
   }
	 
}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {

     //TODO : Input key-value pair has to be sorted and key-value pair has to be unique
     ofstream myfile (output_files[0].c_str(), std::ofstream::app);
     if (myfile.is_open())
      {
    	myfile << key << "," << val << endl;
	myfile.close();	
  	}
     else cout << "Unable to open file";
    

}
