// Ryan Bagby
// Lakshmi Priyanka Selvaraj


#include <chrono>
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <string>

using namespace std;
using namespace std::chrono;
using namespace std::this_thread;

mutex mtx;

queue<long> *q_lcr;
queue<pair<long,int>> *q;           // This array of queues stores messages from neighbors. 
set<int> *neighbors;                // Set of edges between nodes.

bool round_completion = false;      // Used if n rounds are completed.
int go_ahead_signal   = 0;
int num_msgs          = 0;
int num_threads;
int round_no;
int delay;
int *num_nghbrs;


std::random_device rd;                                  // Obtains a seed for the random number engine.
std::mt19937 gen(rd());                                 // Standard mersenne_twister_engine seeded with rd().
std::uniform_int_distribution<> distrib(1, 12);



void asynch_lcr(long threadid, int thread_num) {
    long max      = threadid;
    bool push_max = true;
    int rounds    = num_threads;
    int op_t_id   = (thread_num+1) % num_threads;
    long incoming;

    
    while (rounds > 0) {
        delay = distrib(gen);                                   // Generates a random number from the range 1-12
        sleep_for(nanoseconds(delay));                          // Random number delays pushing value to output queue.
        if (push_max) {
            q_lcr[op_t_id].push(max);                           // If a new max is found, thread outputs the new maximum.
            num_msgs++;
        }

        else {
            q_lcr[op_t_id].push(0);
            num_msgs++;
        }
        push_max = false;
        
        while(q_lcr[thread_num].size() == 0) {
            sleep_for(milliseconds(10));
        }

        for (int h = 0; h < q_lcr[thread_num].size(); h++) {                // Retrieve elements from the input queue.
            incoming = q_lcr[thread_num].front();
            
            if (incoming > threadid) {
                max      = incoming;                                    // New maximum found from the input queue.
                push_max = true;
            }
            q_lcr[thread_num].pop();
        }
        rounds--;                                                       // Each thread runs n rounds asynchronously. 
    }
    mtx.lock();
    cout << "My ID is " << threadid << " and my leader is " << max << endl ;    
    mtx.unlock();
}


void asynch_floodmax(long threadid, int thread_num) {
    pair<long,int> incoming;
    long msg_id        = threadid;
    long max_id        = threadid;
    int num_nghbr_msgs = 0;
    int round_num      = 0;
    int r;
	string status      = " ";
	int parent = 0;
	boolean leaf_node = true;
	boolean terminate = false;
	
	
	

    while (round_num < num_threads) {

        for (int j = 0; j < num_threads; j++) {
            if (thread_num != j && neighbors[thread_num].find(j) != neighbors[thread_num].end()) {
                mtx.lock();
                q[j].push(make_pair(max_id, round_num));
                num_msgs++;
                mtx.unlock();
            }
        }
        delay = distrib(gen);
        sleep_for(milliseconds(delay));

        while (num_nghbr_msgs < num_nghbrs[thread_num]) {

            while (q[thread_num].empty()) {
                sleep_for(nanoseconds(5));
            }
            mtx.lock();
            incoming = q[thread_num].front();
            r        = get<1>(incoming);
            msg_id   = get<0>(incoming);
            mtx.unlock();

            while (r != round_num) {          // Only look at messages from neighbors in current round.
                mtx.lock();
                q[thread_num].push(incoming);
                q[thread_num].pop();

                incoming = q[thread_num].front();
                r        = get<1>(incoming);
                msg_id   = get<0>(incoming);
                mtx.unlock();
            }
            if (msg_id > max_id) {
                max_id = msg_id;
            }
            
            mtx.lock();
            q[thread_num].pop();
            mtx.unlock();

            num_nghbr_msgs++;
        }
        round_num++;
        num_nghbr_msgs = 0;                          
    }
    
    if(threadid == max_id){
    	status = "Leader";
	}
	else
	{
		status = "Non-Leader";
	}
	//Part 1 send broadcast
	if (status!="Leader")
	{
		//All non leader processes receive the broadcast message & process also send broadcast to my neighbors minus parent
		// Leader process just sends the broadcast out
		while(q[thread_num].empty())
		{
			sleep_for(nanoseconds(5));
		}
		//considers the message it receives first as its parent
		mtx.lock();
		incoming = q[thread_num].front();
		max_id       = get<0>(incoming);
		parent       = get<1>(incoming);
		q[thread_num].pop();
		mtx.unlock();	
	}
	for(int j=0; j< num_threads;j++){
		//Please check if the third condition is correct, we want to exclude the paren from our brpadcasting list
		if(thread_num!=j && neighbors[thread_num].find(j) != neighbors[thread_num].end() && j != parent) 
		{
			//send broadcast
			mtx.lock();
			q[j].push(make_pair(max_id, j));
			mtx.unlock();
			leaf_node = false;
		}
	}
	while(!q[thread_num].empty()) q[thread_num].pop(); //clear the queue for processing acks
									
	//receive acks from my children and send acks to my parent
	if(!leaf_node)
	{	
		while(q[thread_num].empty())
		{
			sleep_for(nanoseconds(5));
		}
		//process all messages from all my children
		for(int j=0; j<q[thread_num].size();j++)
		{
			mtx.lock();
   			incoming = q[thread_num].front();
    		r        = get<0>(incoming); //ack
    		msg_id   = get<1>(incoming); //thread_no
    		mtx.unlock();
		}
		// receive acks from my children and push all to my parent queue
	}	
	//if non-empty parent
	//push my ack into my parent queue terminate = true;
	//i was thinking of pushing 0 for ack, we push a message of type <long, int> so i was thinking we can't send a string like 'ack' ??? *Thinking out loud*
	
	//if empty parent(leader) wait for all acks, check the number and terminate
         	   	
    mtx.lock();
    cout << "I am Thread ID " << threadid << " and I am a "<<status<<endl;
    mtx.unlock();
}


int main(int argc, char* argv[]) {
    long i = 0;
    int data;
    int c;
    bool lcr = false;


    ifstream infile;
    infile.open("connectivity.txt");                                   // This file contains the input data.
    infile >> data;                                             // This initial data contains the number of threads.
    num_threads = data;

    int thread_ids[num_threads] = {};                           // Contains ID's of the processes.
    int data_collector[num_threads*(num_threads+1)] = {};       // Temporary array to collect data from file.
    neighbors = new set<int>[num_threads];                      // Set of edges between nodes.

    while (!infile.eof()) {                                     // This collects all remaining data: process id's and connectivity.
        infile >> data;
        data_collector[i++] = data;
    }
    infile.close();
    num_nghbrs = new int[num_threads];
    for (i = 0; i < num_threads; i++) {
        num_nghbrs[i] = 0; 
    }

    c = 0;
    for (int k = 0; k < num_threads+1; k++) {            // Sort out id's from the connectivity data.
        for (int j = 0; j < num_threads; j++) {
            if (k == 0) {
                thread_ids[j] = data_collector[j];
            }
            else {
                if (data_collector[c] == 1) {
                    neighbors[k-1].insert(j);
                    num_nghbrs[k-1]++;
                }
            }
            c++;
        }
    }
    q           = new queue<pair<long,int>>[num_threads];
    q_lcr       = new queue<long>[num_threads];
    
    thread threads[num_threads];
    if (argv[1] == "lcr") {
        for (i = 0; i < num_threads; i++) {                     // Create threads.
            threads[i] = thread(asynch_lcr, thread_ids[i], i);
        }
    }
    else {
        for (i = 0; i < num_threads; i++) {                     // Create threads.
            threads[i] = thread(asynch_floodmax, thread_ids[i], i);
        }
    }
    for (i = 0; i < num_threads; i++) {
        threads[i].join();
    }
    cout << endl << "Total messages sent = " << num_msgs << endl;

   return 0;
}
