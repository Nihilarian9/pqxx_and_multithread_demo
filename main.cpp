/*
*	A simple program to test my understanding of
*	postgreSQL API pqxx and multithreading
*	before implementing them into a broader project.
*
*	Copyright 2018 Nihilarian9 github. Licensed under GPL v2
*	https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
*/


#include <iostream>
#include <thread>
#include <string>
#include <fstream>
#include <pqxx/pqxx>

//	declare functions
static void threading(int tID);
static void avgThreading(int tID);
static void createTickerVec();
static void printTables();

//	struct for holding daily trade info
struct tickInfo {
	std::string date;
	float close;
};

//	variables
static int totalData = 30;
static unsigned int num_cpus, tickerCounter;

//	vector for holding company tickers
std::vector<std::string> tickerNames;


//////////////////////
//	Main Function	//
//////////////////////
int main(int argc, const char *argv[]) {

	//	run function to fill tickerNames with company tickers
	createTickerVec();

	num_cpus = std::thread::hardware_concurrency();
	std::thread cThreads[num_cpus];
	for(int i = 0; i < num_cpus; ++i)
	{
		cThreads[i] = std::thread(threading, i);
	}
	for (int i = 0; i < num_cpus; ++i)
	{
		cThreads[i].join();
	}
	return 0;
}

//	retreives tables from database. Can be added to array
static void printTables() {
	pqxx::connection NC("dbname = test user = postgres password = pass hostaddr = 127.0.0.1 port = 5432");
	std::vector<std::string> tableNames;
	std::string getTables = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';";
	unsigned int counter = 0;

	pqxx::nontransaction N(NC);
	pqxx::result R (N.exec(getTables.c_str()));
	for(pqxx::result::const_iterator c = R.begin(); c != R.end(); ++c) {
		std::cout << counter << " = " << pqxx::to_string(c[0]) << std::endl;
		counter++;
	}
}

//	multithread callback funciton
static void threading(int tID) {
    int start = (totalData / num_cpus) * tID;
    int end = (totalData / num_cpus) * (tID + 1);
    if(tID == num_cpus - 1) {
        end = totalData;
    }
	unsigned int threadWork = 0;
	tickerCounter = 0;

    pqxx::connection C("dbname = test user = postgres password = pass hostaddr = 127.0.0.1 port = 5432");
    std::string cInsert, cTable;

    for(int i = start; i < end; ++i) {
        std::string ticker = tickerNames[i];
        std::string tickerPath = "/home/hex/Projects/Stocks/Profiles/" + ticker;

        //  Create table
        cTable =    std::string("CREATE TABLE t_" + ticker) +
                    std::string("(DATE      INT     PRIMARY KEY     NOT NULL,") +
                    std::string("OPEN       REAL    NOT NULL,") +
                    std::string("HIGH       REAL    NOT NULL,") +
                    std::string("LOW        REAL    NOT NULL,") +
                    std::string("CLOSE      REAL    NOT NULL,") +
                    std::string("VOLUME     INT     NOT NULL);");

        pqxx::work WC(C);
        WC.exec(cTable.c_str());
        WC.commit();

        std::cout << "Processing " << ticker << ": " << tickerCounter << " of " << totalData << std::endl;
        //  insert database
        std::string str1;
        std::ifstream readData(tickerPath.c_str());
        while(getline(readData, str1))
        {
            cInsert =   std::string("INSERT INTO t_" + ticker) +
                        std::string("(DATE,OPEN,HIGH,LOW,CLOSE,VOLUME) ") +
                        std::string("VALUES(" + str1 + ");");

            pqxx::work WI(C);
            WI.exec(cInsert.c_str());
            WI.commit();
			threadWork++;
        }
        tickerCounter++;
    }
	std::cout << "Thread " << tID << " completed with " << threadWork << " updates" << std::endl;
}

//	creates vector containing company ticker names
static void createTickerVec() {
	std::string tickerNamesPath = "/home/hex/Projects/Stocks/temp/tickers";

	//  load filenames ready for loading files
	std::ifstream inTickerNames(tickerNamesPath.c_str());
	std::string tickerStr, tickerSubStr;
	while(getline(inTickerNames, tickerStr))
	{
		std::stringstream tickerSS;
		tickerSS << tickerStr;

		while(tickerSS.good())
		{
			getline(tickerSS, tickerSubStr, ',');
			tickerNames.push_back(tickerSubStr);
		}
	}
}
