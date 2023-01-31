#include <iostream>
#include <string>
#include "db_cxx.h"
using namespace std;
/**
 *
 * Name: Kaiser Vanguardia
 *     : Yushu Chen
 * Filename: milstone1.cpp
 *
 * Purpose: The purpose of this program is to provide the
 * interface for SQL for milestone one. As the interface
 * is provided, the program then connects to a data base
 * in order to begin the database environment for the user
 * to be able to create a database.
 *
 * @return
 */


int main(int argc, char *argv[]) {

    const char *HOME = "/cpsc5300/milestone1";
    const char *DATABASE1 = "database1.db";
    const unsigned int BLOCKSIZE = 4096;

    //String array for each input
    vector<string> answer(100);
    string userInput;
    string userAnswer;

    cout << "Has a directory been made? ";
    cin >> userAnswer;
    if (userAnswer[0] != 'y') {
        cout << "A directory has been made..." << endl;
        return 1;
    }
    const char *home = getenv("HOME");
    string envDirectory = string(home) + "/" + HOME;

    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_message_stream(&cerr);
    env.open(envDirectory.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    cout << "A directory has been made already.. " << endl;

    string ans;

    return 0;
}
