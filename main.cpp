#include <iostream>
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
/**
 *
 * Name: Kaiser Vanguardia
 *     : Yushu Chen
 *
 * Filename: sql.cpp
 * Purpose: The purpose of this file is to SQL Parse the
 * SQL statements inputted by the user.
 */
int main(){


    cout << endl;
    string ans;
    const char* cc;
    //string inputQuery = argv[1];
    hsql::SQLParserResult *result;
    int trialInputSQL = 10;
    int querySIZE = 100;
    string queryInputArr[querySIZE];
    for (int i = 0; i < trialInputSQL; i++) {

        cout << "SQL> ";
        getline(cin,ans);

        if (ans == "quit") {
            return 1;
        }

        cout << ans << endl;
        cc = ans.c_str();

        result = hsql::SQLParser::parseSQLString(cc);
        if (!result->isValid()) {
            delete result;
            continue;
        }
        printStatementInfo(result->getStatement(i));


    }

}