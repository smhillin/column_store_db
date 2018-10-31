
#define DEFAULT_TABLE_LENGTH 50000

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mydb_manager.h"
#include "../archived/hash_table.h"
#include "parse.h"






int main() {
    load_table("/Users/Shaun/CLionProjects/final_project/cs165-2018-base/project_tests/data2.csv");
    save_db();
    return 0;
}
