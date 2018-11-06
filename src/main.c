
#define DEFAULT_TABLE_LENGTH 50000

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mydb_manager.h"
#include "../archived/hash_table.h"
#include "parse.h"






int main() {
    int pl1[] = {3,4,20,38,10};
 //   int pl2[] = {1,5,9,6,14};
    Result* result = malloc(sizeof(Result));
//    DbOperator* query = malloc(sizeof(DbOperator));
//    query->operator_fields.add_operator.num_tuples= 6;
//    query->operator_fields.add_operator.payload1 = pl1;
//    query->operator_fields.add_operator.payload2 = pl2;
//    result = add(query);
//    DbOperator* query = malloc(sizeof(DbOperator));
//    query->operator_fields.sub_operator.num_tuples= 6;
//    query->operator_fields.sub_operator.payload1 = pl1;
//    query->operator_fields.sub_operator.payload2 = pl2;
//    result = sub(query);
    DbOperator* query = malloc(sizeof(DbOperator));
    query->operator_fields.min_operator.num_tuples= 5;
    query->operator_fields.min_operator.payload = pl1;
//   query->operator_fields.sub_operator.payload2 = pl2;
    result = min(query);
    DbOperator* query2 = malloc(sizeof(DbOperator));
    query2->operator_fields.print_operator.num_tuples = 5;
    query2->operator_fields.print_operator.payload= result->payload;
    char* result2 = print_result(query2);
    printf("%s\n", result2);
    return 0;
}
