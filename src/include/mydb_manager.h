//
// Created by shaun hillin on 10/8/18.
//Gsalhab98!
//

#ifndef FINAL_PROJECT_MYDB_MANAGER_H
#define FINAL_PROJECT_MYDB_MANAGER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DB_FILE "/Users/Shaun/CLionProjects/final_project/project_test2/data/repo.csv"  //persistant store
#define MAX_NUM_TABLE 5

#define MAX_COL_NUM 10

#define INIT_COL_SIZE 1000

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64

#define MAX_NUM_POOL_VAR 20

#define DB_NAME "db1"




/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
    INT,
    LONG,
    FLOAT
} DataType;


typedef struct Column {
    char name[MAX_SIZE_NAME];
    int* data;
    // You will implement column indexes later.
    void* index;
    int col_size; //number of ints in the column
    //struct ColumnIndex *index;
    //bool clustered;
} Column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/


typedef struct Table {
    char name[MAX_SIZE_NAME];
    Column *columns;
    size_t col_count;
    size_t table_length;
} Table;


/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/


typedef struct Db {
    char name[MAX_SIZE_NAME];
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;



/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
    /* The operation completed successfully */
            OK,
    /* There was an error with the call. */
            ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;


/*
 * Declares the type of a result column,
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void* payload;
} Result;

/*
 * Variable pool for temporary global
 */

typedef  struct PooledVar {
    char name[MAX_SIZE_NAME];
    Result* result;
} PooledVar;


typedef struct VarPool{
    PooledVar* pooledVar;
    int pool_size;
} VarPool;

typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;

/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;


/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;


/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;

/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;


/*
 * tells the database what type of operator this is
 */

typedef enum OperatorType {
    CREATE,
    INSERT,
    SELECT,
    FETCH,
    PRINT,
    AVG,
    SUM,
    ADD,
    SUB,
    MAX,
    MIN,
    LOAD,
    SHUTDOWN,
} OperatorType;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
    int num_values;
} InsertOperator;
/*
 * necessary fields for insertion
 */
typedef struct LoadOperator {
    char* file_name;
} LoadOperator;

/*
 * neccesary field for select
 */
typedef struct SelectOperator {
    Column* col;
    Table* table;
    char* low;
    char* high;
    char* pooledVar;
} SelectOperator;

/*
 * neccesary field for fetch
 */
typedef struct FetchOperator {
    Column* col;
    Table* table;
    int* pos;
    int num_tuples;
    char* pooledVar;
} FetchOperator;

typedef struct PrintOperator {
    size_t num_tuples;
    DataType data_type;
    void* payload;
} PrintOperator;

typedef struct AvgOperator {
    size_t num_tuples;
    DataType data_type;
    int* payload;
    char* pooledVar;
} AvgOperator;

typedef struct SumOperator {
    size_t num_tuples;
    DataType data_type;
    int* payload;
    char* pooledVar;
} SumOperator;


typedef struct AddOperator {
    size_t num_tuples;
    DataType data_type;
    int* payload1;
    int* payload2;
    char* pooledVar;
} AddOperator;

typedef struct SubOperator {
    size_t num_tuples;
    DataType data_type;
    int* payload1;
    int* payload2;
    char* pooledVar;
} SubOperator;

typedef struct MaxOperator {
    size_t num_tuples;
    DataType data_type;
    int* payload;
    char* pooledVar;
} MaxOperator;

typedef struct MinOperator {
    size_t num_tuples;
    DataType data_type;
    int* payload;
    char* pooledVar;
} MinOperator;

/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */

typedef union OperatorFields {
    InsertOperator insert_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    AvgOperator avg_operator;
    SumOperator sum_operator;
    AddOperator add_operator;
    SubOperator sub_operator;
    MaxOperator max_operator;
    MinOperator min_operator;
} OperatorFields;

typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;





//DB Methods------------------------



/*
 * Function creates a database
 */


Status create_pool();

Status create_db(char *db_name);


/*
 * Creates a table in an existing database
 */

Status* create_table(char* table_name, Db *db,  int num_col, Status *ret_status);

/*
 * Function creates columns in an existing table
 *
 */
Status* create_col(char* col_name, Table* tb, Status *ret_status);


/*
 * Given a table name fetches it from the db.
 */

Table* fetch_table(char* table_name);

/*
 * Given a column insert a number into that column
 */

void insert_col(Column *col, int num);

/*
 * Select statement that chooses which select function to use and then returns a result
 */
Result* select_val(DbOperator* query);

/*
 * Given a position vector return the Result containeing the tuples
 */

Result* fetch(DbOperator* query);


Result* fetch_poolvar(char* name);

/*
 * Given a table and column name will return column from a table
 */

Column* fetch_column(Table *tb, char* col_name);

/*
 * Insert a row of data in a column store table
 */

void relational_insert(DbOperator* query);
/*
 * Deletes all columns from a table
 */

void delete_columns(Table *table);

int store_var(char* var_name, Result* result);

char* print_result(DbOperator* query);

Result* average_col(DbOperator* query);

Result* sum_col(DbOperator* query);

Result* add(DbOperator* query);

Result* sub(DbOperator* query);

Result* max(DbOperator* query);

Result* min(DbOperator* query);



#endif //FINAL_PROJECT_MYDB_MANAGER_H