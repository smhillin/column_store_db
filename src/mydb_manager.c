//
// Created by shaun hillin on 10/8/18.
//




#include "mydb_manager.h"
#include "client_context.h"
#include "utils.h"



// In this class, there will always be only one active database at a time
Db *current_db;


//Variable Pool for temporary arrays




VarPool *varPool;



Status create_pool(){
    varPool = malloc(sizeof(VarPool));
    varPool->pooledVar = malloc(sizeof(PooledVar)*MAX_NUM_POOL_VAR);
    varPool->pool_size = 0;
    struct Status ret_status;
    ret_status.code = OK;
    return ret_status;
}



Status create_db(char *db_name) {
    struct Status ret_status;
    if(current_db != NULL){
        printf("DB already exist");
        ret_status.code = OK;
    } else {
        current_db = malloc(sizeof(Db));  //allocate memory for DB
        strcpy(current_db->name, db_name);   //name of the DB
        current_db->tables_size = 0; //size of the array holding the tables
        current_db->tables = calloc(sizeof(Table), MAX_NUM_TABLE);
        current_db->tables_capacity = 5;  //amount of pointers that can be held in the currently allocated mem slot
        cs165_log(stdout, "%s - has been created\n", current_db->name);
        ret_status.code = OK;
    }
    return ret_status;

}

Status* create_table(char* table_name, Db *db,  int num_col, Status *ret_status) {
    Table *tb = malloc(sizeof(Table));
    strcpy(tb->name,table_name);
    tb->columns = calloc(sizeof(Column), INIT_COL_SIZE);
    tb->col_count = 0;        //initialize column count for created columns
    tb->table_length = num_col;     // max size of table in columns
    tb->table_length=0;   //number of rows in table
    db->tables[db->tables_size] = *tb;   //set table array to point at the current table
    db->tables_size += 1;   //increase table size by 1
    ret_status->code=OK;
    printf("%s table has been created\n", tb->name);
    return ret_status;
}


Status* create_col(char* col_name, Table* tb, Status *ret_status){
    Column *col = malloc(sizeof(Column));  //allocate mem for new column
    col->col_size=0;
    col->data = calloc(sizeof(int), INIT_COL_SIZE);
    strcpy(col->name,col_name);
    printf("%s column has been created\n", col->name);
    ret_status->code=OK;
    tb->columns[tb->col_count] = *col;   //set column array to point at the current column
    tb->col_count += 1;   //advance the pointer by 1
    return ret_status;
}

void delete_col(Column *col) {
    free(col->data);
    free(col);
}

/*
 * Inserts data into column
 */

void insert_col(Column *col, int num){
    int i = col->col_size;
    col->data[i] = num;   //insert new value into column
    col->col_size += 1;      //advance pointer to next positions
}

/*
 * Fetch pointer to column
 */

Column* fetch_column(Table *tb, char* col_name){
    Column *current_column;
    for(int i=0; i < tb->col_count; ++i) {
        current_column = &tb->columns[i];
        if (strcmp(current_column->name, col_name) == 0)
            return (current_column);
    }
    printf("Column Not Found\n");
    return(0);
}
/*
 * Fetch table form db
 */

Table* fetch_table(char* table_name){
    Table *current_tb;
    for(int i=0; i < current_db->tables_size; ++i){
        current_tb = &current_db->tables[i];
        if (strcmp(current_tb->name, table_name) == 0)
            return(current_tb);
    }
    printf("Table Not Found\n");
    return(0);
}


void relational_insert(DbOperator* query) {
    char *table_name = query->operator_fields.insert_operator.table->name;
    Table *working_table = fetch_table(table_name);  //fetch table to work with
    int *values = query->operator_fields.insert_operator.values;  //array of values to be inserted
    //Column** table_col = working_table->columns;  //pointer to columns
    working_table->table_length+= 1;  //add length to table
    for(int i=0; i < query->operator_fields.insert_operator.num_values; ++i)
    {
        char* column_name = working_table->columns[i].name;
        Column *working_col = fetch_column(working_table, column_name); //fetch working col
        insert_col(working_col, values[i]);

    }
    //cs165_log(stdout, "Success: Row Inserted\n");

}

//SELECT OPERATORS

/*
 * Return a positions array of col given a column and low and high
 */
void* select_high_low(Result* result,Column* col,int low,int high){
    int j=0;
    for (int i=0; i < col->col_size; ++i){
        if (col->data[i] < high && col->data[i] > low) {
            result->payload[j] = i;
            result->num_tuples +=1;
            ++j;
        }
    }

}


/*
 * Return a positions array of col given a column and low
 */
void* select_low(Result* result,Column* col,int low){
    int j=0;
    for (int i=0; i < col->col_size; ++i){
        if (col->data[i] > low) {
            result->payload[j] = i;
            result->num_tuples +=1;
            ++j;
        }
    }

}

/*
 * Return a positions array of col given a column and high
 */

void* select_high(Result* result,Column* col,int high){
    int j=0;
    for (int i=0; i < col->col_size; ++i){
        if (col->data[i] < high) {
            result->payload[j] = i;
            result->num_tuples +=1;
            ++j;
        }
    }

}

/*
 * Stores temporary variable in variable pool
 */
void* store_var(char* var_name, Result* result){
    PooledVar* var= malloc(sizeof(PooledVar));
    var->result = result;
    strcpy(var->name,var_name);
    varPool->pooledVar[varPool->pool_size] = *var;
    varPool->pool_size += 1;
}

/*
 * Select statement that chooses which select function to use and then returns a result
 */
Result* select_val(DbOperator* query) {
    char* low_c = query->operator_fields.select_operator.low;
    char* high_c = query->operator_fields.select_operator.high;
    Column* col = query->operator_fields.select_operator.col;
    int* temp_pos;
    //initialize result vector
    Result* result = malloc(sizeof(result));
    result->num_tuples = 0;
    result->data_type =  INT;
    result->payload = calloc(sizeof(int),INIT_COL_SIZE);

    //temporary position array
    if (strcmp(low_c,"null")==0){
        int high = atoi(query->operator_fields.select_operator.high);
        select_high(result,col,high);
    }
    else if (strcmp(high_c,"null")==0){
        int low = atoi(query->operator_fields.select_operator.low);
        select_low(result,col,low);
    }
    else {
        int high = atoi(query->operator_fields.select_operator.high);
        int low = atoi(query->operator_fields.select_operator.low);
        select_high_low(result, col, low, high);
    }
    return(result);
}
/*
 * Given a position vector return the Result containeing the tuples
 */

Result* fetch(DbOperator* query) {
    Result* result = malloc(sizeof(Result));
    result->num_tuples = 0;
    result->data_type =  INT;

    Column* col = query->operator_fields.fetch_operator.col;
    int* pos = query->operator_fields.fetch_operator.pos;
    int size = query->operator_fields.fetch_operator.num_tuples;
    result->payload = calloc(sizeof(int),size);
    for (int i=0; i < size; ++i){
       result->payload[i] = col->data[pos[i]];
       //increase number of tuples
       result->num_tuples += 1;
    }
    return(result);

}

Result* fetch_poolvar(char* name){
    for (int i = 0; i < varPool->pool_size;++i){
        if(strcmp(name, varPool->pooledVar[i].name)==0)
          return(varPool->pooledVar[i].result);
    }
    log_err("Error: no pooled variable exist with that name");
    return(0);
}

/*
 * Given a result print result to screen
 */
char* print_result(DbOperator* query) {

    int num_tuples = query->operator_fields.print_operator.num_tuples;
    char* result_string = malloc(num_tuples*10* sizeof(char));
    DataType data_type = query->operator_fields.print_operator.data_type;
    int result_size = 0;
    if (data_type == INT) {
        int *payload = query->operator_fields.print_operator.payload;
        int j =0;
        for (int i = 0; i < num_tuples; ++i) {

            //convert result to char*
            char* c = malloc(sizeof(char)*10);

            snprintf(c, sizeof(int), "%d, ", payload[i]);
            result_string = concat(result_string, c);


        }
        return(result_string);
    }
}


/*
 * Given a column return the average
 */

//float average_col(DbOperator query){
//    int num_tuples = query.operator_fields.avg_operator.num_tuples;
//    int total = 0;
//    int* col = query.operator_fields.avg_operator.payload;
//    for (int i =0; i < num_tuples; ++i){
//        total += col[i];
//    }
//    float average = total/num_tuples;
//    return (average);
//}




void delete_columns(Table *table) {
    for (int i = 0; i < table->col_count; i++) {
        delete_col(&table->columns[i]);
    }
}

