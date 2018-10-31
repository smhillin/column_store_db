//
// Created by shaun hillin on 10/8/18.
//




#include "mydb_manager.h"
#include "client_context.h"
#include "utils.h"



// In this class, there will always be only one active database at a time
Db *current_db;


//Variable Pool for temporary arrays




VarPool *varpool;



Status create_pool(){
    varpool = malloc(sizeof(VarPool));
    varpool->pooledVar= (malloc(1024));
    varpool->pool_size = 0;
    struct Status ret_status;
    ret_status.code = OK;
    return ret_status;
}



Status create_db(char *db_name) {
    current_db = malloc(sizeof(Db));  //allocate memory for DB
    strcpy(current_db->name,db_name);   //name of the DB
    current_db->tables_size = 0; //size of the array holding the tables
    current_db->tables_capacity= 5;  //amount of pointers that can be held in the currently allocated mem slot
    cs165_log(stdout, "%s - has been created\n", current_db->name);
    struct Status ret_status;
    ret_status.code = OK;
    return ret_status;
}

Status* create_table(char* table_name, Db *db,  int num_col, Status *ret_status) {
    Table *tb = malloc(sizeof(Table));
    strcpy(tb->name,table_name);
    tb->col_count = 0;        //initialize column count for created columns
    tb->table_length = num_col;     // max size of table in columns
    db->tables[db->tables_size] = tb;   //set table array to point at the current table
    db->tables_size += 1;   //increase table size by 1
    tb->table_length=0;   //number of rows in table
    ret_status->code=OK;
    printf("%s table has been created\n", tb->name);
    return ret_status;
}


Status* create_col(char* col_name, Table* tb, Status *ret_status){
    Column *col = malloc(sizeof(Column));  //allocate mem for new column
    tb->columns[tb->col_count] = col;   //set column array to point at the current column
    tb->col_count += 1;   //advance the pointer by 1
    col->col_size=0;
    strcpy(col->name,col_name);
    printf("%s column has been created\n", col->name);
    ret_status->code=OK;
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
        current_column = tb->columns[i];
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
        current_tb = current_db->tables[i];
        if (strcmp(current_tb->name, table_name) == 0)
            return(current_tb);
        printf("Table Not Found\n");
    }
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
        char* column_name = working_table->columns[i]->name;
        Column *working_col = fetch_column(working_table, column_name); //fetch working col
        insert_col(working_col, values[i]);

    }
    //cs165_log(stdout, "Success: Row Inserted\n");

}

//SELECT OPERATORS

/*
 * Return a positions array of col given a column and low and high
 */
int* select_high_low(Column* col,int low,int high){
    int* temp_pos=malloc(sizeof(int)*INIT_COL_SIZE);
    int j=0;
    for (int i=0; i < col->col_size; ++i){
        if (col->data[i] > low && col->data[i] < high)
            temp_pos[j] = i;
            ++j;
    }
    return(temp_pos);

}


/*
 * Return a positions array of col given a column and low
 */
int* select_low(Column* col,int low){
    int* temp_pos=malloc(sizeof(int)*INIT_COL_SIZE);
    int j=0;
    for (int i=0; i < col->col_size; ++i){
        if (col->data[i] > low)
            temp_pos[j] = i;
        ++j;
    }
    return(temp_pos);
}

/*
 * Return a positions array of col given a column and high
 */

int* select_high(Column* col,int high){
    int* temp_pos=malloc(sizeof(int)*INIT_COL_SIZE);
    int j=0;
    for (int i=0; i < col->col_size; ++i){
        if (col->data[i] < high) {
            temp_pos[j] = i;
            ++j;
        }
    }
    return(temp_pos);
}

/*
 * Stores temporary variable in variable pool
 */
void* store_var(char* var_name, Result* result){
    PooledVar* var= malloc(sizeof(PooledVar));
    var->result = result;
    var->name = var_name;
    varpool->pooledVar[varpool->pool_size] = *var;
    varpool->pool_size += 1;
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
    Result* result = malloc(sizeof(int)*INIT_COL_SIZE);
    result->num_tuples = 0;
    result->data_type =  INT;

    //temporary position array
    if (strcmp(low_c,"null")==0){
        int high = atoi(query->operator_fields.select_operator.high);
        result->payload=select_high(col,high);
    }
    else if (strcmp(high_c,"null")==0){
        int low = atoi(query->operator_fields.select_operator.low);
        result->payload=select_low(col,low);
    }
    else {
        int high = atoi(query->operator_fields.select_operator.high);
        int low = atoi(query->operator_fields.select_operator.low);
        result->payload=select_high_low(col, low, high);
    }
    result->num_tuples  = sizeof(result->payload);
    return(result);
}
/*
 * Given a position vector return the Result containeing the tuples
 */

Result* fetch(DbOperator* query) {
    Result* result = malloc(sizeof(Result));
    Column* temp_payload = malloc(sizeof(int)*INIT_COL_SIZE);
    result->num_tuples = 0;
    result->data_type =  INT;
    Column* col = query->operator_fields.fetch_operator.col;
    int* pos = query->operator_fields.fetch_operator.pos;
    for (int i=0; i < sizeof(pos); ++i){
       temp_payload[i] = col[pos[i]];
       //increase number of tuples
       result->num_tuples += 1;
    }
    result->payload = temp_payload;
    return(result);

}

Result* fetch_poolvar(char* name){
    for (int i; i < varpool->pool_size;++i){
        if(strcmp(name, varpool->pooledVar[i].name))
          return(varpool->pooledVar[i].result);
    }
    log_err("Error: no pooled variable exist with that name");
    return(0);
}


/*
 * Given a result print result to screen
 */
void* print_result(DbOperator* query) {
    int num_tuples = query->operator_fields.print_operator.num_tuples;
    DataType data_type = query->operator_fields.print_operator.data_type;
    if (data_type == INT) {
        int *payload = query->operator_fields.print_operator.payload;
        for (int i = 0; i < num_tuples; ++i) {
            printf("%d", payload[i]);
        }
    }
}




void delete_columns(Table *table) {
    for (int i = 0; i < table->col_count; i++) {
        delete_col(table->columns[i]);
    }
}

