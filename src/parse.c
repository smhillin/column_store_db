/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */



#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "parse.h"
#include "utils.h"
#include "client_context.h"
#include "mydb_manager.h"




extern Db *current_db;
extern VarPool *varPool;

extern message_status parse_create_tbl(char* create_arguments);

//extern Status create_table(Db* db, const char* name, size_t num_columns, Status *ret_status);

extern Status create_db(char* name);

extern message_status parse_create(char* create_arguments);

extern DbOperator* parse_insert(char* query_command, message* send_message);

///*
// * Loop through db and print out nth element of each column to form Rows.
// */
//Status* save_db(){
//    FILE *fp;
//    char buffer[sizeof(int)];
//    fp= fopen(filename, "w");
//    //retrieves tables to loop through
//    for(int i=0; i < current_db->tables_size; ++i){
//        Table *working_table = current_db->tables[i];
//        int num_rows = working_table->columns[0]->col_size;
//        //prints out the nth value of all columns
//        for(int i=0; i < num_rows; ++i){
//            //prints out values from
//            for (int j =0; j < working_table->col_count; ++j){
//                Column* working_column = working_table->columns[i];
//                int value = working_column->data[i];
//                fprintf(fp, "%d", value);
//                fputs(",", fp);
//            }
//            fputs("\n", fp);  //newline for next row
//        }
//    }
//    printf("DB successfully printed");
//}

/*
 * Counts the number of takens in a statement
 */
int count_tokens(char* state){
    int count=0;
    for(int i=0; state[i] != '\n'; ++i){
        if (state[i] == ','){
            count += 1;
        }
    }
    return(count+1);
}

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}
/*
 * Builds dsl create table statement
 */
char* dsl_create_tbl(char* db_tbl_col){
    char *command= malloc(sizeof(char)*150);
    char *db_name = strip_db(db_tbl_col);
    char *tbl_name = strip_table(db_tbl_col);
    //create table
    strcat(command, "create(tbl,\"");
    strcat(command, tbl_name);
    strcat(command, "\",");
    strcat(command, db_name);
    strcat(command, ",");
    strcat(command, "1");
    strcat(command, ")\n");
    //create tbl
    return(command);
}
/*
 * Builds dsl create column statement
 */
char* dsl_create_col(char* db_tbl_col){
    char *command= malloc(sizeof(char)*150);
    char *db_name = strip_db(db_tbl_col);
    char *tbl_name = strip_table(db_tbl_col);
    char* col_name  = strip_col(db_tbl_col);
    strcat(command, "create(col,\"");
    strcat(command, col_name);
    strcat(command, "\",");
    strcat(command, db_name);
    strcat(command, ".");
    strcat(command, tbl_name);
    strcat(command, ")\n");
    return(command);
}

/*
 * Builds a dsl insert statement
 */
char* dsl_insert_row(char* line, char* table_name){
    char *command= malloc(sizeof(char)*150);
    strcat(command, "relational_insert(db1.");
    strcat(command, table_name);
    strcat(command, ",");
    strcat(command, line);
    strcat(command, ")\n");
    return(command);
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

void load_table(const char* filename){
    FILE *fp;
    fp= fopen(filename, "r");
    DbOperator* query = malloc(sizeof(DbOperator));
    message* send_message= malloc(sizeof(message));
    char* table_name;
    if (fp == NULL) {
        exit(EXIT_FAILURE);
    }
    //read line by line
    const size_t line_size = 1024;
    static char line[line_size];
    //read column names
    int line_nums = 0;    //line we reading from document
    while (fgets(line, line_size, fp)) {
        char *dsl_statement;   //creates buffer for dsl statement
        //processes the first line with table and db info
        if (line_nums == 0){
            send_message->status = OK_DONE;
            message_status* status;
            char *line_arguments_index = &line[0];
            int token_count = count_tokens(line);
            for (int i = 0; i < token_count; ++i) {
                //create DB
                char *db_tbl_col = next_token(&line_arguments_index, status);
                char *db_name = strip_db(db_tbl_col);
                table_name = strip_table(db_tbl_col);
                //create the db and table name and create table for first token
                if (i == 0) {
                    create_db(db_name);  //create_db
                    dsl_statement = dsl_create_tbl(db_tbl_col);
                    printf("%s", dsl_statement);
                    parse_create(dsl_statement);
                }
                //creates columns on all tokens of first row
                dsl_statement = dsl_create_col(db_tbl_col);
                printf("%s", dsl_statement);
                parse_create(dsl_statement);
            }
            line_nums+=1;
        }
        //insert rows
        else {
            line[strlen(line) - 1] = '\0';   //strip newline character
            char *dsl_statement = dsl_insert_row(line,table_name);
            printf("%s", dsl_statement);
            send_message->status = OK_DONE;
            query = parse_insert(dsl_statement,send_message);
            relational_insert(query);
        }
    }
    cs165_log(stdout, "Success, %s table loaded\n", table_name);
    fclose(fp);

}

/*
 * Given a table return top row for row stores
 */
void build_top_row(char* db_name,Table *tb,FILE *fp){
    Table *working_table = tb;
    char* table_name = working_table->name;
    Column *working_column = malloc(sizeof(Column));
    //iterate through column names
    for(int i= 0; i < working_table->col_count; ++i) {
        working_column = &working_table->columns[i];
        char* column_name = working_column->name;
        if (i < (working_table->col_count-1)) {
            fprintf(fp, "%s.%s.%s,", db_name, table_name, column_name);
        }
            //last column no coma
        else {
            fprintf(fp, "%s.%s.%s", db_name, table_name, column_name);
        }
    }
    fprintf(fp,"\n");
    }

/*
 * Saves DB to repo
 */
message_status save_db(){
    cs165_log(stdout, "saving to.... %s \n", DB_FILE);
    message_status status = OK_DONE;
    FILE *fp = fopen(DB_FILE, "w");
    Table *working_table= malloc(sizeof(Table));
    Column *working_column = malloc(sizeof(Column));
    char* db_name = current_db->name;
    //save top row with table info
    //iterate through tables and save tables
    //iterate through tables and save tables
    for(int i=0; i < current_db->tables_size; ++i) {
        working_table = &current_db->tables[i];
        //print top row
        build_top_row(db_name,working_table, fp);
        //iterate through row
        for(int j=0; j < working_table->table_length; ++j){
            //save row
            for(int i= 0; i < working_table->col_count; ++i) {
                working_column = &working_table->columns[i];
                fprintf(fp, "%d,", working_column->data[j]);
            }
            fprintf(fp,"\n");
        }
        cs165_log(stdout, "%s saved \n", working_table->name);

    }
    cs165_log(stdout, "DB saved \n");
    fclose(fp);
    return status;
}

// "Users/Shaun/CLionProjects/final_project/cs165-2018-base/project_tests/test01.dsl"


message_status parse_create_col(char* create_arguments) {
    message_status status = OK_DONE;
    char **create_arguments_index = &create_arguments;
    char *col_name = next_token(create_arguments_index, &status);
    char *db_tbl_name = next_token(create_arguments_index, &status);


    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // Get the col name free of quotation marks
    col_name = trim_quotes(col_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(db_tbl_name) - 1;
    if (db_tbl_name[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character.
    db_tbl_name[last_char] = '\0';

    //  strip out table name only
    char* tbl_name = strip_table(db_tbl_name);

    //  strip out db name only

    char* db_name = strip_db(db_tbl_name);

    // check that the database argument is the current active database
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }
    //retrieve pointer to table
    Table *working_tbl = fetch_table(tbl_name);
    Status create_status;
    create_col(col_name, working_tbl, &create_status);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a table failed.");
        return EXECUTION_ERROR;
    }

    return status;
}


message_status parse_create_tbl(char* create_arguments) {
    if (current_db == NULL){
        create_db(DB_NAME);
    }
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';

    // check that the database argument is the current active database
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }

    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Status create_status;
    create_table(table_name, current_db, column_cnt, &create_status);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a table failed.");
        return EXECUTION_ERROR;
    }

    return status;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

message_status parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return INCORRECT_FORMAT;
        }
        if (create_db(db_name).code == OK) {
            return OK_DONE;
        } else {
            return EXECUTION_ERROR;
        }
    }
}


/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 *
 **/
message_status parse_create(char* create_arguments) {
    message_status mes_status;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);

    // check for create statement if so strip
    if (strncmp(tokenizer_copy, "c", 1) == 0) {
        tokenizer_copy=strip_command(tokenizer_copy);
    }
    // check for leading parenthesis after create.
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return mes_status;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                mes_status = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                mes_status = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0){
                mes_status = parse_create_col(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return mes_status;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    int columns_inserted = 0;
    char* token = NULL;
    // check for insert statement if so strip
    if (strncmp(query_command, "r", 1) == 0) {
        query_command=strip_command(query_command);
    }
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* db_table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        //

        //  strip out table name only
        char* table_name = strip_table(db_table_name);


        // lookup the table and make sure it exists. 
        Table* insert_table = fetch_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = calloc(insert_table->col_count, sizeof(int));  //number of cols in table which is number of ints to insert
        // parse inputs until we reach the end. Turn each given string into an integer.,sizeof(int)*
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted += 1;
        }
        //creates a value for number of calue inserts to be used later
        dbo->operator_fields.insert_operator.num_values = columns_inserted;
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}


DbOperator* parse_select(char* var_name,char* query_command, message* send_message) {
    //create pooled var name
    char* pooledVar = var_name;

    char *token = NULL;
    // check for insert statement if so strip
    if (strncmp(query_command, "s", 1) == 0) {
        query_command = strip_command(query_command);
    }
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char **command_index = &query_command;
        // parse table input
        char *db_table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        //

        //  strip out table name only
        char *table_name = strip_table(db_table_name);

        //  strip out db name only

        char *db_name = strip_db(db_table_name);

        //strip out column name
        char *select_col = strip_col(db_table_name);

        // lookup the table and make sure it exists.
        Table *insert_table = fetch_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // strip out low select
        char* low = next_token(command_index, &send_message->status);

        // strip out high select
        char* high = next_token(command_index, &send_message->status);
        high = strip_par(high);

        //checks to make sure we have at least one value to select
        if (low == NULL && high == NULL) {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        // make select operator.
        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT;
        dbo->operator_fields.select_operator.pooledVar = pooledVar;
        dbo->operator_fields.select_operator.table = fetch_table(table_name);
        dbo->operator_fields.select_operator.col = fetch_column(fetch_table(table_name),select_col);
        //creates a value for number of calue inserts to be used later
        dbo->operator_fields.select_operator.low = low;
        dbo->operator_fields.select_operator.high = high;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_fetch(char* var_name, char* query_command, message* send_message) {

    //create pooled var entry
    PooledVar *pooledVar = malloc(sizeof(PooledVar));
    strcpy(pooledVar->name,var_name);

    char *token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char **command_index = &query_command;
        // parse table input
        char *db_table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }



        //  strip out table name only
        char *table_name = strip_table(db_table_name);

        //  strip out db name only

        char *db_name = strip_db(db_table_name);

        //strip out column name
        char *fetch_col = strip_col(db_table_name);

        // lookup the table and make sure it exists.
        Table *insert_table = fetch_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // strip out position vector
        char* token = next_token(command_index, &send_message->status);

        // strip out pool var name and retrieve result
        char* temp_result= strip_par(token);

        //rettrieved position array
        Result* result  = fetch_poolvar(temp_result);



        //checks to make sure we have position vector
        if (result == NULL) {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        // make select operator.
        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = FETCH;
        dbo->operator_fields.fetch_operator.pooledVar = var_name;
        dbo->operator_fields.fetch_operator.table = fetch_table(table_name);
        dbo->operator_fields.fetch_operator.col = fetch_column(fetch_table(table_name),fetch_col);
        //creates a position vector
        dbo->operator_fields.fetch_operator.pos = result->payload;
        dbo->operator_fields.fetch_operator.num_tuples = result->num_tuples;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

//DbOperator* parse_avg(char* var_name, char* query_command, message* send_message) {
//
//    //create pooled var entry
//    PooledVar *pooledVar = malloc(sizeof(PooledVar));
//    strcpy(pooledVar->name,var_name);
//
//    char *token = NULL;
//    // check for leading '('
//    if (strncmp(query_command, "(", 1) == 0) {
//
//        query_command++;
//
//        // strip out pool var name and retrieve result
//        char* var_name= strip_par(query_command);
//
//        // strip out pool var name and retrieve result
//        char* temp_result= strip_par(token);
//
//        //rettrieved position array
//        Result* result  = fetch_poolvar(temp_result);
//
//
//
//
//        // make select operator.
//        DbOperator *dbo = malloc(sizeof(DbOperator));
//        dbo->type = AVG;
//        dbo->operator_fields.avg_operator.num_tuples = result->num_tuples;
//        dbo->operator_fields.avg_operator.payload = result->payload;
//        dbo->operator_fields.avg_operator.data_type = result->data_type;
//
//        return dbo;
//    } else {
//        send_message->status = UNKNOWN_COMMAND;
//        return NULL;
//    }
//}

DbOperator* parse_print(char* query_command, message* send_message) {
    char *token = NULL;
    // check for insert statement if so strip

    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;


        // strip out pool var name and retrieve result
        char* var_name= strip_par(query_command);

        Result* result  = fetch_poolvar(var_name);


        //checks to make sure we have result vector
        if (result == NULL) {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        // make select operator.
        DbOperator *dbo = malloc(sizeof(DbOperator));
        dbo->type = PRINT;
        dbo->operator_fields.print_operator.num_tuples = result->num_tuples;
        dbo->operator_fields.print_operator.payload = result->payload;
        dbo->operator_fields.print_operator.data_type = result->data_type;
        return dbo;
    }

    else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}
/*
 * Parses and calls load table
 */
message_status parse_load(char* create_arguments) {
    message_status mes_status;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input.
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for load statement if so strip
    if (strncmp(tokenizer_copy, "l", 1) == 0) {
        tokenizer_copy=strip_command(tokenizer_copy);
    }
    // check for leading parenthesis after create.
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return mes_status;
        } else {
            // pass off to next parse function.
            if (strcmp(token, "db") == 0) {
                mes_status = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                mes_status = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0){
                mes_status = parse_create_col(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return mes_status;
}



/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 * 
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse? 
 *      What if such command requires multiple arguments?
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    DbOperator *dbo = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    cs165_log(stdout, "QUERY: %s\n", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);
    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        send_message->status = parse_create(query_command);
        dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        char* url = strip_url(query_command);
        load_table(url);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, send_message);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        printf("shutting down....");
        save_db();
        exit(1);
    }
      //here we strip off temporary storage variable name  for select and fetch queries
      else {
        if (varPool == NULL){
            create_pool();
        }
        //trim new variable plus = sign
        if (strncmp(query_command, "select", 6) == 0){
            query_command += 6;
            dbo = parse_select(handle,query_command, send_message);
        }
        else if(strncmp(query_command, "fetch", 5) == 0){
            query_command += 5;
            dbo = parse_fetch(handle, query_command, send_message);

        }
//        else if(strncmp(query_command, "avg", 3) == 0){
//            query_command += 3;
//            dbo = parse_avg(handle, query_command, send_message);
//
//        }
    }

    if (dbo == NULL) {
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
