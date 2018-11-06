
#ifndef DEFINITIONS_H
#define DEFINITIONS_H

#include "client_context.h"
#include "mydb_manager.h"
#include "util.h"

#endif
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 * 
 */

extern char* trim_whitespace(char *str);

Table* lookup_table(char *name) {
	// void pattern for 'using' a variable to prevent compiler unused variable warning
	(void) name;

	return NULL;
}

/**
 * This takes a database.table.col and strip out the column
 */

char* strip_col(char* db_table_col) {
	char* col_name = malloc(sizeof(db_table_col));
	for (int i=0; i < strlen(db_table_col); ++i) {
		//strip table
		if (db_table_col[i] == '.') {
			for (int j = 0; db_table_col[i] != '\0'; ++j) {
				++i;
				//loop until next dot is found
				if (db_table_col[i] == '.') {
					++i;
					for (int k = 0; db_table_col[i] != '\0' && db_table_col[i] != '\n'; ++k) {    //loop through column name
						col_name[k] = db_table_col[i];
						++i;
					}
				}
			}
			return (col_name);
		}
	}
	return(0);
}



/**
 * This takes a database.table and strip out the table
 */

char* strip_table(char* db_table) {
	char* table_name = malloc(sizeof(db_table));
	for (int i=0; i < strlen(db_table); ++i){
		if (db_table[i] == '.') {
			++i;
			for ( int j = 0; (db_table[i] != '.' && db_table[i] != '\0'); ++j) {
				table_name[j] = db_table[i];
				++i;
			}
			return(table_name);

		}
	}
	return(0);
}

/*
 * Strips DB name from string
 */

char* strip_db(char* db_table) {
	char* db_name_strip = malloc(sizeof(db_table));
	for (int i=0; db_table[i] != '.'; ++i){
		db_name_strip[i] = db_table[i];
	}
	return(db_name_strip);
}

char* strip_command(char* tokenizer){
	char* stripped_token = malloc(1024);
	for (int i=0; tokenizer[i] != '\0'; ++i){
		if (tokenizer[i] == '(') {
			for ( int j = 0; tokenizer[i] != '\n'; ++j) {
				stripped_token[j] = tokenizer[i];
				++i;
			}
			return(stripped_token);
		}
	}
	return (0);
}


char* strip_var(char* tokenizer){
    char* stripped_token[MAX_SIZE_NAME];
    int j = 0;
    for (int i=0; tokenizer[i] != '='; ++i){
      stripped_token[j] = tokenizer[i];
      ++j;
      return(stripped_token);
    }
    return (0);
}
/*
 * Strips url from load command
 */
char* strip_url(char* arguments){
	char* stripped_url = malloc(1024);
	for (int i=0; arguments[i] != '\0'; ++i){
		if (arguments[i] == '\"') {
			++i;
			for ( int j = 0; arguments[i] != '\"'; ++j) {
				stripped_url[j] = arguments[i];
				++i;
			}
			return(stripped_url);
		}
	}
	return (0);

}

char* strip_par(char* arguments){
    char* stripped = malloc(1024);
    for (int i = 0; arguments[i] != ')'; ++i){
        stripped[i] = arguments[i];
    }
    return(stripped);
}


/*
 * given two char* concat
 *
 */

char* concat(const char *s1, const char *s2)
{
    char *result = malloc(strlen(s1) + strlen(s2) + 1); // +1 for the null-terminator
    // in real code you would check for errors in malloc here
    strcpy(result, s1);
    strcat(result, s2);
    return result;
}


/*
 * Returns 0 if a string contains a .
 */
int* cont_per(char* str){
    for(int i=0; i < sizeof(str); ++i){
        if (str[i] == '.' ){
            return 0;
        }
    }
    return 1;
}

/**
*  Getting started hint:
* 		What other entities are context related (and contextual with respect to what scope in your design)?
* 		What else will you define in this file?
 * 		load("/Users/Shaun/CLionProjects/final_project/cs165-2018-base/project_tests/data2.csv")
**/
