#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "mydb_manager.h"


Table* lookup_table(char *name);

char* strip_table(char* db_table);

char* strip_db(char* db_table);

char* strip_col(char* db_table_col);

char* strip_command(char* tokenizer);

char* strip_url(char* arguments);

char* strip_par(char* arguments);

char* concat(const char *s1, const char *s2);

char* strip_var(char* tokenizer);

int* cont_per(char* str);

#endif
