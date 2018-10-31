#ifndef PARSE_H__
#define PARSE_H__
#include "message.h"
#include "client_context.h"


DbOperator* parse_command(char* query_command, message* send_message, int client, ClientContext* context);
void load_table(const char* filename);
message_status save_db();

#endif
