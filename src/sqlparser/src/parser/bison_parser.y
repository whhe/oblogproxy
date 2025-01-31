// clang-format off
%{
  // clang-format on
  /**
 * bison_parser.y
 * defines bison_parser.h
 * outputs bison_parser.c
 *
 * Grammar File Spec: http://dinosaur.compilertools.net/bison/bison_6.html
 *
 */
  /*********************************
 ** Section 1: C Declarations
 *********************************/

#include "bison_parser.h"
#include "flex_lexer.h"

#include <stdio.h>
#include <string.h>

  using namespace hsql;

  int yyerror(YYLTYPE * llocp, SQLParserResult * result, yyscan_t scanner, const char* msg) {
    result->setIsValid(false);
    result->setErrorDetails(strdup(msg), llocp->first_line, llocp->first_column);
    return 0;
  }
  // clang-format off
%}
// clang-format on
/*********************************
 ** Section 2: Bison Parser Declarations
 *********************************/

// Specify code that is included in the generated .h and .c files
// clang-format off
%code requires {
// %code requires block

#include "../SQLParserResult.h"
#include "../sql/statements.h"
#include "parser_typedef.h"

// Auto update column and line number
#define YY_USER_ACTION                        \
  yylloc->first_line = yylloc->last_line;     \
  yylloc->first_column = yylloc->last_column; \
  for (int i = 0; yytext[i] != '\0'; i++) {   \
    yylloc->total_column++;                   \
    yylloc->string_length++;                  \
    if (yytext[i] == '\n') {                  \
      yylloc->last_line++;                    \
      yylloc->last_column = 0;                \
    } else {                                  \
      yylloc->last_column++;                  \
    }                                         \
  }
}

// Define the names of the created files (defined in Makefile)
// %output  "bison_parser.cpp"
// %defines "bison_parser.h"

// Tell bison to create a reentrant parser
%define api.pure full

// Prefix the parser
%define api.prefix {hsql_}
%define api.token.prefix {SQL_}

%define parse.error verbose
%locations

%initial-action {
  // Initialize
  @$.first_column = 0;
  @$.last_column = 0;
  @$.first_line = 0;
  @$.last_line = 0;
  @$.total_column = 0;
  @$.string_length = 0;
};


// Define additional parameters for yylex (http://www.gnu.org/software/bison/manual/html_node/Pure-Calling.html)
%lex-param   { yyscan_t scanner }

// Define additional parameters for yyparse
%parse-param { hsql::SQLParserResult* result }
%parse-param { yyscan_t scanner }

/*********************************
 ** Define all data-types (http://www.gnu.org/software/bison/manual/html_node/Union-Decl.html)
 *********************************/
%union {
  // clang-format on
  bool bval;
  char* sval;
  double fval;
  int64_t ival;
  uintmax_t uval;

  // statements
  hsql::AlterStatement* alter_stmt;
  hsql::CreateStatement* create_stmt;
  hsql::CreateBinlogStatement* create_binlog;
  hsql::DeleteStatement* delete_stmt;
  hsql::DropStatement* drop_stmt;
  hsql::ExecuteStatement* exec_stmt;
  hsql::ExportStatement* export_stmt;
  hsql::ImportStatement* import_stmt;
  hsql::InsertStatement* insert_stmt;
  hsql::PrepareStatement* prep_stmt;
  hsql::SelectStatement* select_stmt;
  hsql::ShowStatement* show_stmt;
  hsql::ShowBinaryLogsStatement* show_binary;
  hsql::ShowBinlogEventsStatement* show_binlog_event;
  hsql::ShowMasterStatusStatement* show_master;
  hsql::ShowSlaveStatusStatement* show_slave;
  hsql::ShowBinlogServerStatement* show_binlog_server;
  hsql::PurgeBinlogStatement* purge_binlog;
  hsql::DropBinlogStatement* drop_binlog;
  hsql::AlterBinlogStatement* alter_binlog;
  hsql::ShowBinlogStatusStatement* show_binlog_status;
  hsql::ReportStatement* report;
  hsql::ShowProcessListStatement* show_processlist;
  hsql::ShowDumpListStatement* show_dumplist;
  hsql::KillStatement* kill_conn;
  hsql::SetStatement* set_stmt;
  hsql::SQLStatement* statement;
  hsql::TransactionStatement* transaction_stmt;
  hsql::UpdateStatement* update_stmt;
  hsql::CreateBinlogInstanceStatement* create_binlog_instance;
  hsql::AlterBinlogInstanceStatement* alter_binlog_instance;
  hsql::StartBinlogInstanceStatement* start_binlog_instance;
  hsql::StopBinlogInstanceStatement* stop_binlog_instance;
  hsql::DropBinlogInstanceStatement* drop_binlog_instance;
  hsql::ShowBinlogInstanceStatement* show_binlog_instance;
  hsql::ShowNodesStatement* show_nodes;
  hsql::SwitchMasterInstanceStatement* switch_master;
  hsql::SetPasswordStatement* set_password;

  hsql::Alias* alias_t;
  hsql::AlterAction* alter_action_t;
  hsql::ColumnDefinition* column_t;
  hsql::ColumnType column_type_t;
  hsql::ConstraintType column_constraint_t;
  hsql::DatetimeField datetime_field;
  hsql::DropColumnAction* drop_action_t;
  hsql::Expr* expr;
  hsql::GroupByDescription* group_t;
  hsql::ImportType import_type_t;
  hsql::JoinType join_type;
  hsql::LimitDescription* limit;
  hsql::TenantName* tenant_name;
  hsql::UserInfo* user_info;
  hsql::OrderDescription* order;
  hsql::OrderType order_type;
  hsql::SetOperation* set_operator_t;
  hsql::TableConstraint* table_constraint_t;
  hsql::TableElement* table_element_t;
  hsql::TableName table_name;
  hsql::TableRef* table;
  hsql::UpdateClause* update_t;
  hsql::SetClause* set_t;
  hsql::SetClause* var_t;
  hsql::WithDescription* with_description_t;
  hsql::LockingClause* locking_t;
  hsql::InstanceFlag instance_flag;
  hsql::ShowInstanceMode show_instance_mode;

  std::vector<char*>* str_vec;
  std::unordered_set<hsql::ConstraintType>* column_constraint_set;
  std::vector<hsql::Expr*>* expr_vec;
  std::vector<hsql::OrderDescription*>* order_vec;
  std::vector<hsql::SQLStatement*>* stmt_vec;
  std::vector<hsql::TableElement*>* table_element_vec;
  std::vector<hsql::TableRef*>* table_vec;
  std::vector<hsql::UpdateClause*>* update_vec;
  std::vector<hsql::SetClause*>* set_vec;
  std::vector<hsql::SetClause*>* var_vec;
  std::vector<hsql::WithDescription*>* with_description_vec;
  std::vector<hsql::LockingClause*>* locking_clause_vec;

  std::pair<int64_t, int64_t>* ival_pair;

  hsql::RowLockMode lock_mode_t;
  hsql::RowLockWaitPolicy lock_wait_policy_t;
}

    /*********************************
     ** Destructor symbols
     *********************************/
    // clang-format off
    %destructor { } <fval> <ival> <bval> <join_type> <order_type> <datetime_field> <column_type_t> <column_constraint_t> <import_type_t> <column_constraint_set> <lock_mode_t> <lock_wait_policy_t> <instance_flag>
    %destructor {
      free( ($$.name) );
      free( ($$.schema) );
    } <table_name>
    %destructor {
      if ($$) {
        for (auto ptr : *($$)) {
          free(ptr);
        }
      }
      delete ($$);
    } <str_vec>
    %destructor { free( ($$) ); } <sval>
    %destructor {
      if ($$) {
        for (auto ptr : *($$)) {
          delete ptr;
        }
      }
      delete ($$);
    } <table_vec> <table_element_vec> <update_vec> <set_vec> <var_vec> <expr_vec> <order_vec> <stmt_vec>
    %destructor { delete ($$); } <*>


    /*********************************
     ** Token Definition
     *********************************/
    %token <sval> IDENTIFIER STRING
    %token <fval> FLOATVAL
    %token <ival> INTVAL

    /* SQL Keywords */
    %token DEALLOCATE PARAMETERS INTERSECT TEMPORARY TIMESTAMP
    %token DISTINCT NVARCHAR RESTRICT TRUNCATE ANALYZE BETWEEN
    %token CASCADE COLUMNS CONTROL DEFAULT EXECUTE EXPLAIN
    %token INTEGER NATURAL PREPARE PRIMARY SCHEMAS CHARACTER_VARYING REAL DECIMAL SMALLINT BIGINT
    %token SPATIAL VARCHAR VIRTUAL DESCRIBE BEFORE COLUMN CREATE DELETE DIRECT
    %token DOUBLE ESCAPE EXCEPT EXISTS EXTRACT CAST FORMAT GLOBAL HAVING IMPORT
    %token INSERT ISNULL OFFSET RENAME SCHEMA SELECT SORTED
    %token TABLES UNIQUE UNLOAD UPDATE VALUES AFTER ALTER CROSS
    %token DELTA FLOAT GROUP INDEX INNER LIMIT LOCAL MERGE MINUS ORDER
    %token OUTER RIGHT TABLE UNION USING WHERE CALL CASE CHAR COPY DATE DATETIME
    %token DESC DROP ELSE FILE FROM FULL HASH HINT INTO JOIN STOP START FORCE HISTORY
    %token LEFT LIKE LOAD LONG NULL PLAN SHOW TEXT THEN TIME
    %token VIEW WHEN WITH ADD ALL AND ASC END FOR INT KEY
    %token NOT OFF SET TOP AS BY IF IN IS OF ON OR TO NO NODES
    %token ARRAY CONCAT ILIKE SECOND MINUTE HOUR DAY MONTH YEAR
    %token SECONDS MINUTES HOURS DAYS MONTHS YEARS INTERVAL
    %token TRUE FALSE BOOLEAN
    %token TRANSACTION BEGIN COMMIT ROLLBACK
    %token NOWAIT SKIP LOCKED SHARE
    %token VARIABLES SESSION AT NAMES BINLOG SERVER UUID MASTER STATUS BINARY INSTANCE INSTANCES PROCESSLIST DUMPLIST
    %token LOGS EVENTS PURGE SLAVE TENANT CLUSTER URL USER PASSWORD REPORT REPLICATE NUM KILL CONNECTION QUERY SWITCH
    /* Binlog Options*/
    %token INITIAL_TRX_XID INITIAL_TRX_GTID_SEQ PROCESS_ONLY OBCDC_ONLY NOW DEFER
    /* Binlog obcdc_options*/
    %token START_TIMESTAMP ROOTSERVER_LIST CLUSTER_URL CLUSTER_USER CLUSTER_PASSWORD EXTRA_OBCDC_CFG
    /* Binlog binlog_options*/
    %token _INITIAL_OB_TXN_ID _INITIAL_OB_TXN_GTID_SEQ
    /* Binlog slot_options*/
    %token IP ZONE REGION

    /*********************************
     ** Non-Terminal types (http://www.gnu.org/software/bison/manual/html_node/Type-Decl.html)
     *********************************/
    %type <stmt_vec>               statement_list
    %type <statement>              statement preparable_statement
    %type <exec_stmt>              execute_statement
    %type <transaction_stmt>       transaction_statement
    %type <prep_stmt>              prepare_statement
    %type <select_stmt>            select_statement select_with_paren select_no_paren select_clause select_within_set_operation select_within_set_operation_no_parentheses
    %type <import_stmt>            import_statement
    %type <export_stmt>            export_statement
    %type <create_stmt>            create_statement
    %type <create_binlog>          create_binlog_statement
    %type <alter_binlog>           alter_binlog_statement
    %type <create_binlog_instance> create_binlog_instance_statement
    %type <alter_binlog_instance>  alter_binlog_instance_statement
    %type <start_binlog_instance>  start_binlog_instance_statement
    %type <stop_binlog_instance>   stop_binlog_instance_statement
    %type <drop_binlog_instance>   drop_binlog_instance_statement
    %type <show_binlog_instance>   show_binlog_instance_statement
    %type <switch_master>          switch_master_instance_statement
    %type <show_nodes>             show_nodes_statement
    %type <set_password>           set_password_statement
    %type <show_binary>            show_binary_statement
    %type <show_binlog_event>      show_binlog_event_statement
    %type <show_master>            show_master_statement
    %type <show_slave>             show_slave_statement
    %type <show_binlog_server>     show_binlog_server_statement
    %type <purge_binlog>           purge_binlog_statement
    %type <drop_binlog>            drop_binlog_statement
    %type <show_binlog_status>     show_binlog_status_statement
    %type <report>                 report_statement
    %type <show_processlist>       show_processlist_statement
    %type <show_dumplist>          show_dumplist_statement
    %type <kill_conn>              kill_statement
    %type <insert_stmt>            insert_statement
    %type <delete_stmt>            delete_statement truncate_statement
    %type <update_stmt>            update_statement
    %type <drop_stmt>              drop_statement
    %type <alter_stmt>             alter_statement
    %type <show_stmt>              show_statement
    %type <set_stmt>               set_statement
    %type <table_name>             table_name
    %type <sval>                   opt_index_name opt_for_instance
    %type <sval>                   file_path prepare_target_query
    %type <bval>                   opt_not_exists opt_exists opt_distinct opt_all opt_force
    %type <ival_pair>              opt_decimal_specification
    %type <ival>                   opt_time_precision
    %type <join_type>              opt_join_type
    %type <table>                  opt_from_clause from_clause table_ref table_ref_atomic table_ref_name nonjoin_table_ref_atomic
    %type <table>                  join_clause table_ref_name_no_alias
    %type <expr>                   expr operand scalar_expr unary_expr binary_expr logic_expr exists_expr extract_expr cast_expr
    %type <expr>                   function_expr between_expr expr_alias param_expr var_expr
    %type <expr>                   column_name literal int_literal num_literal string_literal bool_literal date_literal interval_literal
    %type <expr>                   comp_expr opt_where join_condition opt_having case_expr case_list in_expr hint opt_defer
    %type <expr>                   array_expr array_index null_literal opt_from opt_in
    %type <limit>                  opt_limit opt_top
    %type <tenant_name>            tenant_name opt_for_tenant opt_for
    %type <user_info>              opt_user_info
    %type <order>                  order_desc
    %type <order_type>             opt_order_type
    %type <datetime_field>         datetime_field datetime_field_plural duration_field
    %type <column_t>               column_def
    %type <table_element_t>        table_elem
    %type <column_type_t>          column_type
    %type <table_constraint_t>     table_constraint
    %type <update_t>               update_clause
    %type <set_t>                  set_clause binlog_instance_option obcdc_option instance_binlog_option slot_option binlog_option
    %type <var_t>                  var_clause
    %type <locking_t>              locking_clause
    %type <group_t>                opt_group
    %type <alias_t>                opt_table_alias table_alias opt_alias alias
    %type <with_description_t>     with_description
    %type <set_operator_t>         set_operator set_type
    %type <column_constraint_t>    column_constraint
    %type <column_constraint_set>  opt_column_constraints
    %type <column_constraint_set>  column_constraint_set
    %type <alter_action_t>         alter_action
    %type <drop_action_t>          drop_action
    %type <lock_wait_policy_t>     opt_row_lock_policy
    %type <lock_mode_t>            row_lock_mode
    %type <instance_flag>          opt_instance_flag

    // ImportType is used for compatibility reasons
    %type <import_type_t>          opt_file_type file_type

    %type <str_vec>                ident_commalist opt_column_list opt_instance_name_list
    %type <expr_vec>               expr_list select_list opt_literal_list literal_list hint_list opt_hints
    %type <table_vec>              table_ref_commalist
    %type <order_vec>              opt_order order_list
    %type <with_description_vec>   opt_with_clause with_clause with_description_list
    %type <update_vec>             update_clause_commalist
    %type <set_vec>                set_clause_commalist binlog_instance_option_commalist with_binlog_clause binlog_option_list binlog_option_clause
    %type <var_vec>                var_clause_commalist
    %type <table_element_vec>      table_elem_commalist
    %type <locking_clause_vec>     opt_locking_clause_list opt_locking_clause

    /******************************
     ** Token Precedence and Associativity
     ** Precedence: lowest to highest
     ******************************/
    %left     OR
    %left     AND
    %right    NOT
    %nonassoc '=' EQUALS NOTEQUALS LIKE ILIKE
    %nonassoc '<' '>' LESS GREATER LESSEQ GREATEREQ

    %nonassoc NOTNULL
    %nonassoc ISNULL
    %nonassoc IS        /* sets precedence for IS NULL, etc */
    %left     '+' '-'
    %left     '*' '/' '%'
    %left     '^'
    %left     CONCAT

    /* Unary Operators */
    %right    UMINUS
    %left     '[' ']'
    %left     '(' ')'
    %left     '.'
    %left     JOIN
    %left     '@'
%%
/*********************************
  ** Section 3: Grammar Definition
*********************************/

// Defines our general input.
input : statement_list opt_semicolon {
  for (SQLStatement* stmt : *$1) {
    // Transfers ownership of the statement.
    result->addStatement(stmt);
  }

  unsigned param_id = 0;
  for (void* param : yyloc.param_list) {
    if (param) {
      Expr* expr = (Expr*)param;
      expr->ival = param_id;
      result->addParameter(expr);
      ++param_id;
    }
  }
    delete $1;
  };

// clang-format on
statement_list : statement {
  $1->stringLength = yylloc.string_length;
  yylloc.string_length = 0;
  $$ = new std::vector<SQLStatement*>();
  $$->push_back($1);
}
| statement_list ';' statement {
  $3->stringLength = yylloc.string_length;
  yylloc.string_length = 0;
  $1->push_back($3);
  $$ = $1;
};

statement : prepare_statement opt_hints {
  $$ = $1;
  $$->hints = $2;
}
| preparable_statement opt_hints {
  $$ = $1;
  $$->hints = $2;
}
| import_statement { $$ = $1; }
| export_statement { $$ = $1; };
| set_statement { $$ = $1; };

preparable_statement : select_statement { $$ = $1; }
| create_statement { $$ = $1; }
| insert_statement { $$ = $1; }
| delete_statement { $$ = $1; }
| truncate_statement { $$ = $1; }
| update_statement { $$ = $1; }
| drop_statement { $$ = $1; }
| alter_statement { $$ = $1; }
| execute_statement { $$ = $1; }
| transaction_statement { $$ = $1; }
| show_statement { $$ = $1; }
| show_master_statement { $$ = $1; }
| show_slave_statement { $$ = $1; }
| show_binary_statement { $$ = $1; }
| show_binlog_event_statement { $$ = $1; }
| show_binlog_status_statement { $$ = $1; }
| create_binlog_statement { $$ = $1; }
| drop_binlog_statement { $$ = $1; }
| alter_binlog_statement { $$ = $1; }
| purge_binlog_statement { $$ = $1; }
| show_binlog_server_statement { $$ = $1; }
| create_binlog_instance_statement { $$ = $1; }
| alter_binlog_instance_statement { $$ = $1; }
| start_binlog_instance_statement { $$ = $1; }
| stop_binlog_instance_statement { $$ = $1; }
| drop_binlog_instance_statement { $$ = $1; }
| show_binlog_instance_statement { $$ = $1; }
| switch_master_instance_statement { $$ = $1; }
| set_password_statement { $$ = $1; }
| show_nodes_statement { $$ = $1; }
| report_statement { $$ = $1; }
| show_processlist_statement { $$ = $1; }
| show_dumplist_statement { $$ = $1; }
| kill_statement { $$ = $1; }
;

/******************************
 * Hints
 ******************************/

opt_hints : WITH HINT '(' hint_list ')' { $$ = $4; }
| /* empty */ { $$ = nullptr; };

hint_list : hint {
  $$ = new std::vector<Expr*>();
  $$->push_back($1);
}
| hint_list ',' hint {
  $1->push_back($3);
  $$ = $1;
};

hint : IDENTIFIER {
  $$ = Expr::make(kExprHint);
  $$->name = $1;
}
| IDENTIFIER '(' literal_list ')' {
  $$ = Expr::make(kExprHint);
  $$->name = $1;
  $$->exprList = $3;
};

/******************************
 * Transaction Statement
 ******************************/

transaction_statement : BEGIN opt_transaction_keyword { $$ = new TransactionStatement(kBeginTransaction); }
| ROLLBACK opt_transaction_keyword { $$ = new TransactionStatement(kRollbackTransaction); }
| COMMIT opt_transaction_keyword { $$ = new TransactionStatement(kCommitTransaction); };

opt_transaction_keyword : TRANSACTION | /* empty */
    ;

/******************************
 * Prepared Statement
 ******************************/
prepare_statement : PREPARE IDENTIFIER FROM prepare_target_query {
  $$ = new PrepareStatement();
  $$->name = $2;
  $$->query = $4;
};

prepare_target_query : STRING

                           execute_statement : EXECUTE IDENTIFIER {
  $$ = new ExecuteStatement();
  $$->name = $2;
}
| EXECUTE IDENTIFIER '(' opt_literal_list ')' {
  $$ = new ExecuteStatement();
  $$->name = $2;
  $$->parameters = $4;
};

/******************************
 * Import Statement
 * IMPORT FROM TBL FILE 'test/students.tbl' INTO students
 * COPY students FROM 'test/students.tbl' [WITH FORMAT TBL]
 ******************************/
import_statement : IMPORT FROM file_type FILE file_path INTO table_name {
  $$ = new ImportStatement($3);
  $$->filePath = $5;
  $$->schema = $7.schema;
  $$->tableName = $7.name;
}
| COPY table_name FROM file_path opt_file_type opt_where {
  $$ = new ImportStatement($5);
  $$->filePath = $4;
  $$->schema = $2.schema;
  $$->tableName = $2.name;
  $$->whereClause = $6;
};

file_type : IDENTIFIER {
  if (strcasecmp($1, "csv") == 0) {
    $$ = kImportCSV;
  } else if (strcasecmp($1, "tbl") == 0) {
    $$ = kImportTbl;
  } else if (strcasecmp($1, "binary") == 0 || strcasecmp($1, "bin") == 0) {
    $$ = kImportBinary;
  } else {
    free($1);
    yyerror(&yyloc, result, scanner, "File type is unknown.");
    YYERROR;
  }
  free($1);
};

file_path : string_literal {
  $$ = strdup($1->name);
  delete $1;
};

opt_file_type : WITH FORMAT file_type { $$ = $3; }
| /* empty */ { $$ = kImportAuto; };

/******************************
 * Export Statement
 * COPY students TO 'test/students.tbl' (WITH FORMAT TBL)
 ******************************/
export_statement : COPY table_name TO file_path opt_file_type {
  $$ = new ExportStatement($5);
  $$->filePath = $4;
  $$->schema = $2.schema;
  $$->tableName = $2.name;
}
| COPY select_with_paren TO file_path opt_file_type {
  $$ = new ExportStatement($5);
  $$->filePath = $4;
  $$->select = $2;
};

/******************************
 * Show Statement
 * SHOW TABLES;
 ******************************/

show_statement : SHOW VARIABLES LIKE expr opt_for_instance {
  $$ = new ShowStatement(kShowVar);
  $$->_var_name = $4->name;
  $$->instance_name = $5;
}
| SHOW VARIABLES opt_for_instance {
  $$ = new ShowStatement(kShowVar);
  $$->instance_name = $3;
}
| SHOW SESSION VARIABLES LIKE expr opt_for_instance {
  $$ = new ShowStatement(kShowVar);
  $$->_var_name = $5->name;
  $$->var_type = Session;
  $$->instance_name = $6;
}
| SHOW SESSION VARIABLES opt_for_instance {
  $$ = new ShowStatement(kShowVar);
  $$->var_type = Session;
  $$->instance_name = $4;
}
| SHOW LOCAL VARIABLES LIKE expr opt_for_instance {
  $$ = new ShowStatement(kShowVar);
  $$->_var_name = $5->name;
  $$->var_type = Local;
  $$->instance_name = $6;
}
| SHOW LOCAL VARIABLES opt_for_instance {
  $$ = new ShowStatement(kShowVar);
  $$->var_type = Local;
  $$->instance_name = $4;
}
| SHOW GLOBAL VARIABLES LIKE expr opt_for_instance {
  $$ = new ShowStatement(kShowVar);
  $$->_var_name = $5->name;
  $$->instance_name = $6;
}
| SHOW GLOBAL VARIABLES opt_for_instance {
  $$ = new ShowStatement(kShowVar);
  $$->instance_name = $4;
};

show_master_statement: SHOW MASTER STATUS {
  $$ = new ShowMasterStatusStatement();
};
show_slave_statement: SHOW SLAVE STATUS {
  $$ = new ShowSlaveStatusStatement();
};
show_binary_statement: SHOW BINARY LOGS {
  $$ = new ShowBinaryLogsStatement();
};
show_binlog_event_statement: SHOW BINLOG EVENTS opt_in opt_from opt_limit {
  $$ = new ShowBinlogEventsStatement();
  $$->binlog_file=$4;
  $$->start_pos=$5;
  $$->limit=$6;
};

show_binlog_status_statement: SHOW BINLOG STATUS opt_for_tenant {
  $$ = new ShowBinlogStatusStatement();
  $$->tenant=$4;
};

show_binlog_server_statement: SHOW BINLOG SERVER opt_for_tenant {
  $$ = new ShowBinlogServerStatement();
  $$->tenant=$4;
};

report_statement : REPORT {
  $$ = new ReportStatement();
}
| REPORT int_literal {
  $$ = new ReportStatement();
  $$->timestamp = $2;
};

show_processlist_statement: SHOW PROCESSLIST opt_for_instance {
  $$ = new ShowProcessListStatement();
  $$->instance_name = $3;
}
| SHOW FULL PROCESSLIST opt_for_instance {
 $$ = new ShowProcessListStatement();
 $$->full=true;
 $$->instance_name = $4;
};

show_dumplist_statement: SHOW BINLOG DUMPLIST opt_for_instance {
  $$ = new ShowDumpListStatement();
  $$->instance_name = $4;
}
| SHOW FULL BINLOG DUMPLIST opt_for_instance {
  $$ = new ShowDumpListStatement();
   $$->full=true;
  $$->instance_name = $5;
}

kill_statement: KILL int_literal opt_for_instance {
  $$ = new KillStatement();
  $$->process_id = $2;
  $$->instance_name = $3;
}
| KILL CONNECTION int_literal opt_for_instance {
  $$ = new KillStatement();
  $$->process_id = $3;
  $$->instance_name = $4;
}
| KILL QUERY int_literal opt_for_instance {
  $$ = new KillStatement();
  $$->only_kill_query = true;
  $$->process_id = $3;
  $$->instance_name = $4;
};

purge_binlog_statement: PURGE BINARY LOGS TO expr opt_for_tenant {
  $$ = new PurgeBinlogStatement();
  $$->binlog_file=$5;
  $$->tenant=$6;
}
| PURGE BINARY LOGS TO expr opt_for_instance {
  $$ = new PurgeBinlogStatement();
  $$->binlog_file=$5;
  $$->instance_name=$6;
}
| PURGE BINARY LOGS BEFORE expr opt_for_tenant {
  $$ = new PurgeBinlogStatement();
  $$->purge_ts=$5;
  $$->tenant=$6;
}
| PURGE BINARY LOGS BEFORE expr opt_for_instance {
  $$ = new PurgeBinlogStatement();
  $$->purge_ts=$5;
  $$->instance_name=$6;
};

opt_for_instance: FOR INSTANCE IDENTIFIER { $$ = $3; }
| /* empty */ { $$ = nullptr; };

set_statement: SET set_clause_commalist {
  $$ = new SetStatement();
  $$->sets = $2;
}
| SET binlog_instance_option_commalist {
  $$ = new SetStatement();
  $$->sets = $2;
};

set_clause_commalist : set_clause {
  $$ = new std::vector<SetClause*>();
  $$->push_back($1);
}
| set_clause_commalist ',' set_clause {
  $1->push_back($3);
  $$ = $1;
};

set_clause : IDENTIFIER '=' expr {
  $$ = new SetClause();
  $$->column = $1;
  $$->value = $3;
  $$->type = Session;
}
| AT IDENTIFIER '=' expr {
   $$ = new SetClause();
   $$->column = $2;
   $$->value = $4;
   $$->type = Session;
   $$->var_type = kUser;
 }
| AT AT IDENTIFIER '=' expr {
   $$ = new SetClause();
   $$->column = $3;
   $$->value = $5;
   $$->type = Global;
   $$->var_type = kSys;
}
| AT AT GLOBAL '.' IDENTIFIER '=' expr {
   $$ = new SetClause();
   $$->column = $5;
   $$->value = $7;
   $$->type = Global;
}
| AT AT SESSION '.' IDENTIFIER '=' expr {
   $$ = new SetClause();
   $$->column = $5;
   $$->value = $7;
   $$->type = Session;
}
| AT AT LOCAL '.' IDENTIFIER '=' expr {
   $$ = new SetClause();
   $$->column = $5;
   $$->value = $7;
   $$->type = Local;
}
| GLOBAL IDENTIFIER '=' expr {
   $$ = new SetClause();
   $$->column = $2;
   $$->value = $4;
   $$->type = Global;
}
| SESSION IDENTIFIER '=' expr {
   $$ = new SetClause();
   $$->column = $2;
   $$->value = $4;
   $$->type = Session;
}
| LOCAL IDENTIFIER '=' expr {
   $$ = new SetClause();
   $$->column = $2;
   $$->value = $4;
   $$->type = Local;
}
;

var_clause_commalist : var_clause {
  $$ = new std::vector<SetClause*>();
  $$->push_back($1);
}
| var_clause_commalist ',' var_clause {
  $1->push_back($3);
  $$ = $1;
};

var_clause : IDENTIFIER {
  $$ = new SetClause();
  $$->column = $1;
  $$->type = Session;
}
| AT IDENTIFIER {
   $$ = new SetClause();
   $$->column = $2;
   $$->type = Session;
   $$->var_type = kUser;
 }
| AT AT GLOBAL '.' IDENTIFIER {
   $$ = new SetClause();
   $$->column = $5;
   $$->type = Global;
}
| AT AT IDENTIFIER {
   $$ = new SetClause();
   $$->column = $3;
   $$->type = Global;
}
| AT AT SESSION '.' IDENTIFIER {
   $$ = new SetClause();
   $$->column = $5;
   $$->type = Session;
}
| AT AT LOCAL '.' IDENTIFIER {
   $$ = new SetClause();
   $$->column = $5;
   $$->type = Local;
}
| GLOBAL IDENTIFIER {
   $$ = new SetClause();
   $$->column = $2;
   $$->type = Global;
}
| SESSION IDENTIFIER {
   $$ = new SetClause();
   $$->column = $2;
   $$->type = Session;
}
| LOCAL IDENTIFIER {
   $$ = new SetClause();
   $$->column = $2;
   $$->type = Local;
};

set_password_statement : SET PASSWORD FOR IDENTIFIER '=' PASSWORD '(' IDENTIFIER ')' {
  $$ = new SetPasswordStatement();
  $$->user = $4;
  $$->password = $8;
}
| SET PASSWORD FOR IDENTIFIER '=' IDENTIFIER {
  $$ = new SetPasswordStatement();
  $$->user = $4;
  $$->password = $6;
};

/******************************
 * Create Statement
 * CREATE TABLE students (name TEXT, student_number INTEGER, city TEXT, grade DOUBLE)
 * CREATE TABLE students FROM TBL FILE 'test/students.tbl'
 ******************************/
create_statement : CREATE TABLE opt_not_exists table_name FROM IDENTIFIER FILE file_path {
  $$ = new CreateStatement(kCreateTableFromTbl);
  $$->ifNotExists = $3;
  $$->schema = $4.schema;
  $$->tableName = $4.name;
  if (strcasecmp($6, "tbl") != 0) {
    free($6);
    yyerror(&yyloc, result, scanner, "File type is unknown.");
    YYERROR;
  }
  free($6);
  $$->filePath = $8;
}
| CREATE TABLE opt_not_exists table_name '(' table_elem_commalist ')' {
  $$ = new CreateStatement(kCreateTable);
  $$->ifNotExists = $3;
  $$->schema = $4.schema;
  $$->tableName = $4.name;
  $$->setColumnDefsAndConstraints($6);
  delete $6;
  if (result->errorMsg()) {
    delete $$;
    YYERROR;
  }
}
| CREATE TABLE opt_not_exists table_name AS select_statement {
  $$ = new CreateStatement(kCreateTable);
  $$->ifNotExists = $3;
  $$->schema = $4.schema;
  $$->tableName = $4.name;
  $$->select = $6;
}
| CREATE INDEX opt_not_exists opt_index_name ON table_name '(' ident_commalist ')' {
  $$ = new CreateStatement(kCreateIndex);
  $$->indexName = $4;
  $$->ifNotExists = $3;
  $$->tableName = $6.name;
  $$->indexColumns = $8;
}
| CREATE VIEW opt_not_exists table_name opt_column_list AS select_statement {
  $$ = new CreateStatement(kCreateView);
  $$->ifNotExists = $3;
  $$->schema = $4.schema;
  $$->tableName = $4.name;
  $$->viewColumns = $5;
  $$->select = $7;
};

create_binlog_statement:CREATE BINLOG opt_not_exists opt_for_tenant opt_user_info opt_from binlog_option_clause {
  $$ = new CreateBinlogStatement();
  $$->is_for_not_exists = $3;
  $$->tenant = $4;
  $$->user_info = $5;
  $$->ts = $6;
  $$->binlog_options = $7;
};

alter_binlog_statement : ALTER BINLOG IDENTIFIER '.' IDENTIFIER SET binlog_instance_option_commalist {
  $$ = new AlterBinlogStatement();
  $$->tenant = new TenantName($3, $5);
  $$->instance_options = $7;
};

opt_not_exists : IF NOT EXISTS { $$ = true; }
| /* empty */ { $$ = false; };

table_elem_commalist : table_elem {
  $$ = new std::vector<TableElement*>();
  $$->push_back($1);
}
| table_elem_commalist ',' table_elem {
  $1->push_back($3);
  $$ = $1;
};

table_elem : column_def { $$ = $1; }
| table_constraint { $$ = $1; };

column_def : IDENTIFIER column_type opt_column_constraints {
  $$ = new ColumnDefinition($1, $2, $3);
  if (!$$->trySetNullableExplicit()) {
    yyerror(&yyloc, result, scanner, ("Conflicting nullability constraints for " + std::string{$1}).c_str());
  }
};

column_type : BIGINT { $$ = ColumnType{DataType::BIGINT}; }
| BOOLEAN { $$ = ColumnType{DataType::BOOLEAN}; }
| CHAR '(' INTVAL ')' { $$ = ColumnType{DataType::CHAR, $3}; }
| CHARACTER_VARYING '(' INTVAL ')' { $$ = ColumnType{DataType::VARCHAR, $3}; }
| DATE { $$ = ColumnType{DataType::DATE}; };
| DATETIME { $$ = ColumnType{DataType::DATETIME}; }
| DECIMAL opt_decimal_specification {
  $$ = ColumnType{DataType::DECIMAL, 0, $2->first, $2->second};
  delete $2;
}
| DOUBLE { $$ = ColumnType{DataType::DOUBLE}; }
| FLOAT { $$ = ColumnType{DataType::FLOAT}; }
| INT { $$ = ColumnType{DataType::INT}; }
| INTEGER { $$ = ColumnType{DataType::INT}; }
| LONG { $$ = ColumnType{DataType::LONG}; }
| REAL { $$ = ColumnType{DataType::REAL}; }
| SMALLINT { $$ = ColumnType{DataType::SMALLINT}; }
| TEXT { $$ = ColumnType{DataType::TEXT}; }
| TIME opt_time_precision { $$ = ColumnType{DataType::TIME, 0, $2}; }
| TIMESTAMP { $$ = ColumnType{DataType::DATETIME}; }
| VARCHAR '(' INTVAL ')' { $$ = ColumnType{DataType::VARCHAR, $3}; }

opt_time_precision : '(' INTVAL ')' { $$ = $2; }
| /* empty */ { $$ = 0; };

opt_decimal_specification : '(' INTVAL ',' INTVAL ')' { $$ = new std::pair<int64_t, int64_t>{$2, $4}; }
| '(' INTVAL ')' { $$ = new std::pair<int64_t, int64_t>{$2, 0}; }
| /* empty */ { $$ = new std::pair<int64_t, int64_t>{0, 0}; };

opt_column_constraints : column_constraint_set { $$ = $1; }
| /* empty */ { $$ = new std::unordered_set<ConstraintType>(); };

column_constraint_set : column_constraint {
  $$ = new std::unordered_set<ConstraintType>();
  $$->insert($1);
}
| column_constraint_set column_constraint {
  $1->insert($2);
  $$ = $1;
}

column_constraint : PRIMARY KEY { $$ = ConstraintType::PrimaryKey; }
| UNIQUE { $$ = ConstraintType::Unique; }
| NULL { $$ = ConstraintType::Null; }
| NOT NULL { $$ = ConstraintType::NotNull; };

table_constraint : PRIMARY KEY '(' ident_commalist ')' { $$ = new TableConstraint(ConstraintType::PrimaryKey, $4); }
| UNIQUE '(' ident_commalist ')' { $$ = new TableConstraint(ConstraintType::Unique, $3); };


/******************************
 * Binlog Instance Statement
 ******************************/

create_binlog_instance_statement : CREATE BINLOG INSTANCE IDENTIFIER opt_for binlog_instance_option_commalist {
  $$ = new CreateBinlogInstanceStatement();
  $$->instance_name = $4;
  $$->tenant = $5;
  $$->instance_options = $6;
};

alter_binlog_instance_statement : ALTER BINLOG INSTANCE IDENTIFIER SET binlog_instance_option_commalist {
  $$ = new AlterBinlogInstanceStatement();
  $$->instance_name = $4;
  $$->instance_options = $6;
};

binlog_instance_option_commalist : binlog_instance_option {
  $$ = new std::vector<SetClause*>();
  $$->push_back($1);
}
| binlog_instance_option_commalist ',' binlog_instance_option {
  $1->push_back($3);
  $$ = $1;
};

binlog_instance_option: obcdc_option { $$ = $1; }
| instance_binlog_option { $$ = $1; }
| slot_option { $$ = $1; };

obcdc_option : START_TIMESTAMP '=' expr {
  $$ = new SetClause();
  $$->set_column("start_timestamp");
  $$->value = $3;
  $$->type = Global;
}
| ROOTSERVER_LIST '=' expr {
  $$ = new SetClause();
  $$->set_column("rootserver_list");
  $$->value = $3;
  $$->type = Global;
}
| CLUSTER_URL '=' expr {
  $$ = new SetClause();
  $$->set_column("cluster_url");
  $$->value = $3;
  $$->type = Global;
}
| CLUSTER_USER '=' expr {
  $$ = new SetClause();
  $$->set_column("cluster_user");
  $$->value = $3;
  $$->type = Global;
}
| CLUSTER_PASSWORD '=' expr {
  $$ = new SetClause();
  $$->set_column("cluster_password");
  $$->value = $3;
  $$->type = Global;
}
| EXTRA_OBCDC_CFG '=' expr {
  $$ = new SetClause();
  $$->set_column("extra_obcdc_cfg");
  $$->value = $3;
  $$->type = Global;
};

instance_binlog_option : IDENTIFIER '=' expr {
  $$ = new SetClause();
  $$->lower_column($1);
  $$->value = $3;
  $$->type = Global;
}
| _INITIAL_OB_TXN_ID '=' expr {
  $$ = new SetClause();
  $$->set_column("initial_ob_txn_id");
  $$->value = $3;
  $$->type = Global;
}
| _INITIAL_OB_TXN_GTID_SEQ '=' expr {
  $$ = new SetClause();
  $$->set_column("initial_ob_txn_gtid_seq");
  $$->value = $3;
  $$->type = Global;
};

slot_option : IP '=' expr {
  $$ = new SetClause();
  $$->set_column("ip");
  $$->value = $3;
  $$->type = Global;
}
| ZONE '=' expr {
  $$ = new SetClause();
  $$->set_column("zone");
  $$->value = $3;
  $$->type = Global;
}
| REGION '=' expr {
  $$ = new SetClause();
  $$->set_column("region");
  $$->value = $3;
  $$->type = Global;
}
| GROUP '=' expr {
  $$ = new SetClause();
  $$->set_column("group");
  $$->value = $3;
  $$->type = Global;
};

start_binlog_instance_statement : START BINLOG INSTANCE IDENTIFIER opt_instance_flag {
 $$ = new StartBinlogInstanceStatement();
 $$->instance_name = $4;
 $$->flag = $5;
};

stop_binlog_instance_statement : STOP BINLOG INSTANCE IDENTIFIER opt_instance_flag {
 $$ = new StopBinlogInstanceStatement();
 $$->instance_name = $4;
 $$->flag = $5;
};

opt_instance_flag : PROCESS_ONLY { $$ = InstanceFlag::PROCESS_ONLY; }
| OBCDC_ONLY { $$ = InstanceFlag::OBCDC_ONLY; }
| /* empty */ { $$ = InstanceFlag::BOTH; };

drop_binlog_instance_statement : DROP BINLOG INSTANCE IDENTIFIER opt_force opt_defer {
 $$ = new DropBinlogInstanceStatement();
 $$->instance_name = $4;
 $$->force = $5;
 $$->defer_execution_sec = $6;
};

opt_force: FORCE { $$ = true; }
| /* empty */ { $$ = false; };

opt_defer: NOW { $$ = Expr::makeLiteral((int64_t) 0); }
| DEFER int_literal { $$ = $2; }
| /* empty */ { $$ = nullptr; };

switch_master_instance_statement : SWITCH MASTER INSTANCE TO IDENTIFIER opt_for_tenant {
  $$ = new SwitchMasterInstanceStatement();
  $$->instance_name = $5;
  $$->tenant_name = $6;
}
| SWITCH MASTER INSTANCE TO IDENTIFIER opt_for_tenant FULL {
  $$ = new SwitchMasterInstanceStatement();
  $$->full = true;
  $$->instance_name = $5;
  $$->tenant_name = $6;
};

show_binlog_instance_statement : SHOW BINLOG INSTANCE IDENTIFIER {
  $$ = new ShowBinlogInstanceStatement();
  $$->mode = ShowInstanceMode::INSTANCE;
  $$->instance_names = new std::vector<char*>();
  $$->instance_names->push_back($4);
}
| SHOW HISTORY BINLOG INSTANCE IDENTIFIER {
  $$ = new ShowBinlogInstanceStatement();
  $$->history = true;
  $$->mode = ShowInstanceMode::INSTANCE;
  $$->instance_names = new std::vector<char*>();
  $$->instance_names->push_back($5);
}
| SHOW BINLOG INSTANCES opt_instance_name_list opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->mode = ShowInstanceMode::INSTANCE;
  $$->instance_names = $4;
  $$->limit = $5;
}
| SHOW HISTORY BINLOG INSTANCES opt_instance_name_list opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->history = true;
  $$->mode = ShowInstanceMode::INSTANCE;
  $$->instance_names = $5;
  $$->limit = $6;
}
| SHOW BINLOG INSTANCES FOR tenant_name opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->mode = ShowInstanceMode::TENANT;
  $$->tenant = $5;
  $$->limit = $6;
}
| SHOW HISTORY BINLOG INSTANCES FOR tenant_name opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->history = true;
  $$->mode = ShowInstanceMode::TENANT;
  $$->tenant = $6;
  $$->limit = $7;
}
| SHOW BINLOG INSTANCES FROM IP IDENTIFIER opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->mode = ShowInstanceMode::IP;
  $$->ip = $6;
  $$->limit = $7;
}
| SHOW HISTORY BINLOG INSTANCES FROM IP IDENTIFIER opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->history = true;
  $$->mode = ShowInstanceMode::IP;
  $$->ip = $7;
  $$->limit = $8;
}
| SHOW BINLOG INSTANCES FROM ZONE IDENTIFIER opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->mode = ShowInstanceMode::ZONE;
  $$->zone = $6;
  $$->limit = $7;
}
| SHOW HISTORY BINLOG INSTANCES FROM ZONE IDENTIFIER opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->history = true;
  $$->mode = ShowInstanceMode::ZONE;
  $$->zone = $7;
  $$->limit = $8;
}
| SHOW BINLOG INSTANCES FROM REGION IDENTIFIER opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->mode = ShowInstanceMode::REGION;
  $$->region = $6;
  $$->limit = $7;
}
| SHOW HISTORY BINLOG INSTANCES FROM REGION IDENTIFIER opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->history = true;
  $$->mode = ShowInstanceMode::REGION;
  $$->region = $7;
  $$->limit = $8;
}
| SHOW BINLOG INSTANCES FROM GROUP IDENTIFIER opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->mode = ShowInstanceMode::GROUP;
  $$->group = $6;
  $$->limit = $7;
}
| SHOW HISTORY BINLOG INSTANCES FROM GROUP IDENTIFIER opt_limit {
  $$ = new ShowBinlogInstanceStatement();
  $$->history = true;
  $$->mode = ShowInstanceMode::GROUP;
  $$->group = $7;
  $$->limit = $8;
};

opt_instance_name_list : ident_commalist { $$ = $1; }
| /* empty */ { $$ = nullptr; };

show_nodes_statement : SHOW NODES {
  $$ = new ShowNodesStatement();
  $$->option = ShowNodeOption::NODE_ALL;
}
| SHOW NODES FROM IP IDENTIFIER {
  $$ = new ShowNodesStatement();
  $$->option = ShowNodeOption::NODE_IP;
  $$->ip = $5;
}
| SHOW NODES FROM ZONE IDENTIFIER {
  $$ = new ShowNodesStatement();
  $$->option = ShowNodeOption::NODE_ZONE;
  $$->zone = $5;
}
| SHOW NODES FROM REGION IDENTIFIER {
  $$ = new ShowNodesStatement();
  $$->option = ShowNodeOption::NODE_REGION;
  $$->region = $5;
}
| SHOW NODES FROM GROUP IDENTIFIER {
  $$ = new ShowNodesStatement();
  $$->option = ShowNodeOption::NODE_GROUP;
  $$->group = $5;
};

/******************************
 * Drop Statement
 * DROP TABLE students;
 * DEALLOCATE PREPARE stmt;
 ******************************/

drop_statement : DROP TABLE opt_exists table_name {
  $$ = new DropStatement(kDropTable);
  $$->ifExists = $3;
  $$->schema = $4.schema;
  $$->name = $4.name;
}
| DROP VIEW opt_exists table_name {
  $$ = new DropStatement(kDropView);
  $$->ifExists = $3;
  $$->schema = $4.schema;
  $$->name = $4.name;
}
| DEALLOCATE PREPARE IDENTIFIER {
  $$ = new DropStatement(kDropPreparedStatement);
  $$->ifExists = false;
  $$->name = $3;
}

| DROP INDEX opt_exists IDENTIFIER {
  $$ = new DropStatement(kDropIndex);
  $$->ifExists = $3;
  $$->indexName = $4;
};

drop_binlog_statement:DROP BINLOG opt_exists opt_for_tenant opt_defer {
  $$ = new DropBinlogStatement();
  $$->if_exists = $3;
  $$->tenant_info = $4;
  $$->defer_execution_sec = $5;
}

opt_exists : IF EXISTS { $$ = true; }
| /* empty */ { $$ = false; };

/******************************
 * ALTER Statement
 * ALTER TABLE students DROP COLUMN name;
 ******************************/

alter_statement : ALTER TABLE opt_exists table_name alter_action {
  $$ = new AlterStatement($4.name, $5);
  $$->ifTableExists = $3;
  $$->schema = $4.schema;
};

alter_action : drop_action { $$ = $1; }

drop_action : DROP COLUMN opt_exists IDENTIFIER {
  $$ = new DropColumnAction($4);
  $$->ifExists = $3;
};

/******************************
 * Delete Statement / Truncate statement
 * DELETE FROM students WHERE grade > 3.0
 * DELETE FROM students <=> TRUNCATE students
 ******************************/
delete_statement : DELETE FROM table_name opt_where {
  $$ = new DeleteStatement();
  $$->schema = $3.schema;
  $$->tableName = $3.name;
  $$->expr = $4;
};

truncate_statement : TRUNCATE table_name {
  $$ = new DeleteStatement();
  $$->schema = $2.schema;
  $$->tableName = $2.name;
};

/******************************
 * Insert Statement
 * INSERT INTO students VALUES ('Max', 1112233, 'Musterhausen', 2.3)
 * INSERT INTO employees SELECT * FROM stundents
 ******************************/
insert_statement : INSERT INTO table_name opt_column_list VALUES '(' literal_list ')' {
  $$ = new InsertStatement(kInsertValues);
  $$->schema = $3.schema;
  $$->tableName = $3.name;
  $$->columns = $4;
  $$->values = $7;
}
| INSERT INTO table_name opt_column_list select_no_paren {
  $$ = new InsertStatement(kInsertSelect);
  $$->schema = $3.schema;
  $$->tableName = $3.name;
  $$->columns = $4;
  $$->select = $5;
};

opt_column_list : '(' ident_commalist ')' { $$ = $2; }
| /* empty */ { $$ = nullptr; };

/******************************
 * Update Statement
 * UPDATE students SET grade = 1.3, name='Felix Fürstenberg' WHERE name = 'Max Mustermann';
 ******************************/

update_statement : UPDATE table_ref_name_no_alias SET update_clause_commalist opt_where {
  $$ = new UpdateStatement();
  $$->table = $2;
  $$->updates = $4;
  $$->where = $5;
};

update_clause_commalist : update_clause {
  $$ = new std::vector<UpdateClause*>();
  $$->push_back($1);
}
| update_clause_commalist ',' update_clause {
  $1->push_back($3);
  $$ = $1;
};

update_clause : IDENTIFIER '=' expr {
  $$ = new UpdateClause();
  $$->column = $1;
  $$->value = $3;
};

/******************************
 * Select Statement
 ******************************/

select_statement : opt_with_clause select_with_paren {
  $$ = $2;
  $$->withDescriptions = $1;
}
| opt_with_clause select_no_paren {
  $$ = $2;
  $$->withDescriptions = $1;
}
| opt_with_clause select_with_paren set_operator select_within_set_operation opt_order opt_limit {
  $$ = $2;
  if ($$->setOperations == nullptr) {
    $$->setOperations = new std::vector<SetOperation*>();
  }
  $$->setOperations->push_back($3);
  $$->setOperations->back()->nestedSelectStatement = $4;
  $$->setOperations->back()->resultOrder = $5;
  $$->setOperations->back()->resultLimit = $6;
  $$->withDescriptions = $1;
};

select_within_set_operation : select_with_paren | select_within_set_operation_no_parentheses;

select_within_set_operation_no_parentheses : select_clause { $$ = $1; }
| select_clause set_operator select_within_set_operation {
  $$ = $1;
  if ($$->setOperations == nullptr) {
    $$->setOperations = new std::vector<SetOperation*>();
  }
  $$->setOperations->push_back($2);
  $$->setOperations->back()->nestedSelectStatement = $3;
};

select_with_paren : '(' select_no_paren ')' { $$ = $2; }
| '(' select_with_paren ')' { $$ = $2; };

select_no_paren : select_clause opt_order opt_limit opt_locking_clause {
  $$ = $1;
  $$->order = $2;

  // Limit could have been set by TOP.
  if ($3) {
    delete $$->limit;
    $$->limit = $3;
  }

  if ($4) {
    $$->lockings = $4;
  }
}
| select_clause set_operator select_within_set_operation opt_order opt_limit opt_locking_clause {
  $$ = $1;
  if ($$->setOperations == nullptr) {
    $$->setOperations = new std::vector<SetOperation*>();
  }
  $$->setOperations->push_back($2);
  $$->setOperations->back()->nestedSelectStatement = $3;
  $$->setOperations->back()->resultOrder = $4;
  $$->setOperations->back()->resultLimit = $5;
  $$->lockings = $6;
};

set_operator : set_type opt_all {
  $$ = $1;
  $$->isAll = $2;
};

set_type : UNION {
  $$ = new SetOperation();
  $$->setType = SetType::kSetUnion;
}
| INTERSECT {
  $$ = new SetOperation();
  $$->setType = SetType::kSetIntersect;
}
| EXCEPT {
  $$ = new SetOperation();
  $$->setType = SetType::kSetExcept;
};

opt_all : ALL { $$ = true; }
| /* empty */ { $$ = false; };

select_clause : SELECT opt_top opt_distinct select_list opt_from_clause opt_where opt_group {
  $$ = new SelectStatement();
  $$->limit = $2;
  $$->selectDistinct = $3;
  $$->selectList = $4;
  $$->fromTable = $5;
  $$->whereClause = $6;
  $$->groupBy = $7;
};

opt_distinct : DISTINCT { $$ = true; }
| /* empty */ { $$ = false; };

select_list : expr_list;

opt_from_clause : from_clause { $$ = $1; }
| /* empty */ { $$ = nullptr; };

from_clause : FROM table_ref { $$ = $2; };

opt_from :FROM literal { $$ = $2; }
| /* empty */ { $$ = nullptr; };

opt_user_info :TO USER IDENTIFIER PASSWORD IDENTIFIER {$$ = new UserInfo($3, $5);}
| /* empty */ { $$ = nullptr; };

opt_where : WHERE expr { $$ = $2; }
| /* empty */ { $$ = nullptr; };

opt_group : GROUP BY expr_list opt_having {
  $$ = new GroupByDescription();
  $$->columns = $3;
  $$->having = $4;
}
| /* empty */ { $$ = nullptr; };

opt_having : HAVING expr { $$ = $2; }
| /* empty */ { $$ = nullptr; };

opt_order : ORDER BY order_list { $$ = $3; }
| /* empty */ { $$ = nullptr; };

order_list : order_desc {
  $$ = new std::vector<OrderDescription*>();
  $$->push_back($1);
}
| order_list ',' order_desc {
  $1->push_back($3);
  $$ = $1;
};

order_desc : expr opt_order_type { $$ = new OrderDescription($2, $1); };

opt_order_type : ASC { $$ = kOrderAsc; }
| DESC { $$ = kOrderDesc; }
| /* empty */ { $$ = kOrderAsc; };

// TODO: TOP and LIMIT can take more than just int literals.

opt_top : TOP int_literal { $$ = new LimitDescription($2, nullptr); }
| /* empty */ { $$ = nullptr; };

opt_limit : LIMIT expr { $$ = new LimitDescription($2, nullptr); }
| OFFSET expr { $$ = new LimitDescription(nullptr, $2); }
| LIMIT expr OFFSET expr { $$ = new LimitDescription($2, $4); }
| LIMIT expr ',' expr { $$ = new LimitDescription($4, $2); }
| LIMIT ALL { $$ = new LimitDescription(nullptr, nullptr); }
| LIMIT ALL OFFSET expr { $$ = new LimitDescription(nullptr, $4); }
| /* empty */ { $$ = nullptr; };

/******************************
 * Expressions
 ******************************/
expr_list : expr_alias {
  $$ = new std::vector<Expr*>();
  $$->push_back($1);
}
| expr_list ',' expr_alias {
  $1->push_back($3);
  $$ = $1;
};

opt_literal_list : literal_list { $$ = $1; }
| /* empty */ { $$ = nullptr; };

literal_list : literal {
  $$ = new std::vector<Expr*>();
  $$->push_back($1);
}
| literal_list ',' literal {
  $1->push_back($3);
  $$ = $1;
};

expr_alias : expr opt_alias {
  $$ = $1;
  if ($2) {
    $$->alias = strdup($2->name);
    delete $2;
  }
};

expr : operand | between_expr | logic_expr | exists_expr | in_expr;

operand : '(' expr ')' { $$ = $2; }
| array_index | scalar_expr | unary_expr | binary_expr | case_expr | function_expr | extract_expr | cast_expr | var_expr |
    array_expr | '(' select_no_paren ')' {
  $$ = Expr::makeSelect($2);
};

scalar_expr : column_name | literal;

unary_expr : '-' operand { $$ = Expr::makeOpUnary(kOpUnaryMinus, $2); }
| NOT operand { $$ = Expr::makeOpUnary(kOpNot, $2); }
| operand ISNULL { $$ = Expr::makeOpUnary(kOpIsNull, $1); }
| operand IS NULL { $$ = Expr::makeOpUnary(kOpIsNull, $1); }
| operand IS NOT NULL { $$ = Expr::makeOpUnary(kOpNot, Expr::makeOpUnary(kOpIsNull, $1)); };

binary_expr : comp_expr | operand '-' operand { $$ = Expr::makeOpBinary($1, kOpMinus, $3); }
| operand '+' operand { $$ = Expr::makeOpBinary($1, kOpPlus, $3); }
| operand '/' operand { $$ = Expr::makeOpBinary($1, kOpSlash, $3); }
| operand '*' operand { $$ = Expr::makeOpBinary($1, kOpAsterisk, $3); }
| operand '%' operand { $$ = Expr::makeOpBinary($1, kOpPercentage, $3); }
| operand '^' operand { $$ = Expr::makeOpBinary($1, kOpCaret, $3); }
| operand LIKE operand { $$ = Expr::makeOpBinary($1, kOpLike, $3); }
| operand NOT LIKE operand { $$ = Expr::makeOpBinary($1, kOpNotLike, $4); }
| operand ILIKE operand { $$ = Expr::makeOpBinary($1, kOpILike, $3); }
| operand CONCAT operand { $$ = Expr::makeOpBinary($1, kOpConcat, $3); };

logic_expr : expr AND expr { $$ = Expr::makeOpBinary($1, kOpAnd, $3); }
| expr OR expr { $$ = Expr::makeOpBinary($1, kOpOr, $3); };

in_expr : operand IN '(' expr_list ')' { $$ = Expr::makeInOperator($1, $4); }
| operand NOT IN '(' expr_list ')' { $$ = Expr::makeOpUnary(kOpNot, Expr::makeInOperator($1, $5)); }
| operand IN '(' select_no_paren ')' { $$ = Expr::makeInOperator($1, $4); }
| operand NOT IN '(' select_no_paren ')' { $$ = Expr::makeOpUnary(kOpNot, Expr::makeInOperator($1, $5)); };

// CASE grammar based on: flex & bison by John Levine
// https://www.safaribooksonline.com/library/view/flex-bison/9780596805418/ch04.html#id352665
case_expr : CASE expr case_list END { $$ = Expr::makeCase($2, $3, nullptr); }
| CASE expr case_list ELSE expr END { $$ = Expr::makeCase($2, $3, $5); }
| CASE case_list END { $$ = Expr::makeCase(nullptr, $2, nullptr); }
| CASE case_list ELSE expr END { $$ = Expr::makeCase(nullptr, $2, $4); };

case_list : WHEN expr THEN expr { $$ = Expr::makeCaseList(Expr::makeCaseListElement($2, $4)); }
| case_list WHEN expr THEN expr { $$ = Expr::caseListAppend($1, Expr::makeCaseListElement($3, $5)); };

exists_expr : EXISTS '(' select_no_paren ')' { $$ = Expr::makeExists($3); }
| NOT EXISTS '(' select_no_paren ')' { $$ = Expr::makeOpUnary(kOpNot, Expr::makeExists($4)); };

comp_expr : operand '=' operand { $$ = Expr::makeOpBinary($1, kOpEquals, $3); }
| operand EQUALS operand { $$ = Expr::makeOpBinary($1, kOpEquals, $3); }
| operand NOTEQUALS operand { $$ = Expr::makeOpBinary($1, kOpNotEquals, $3); }
| operand '<' operand { $$ = Expr::makeOpBinary($1, kOpLess, $3); }
| operand '>' operand { $$ = Expr::makeOpBinary($1, kOpGreater, $3); }
| operand LESSEQ operand { $$ = Expr::makeOpBinary($1, kOpLessEq, $3); }
| operand GREATEREQ operand { $$ = Expr::makeOpBinary($1, kOpGreaterEq, $3); };

function_expr : IDENTIFIER '(' ')' { $$ = Expr::makeFunctionRef($1, new std::vector<Expr*>(), false); }
| IDENTIFIER '(' opt_distinct expr_list ')' { $$ = Expr::makeFunctionRef($1, $4, $3); };

extract_expr : EXTRACT '(' datetime_field FROM expr ')' { $$ = Expr::makeExtract($3, $5); };

cast_expr : CAST '(' expr AS column_type ')' { $$ = Expr::makeCast($3, $5); };

var_expr : var_clause { $$ = Expr::makeVar($1); };

datetime_field : SECOND { $$ = kDatetimeSecond; }
| MINUTE { $$ = kDatetimeMinute; }
| HOUR { $$ = kDatetimeHour; }
| DAY { $$ = kDatetimeDay; }
| MONTH { $$ = kDatetimeMonth; }
| YEAR { $$ = kDatetimeYear; };

datetime_field_plural : SECONDS { $$ = kDatetimeSecond; }
| MINUTES { $$ = kDatetimeMinute; }
| HOURS { $$ = kDatetimeHour; }
| DAYS { $$ = kDatetimeDay; }
| MONTHS { $$ = kDatetimeMonth; }
| YEARS { $$ = kDatetimeYear; };

duration_field : datetime_field | datetime_field_plural;

array_expr : ARRAY '[' expr_list ']' { $$ = Expr::makeArray($3); };

array_index : operand '[' int_literal ']' { $$ = Expr::makeArrayIndex($1, $3->ival); };

between_expr : operand BETWEEN operand AND operand { $$ = Expr::makeBetween($1, $3, $5); };

column_name : IDENTIFIER { $$ = Expr::makeColumnRef($1); }
| IDENTIFIER '.' IDENTIFIER { $$ = Expr::makeColumnRef($1, $3); }
| '*' { $$ = Expr::makeStar(); }
| IDENTIFIER '.' '*' { $$ = Expr::makeStar($1); };

literal : string_literal | bool_literal | num_literal | null_literal | date_literal | interval_literal | param_expr;

string_literal : STRING { $$ = Expr::makeLiteral($1); };

bool_literal : TRUE { $$ = Expr::makeLiteral(true); }
| FALSE { $$ = Expr::makeLiteral(false); };

num_literal : FLOATVAL { $$ = Expr::makeLiteral($1); }
| int_literal;

int_literal : INTVAL { $$ = Expr::makeLiteral($1); };

null_literal : NULL { $$ = Expr::makeNullLiteral(); };

date_literal : DATE STRING {
  int day{0}, month{0}, year{0}, chars_parsed{0};
  // If the whole string is parsed, chars_parsed points to the terminating null byte after the last character
  if (sscanf($2, "%4d-%2d-%2d%n", &day, &month, &year, &chars_parsed) != 3 || $2[chars_parsed] != 0) {
    free($2);
    yyerror(&yyloc, result, scanner, "Found incorrect date format. Expected format: YYYY-MM-DD");
    YYERROR;
  }
  $$ = Expr::makeDateLiteral($2);
};

interval_literal : int_literal duration_field {
  $$ = Expr::makeIntervalLiteral($1->ival, $2);
  delete $1;
}
| INTERVAL STRING datetime_field {
  int duration{0}, chars_parsed{0};
  // If the whole string is parsed, chars_parsed points to the terminating null byte after the last character
  if (sscanf($2, "%d%n", &duration, &chars_parsed) != 1 || $2[chars_parsed] != 0) {
    free($2);
    yyerror(&yyloc, result, scanner, "Found incorrect interval format. Expected format: INTEGER");
    YYERROR;
  }
  free($2);
  $$ = Expr::makeIntervalLiteral(duration, $3);
}
| INTERVAL STRING {
  int duration{0}, chars_parsed{0};
  // 'seconds' and 'minutes' are the longest accepted interval qualifiers (7 chars) + null byte
  char unit_string[8];
  // If the whole string is parsed, chars_parsed points to the terminating null byte after the last character
  if (sscanf($2, "%d %7s%n", &duration, unit_string, &chars_parsed) != 2 || $2[chars_parsed] != 0) {
    free($2);
    yyerror(&yyloc, result, scanner, "Found incorrect interval format. Expected format: INTEGER INTERVAL_QUALIIFIER");
    YYERROR;
  }
  free($2);

  DatetimeField unit;
  if (strcasecmp(unit_string, "second") == 0 || strcasecmp(unit_string, "seconds") == 0) {
    unit = kDatetimeSecond;
  } else if (strcasecmp(unit_string, "minute") == 0 || strcasecmp(unit_string, "minutes") == 0) {
    unit = kDatetimeMinute;
  } else if (strcasecmp(unit_string, "hour") == 0 || strcasecmp(unit_string, "hours") == 0) {
    unit = kDatetimeHour;
  } else if (strcasecmp(unit_string, "day") == 0 || strcasecmp(unit_string, "days") == 0) {
    unit = kDatetimeDay;
  } else if (strcasecmp(unit_string, "month") == 0 || strcasecmp(unit_string, "months") == 0) {
    unit = kDatetimeMonth;
  } else if (strcasecmp(unit_string, "year") == 0 || strcasecmp(unit_string, "years") == 0) {
    unit = kDatetimeYear;
  } else {
    yyerror(&yyloc, result, scanner, "Interval qualifier is unknown.");
    YYERROR;
  }
  $$ = Expr::makeIntervalLiteral(duration, unit);
};

param_expr : '?' {
  $$ = Expr::makeParameter(yylloc.total_column);
  $$->ival2 = yyloc.param_list.size();
  yyloc.param_list.push_back($$);
};

/******************************
 * Table
 ******************************/
table_ref : table_ref_atomic | table_ref_commalist ',' table_ref_atomic {
  $1->push_back($3);
  auto tbl = new TableRef(kTableCrossProduct);
  tbl->list = $1;
  $$ = tbl;
};

table_ref_atomic : nonjoin_table_ref_atomic | join_clause;

nonjoin_table_ref_atomic : table_ref_name | '(' select_statement ')' opt_table_alias {
  auto tbl = new TableRef(kTableSelect);
  tbl->select = $2;
  tbl->alias = $4;
  $$ = tbl;
};

table_ref_commalist : table_ref_atomic {
  $$ = new std::vector<TableRef*>();
  $$->push_back($1);
}
| table_ref_commalist ',' table_ref_atomic {
  $1->push_back($3);
  $$ = $1;
};

table_ref_name : table_name opt_table_alias {
  auto tbl = new TableRef(kTableName);
  tbl->schema = $1.schema;
  tbl->name = $1.name;
  tbl->alias = $2;
  $$ = tbl;
};

table_ref_name_no_alias : table_name {
  $$ = new TableRef(kTableName);
  $$->schema = $1.schema;
  $$->name = $1.name;
};

table_name : IDENTIFIER {
  $$.schema = nullptr;
  $$.name = $1;
}
| IDENTIFIER '.' IDENTIFIER {
  $$.schema = $1;
  $$.name = $3;
};

tenant_name: IDENTIFIER '.' IDENTIFIER{
  $$ = new TenantName($1, $3);
};

opt_index_name : IDENTIFIER { $$ = $1; }
| /* empty */ { $$ = nullptr; };

opt_in : IN expr { $$ = $2; }
| /* empty */ { $$ = nullptr; };

opt_for_tenant : FOR TENANT IDENTIFIER '.' IDENTIFIER {$$ = new TenantName($3, $5);}
| /* empty */ { $$ = nullptr; };

opt_for : FOR IDENTIFIER '.' IDENTIFIER {$$ = new TenantName($2, $4);}
| /* empty */ { $$ = nullptr; };

table_alias : alias | AS IDENTIFIER '(' ident_commalist ')' { $$ = new Alias($2, $4); };

opt_table_alias : table_alias | /* empty */ { $$ = nullptr; };

alias : AS IDENTIFIER { $$ = new Alias($2); }
| IDENTIFIER { $$ = new Alias($1); };

opt_alias : alias | /* empty */ { $$ = nullptr; };

/******************************
 * Row Locking Descriptions
 ******************************/

opt_locking_clause : opt_locking_clause_list { $$ = $1; }
| /* empty */ { $$ = nullptr; };

opt_locking_clause_list : locking_clause {
  $$ = new std::vector<LockingClause*>();
  $$->push_back($1);
}
| opt_locking_clause_list locking_clause {
  $1->push_back($2);
  $$ = $1;
};

locking_clause : FOR row_lock_mode opt_row_lock_policy {
  $$ = new LockingClause();
  $$->rowLockMode = $2;
  $$->rowLockWaitPolicy = $3;
  $$->tables = nullptr;
}
| FOR row_lock_mode OF ident_commalist opt_row_lock_policy {
  $$ = new LockingClause();
  $$->rowLockMode = $2;
  $$->tables = $4;
  $$->rowLockWaitPolicy = $5;
};

row_lock_mode : UPDATE { $$ = RowLockMode::ForUpdate; }
| NO KEY UPDATE { $$ = RowLockMode::ForNoKeyUpdate; }
| SHARE { $$ = RowLockMode::ForShare; }
| KEY SHARE { $$ = RowLockMode::ForKeyShare; };

opt_row_lock_policy : SKIP LOCKED { $$ = RowLockWaitPolicy::SkipLocked; }
| NOWAIT { $$ = RowLockWaitPolicy::NoWait; }
| /* empty */ { $$ = RowLockWaitPolicy::None; };

/******************************
 * With Descriptions
 ******************************/

opt_with_clause : with_clause | /* empty */ { $$ = nullptr; };

with_clause : WITH with_description_list { $$ = $2; };

with_description_list : with_description {
  $$ = new std::vector<WithDescription*>();
  $$->push_back($1);
}
| with_description_list ',' with_description {
  $1->push_back($3);
  $$ = $1;
};

with_description : IDENTIFIER AS select_with_paren {
  $$ = new WithDescription();
  $$->alias = $1;
  $$->select = $3;
};

/******************************
 * With Binlog Options
 ******************************/

binlog_option_clause: with_binlog_clause | /* empty */ { $$ = nullptr; };

with_binlog_clause: WITH binlog_option_list { $$ = $2; };

binlog_option_list: binlog_option {
  $$ = new std::vector<SetClause*>();
  $$->push_back($1);
}
| binlog_option_list ',' binlog_option {
  $1->push_back($3);
  $$ = $1;
}

binlog_option: SERVER UUID expr {
  $$ = new SetClause();
  $$->set_column("master_server_uuid");
  $$->value = $3;
  $$->type = Global;
}
| INITIAL_TRX_GTID_SEQ expr {
  $$ = new SetClause();
  $$->set_column("initial_ob_txn_gtid_seq");
  $$->value = $2;
  $$->type = Global;
}
| INITIAL_TRX_XID expr {
  $$ = new SetClause();
  $$->set_column("initial_ob_txn_id");
  $$->value = $2;
  $$->type = Global;
}
| CLUSTER URL expr {
  $$ = new SetClause();
  $$->set_column("cluster_url");
  $$->value = $3;
  $$->type = Global;
}
| REPLICATE NUM expr {
  $$ = new SetClause();
  $$->set_column("replicate_num");
  $$->value = $3;
  $$->type = Global;
}
| IDENTIFIER '=' expr {
  $$ = new SetClause();
  $$->column = $1;
  $$->value = $3;
  $$->type = Global;
};

/******************************
 * Join Statements
 ******************************/

join_clause : table_ref_atomic NATURAL JOIN nonjoin_table_ref_atomic {
  $$ = new TableRef(kTableJoin);
  $$->join = new JoinDefinition();
  $$->join->type = kJoinNatural;
  $$->join->left = $1;
  $$->join->right = $4;
}
| table_ref_atomic opt_join_type JOIN table_ref_atomic ON join_condition {
  $$ = new TableRef(kTableJoin);
  $$->join = new JoinDefinition();
  $$->join->type = (JoinType)$2;
  $$->join->left = $1;
  $$->join->right = $4;
  $$->join->condition = $6;
}
| table_ref_atomic opt_join_type JOIN table_ref_atomic USING '(' column_name ')' {
  $$ = new TableRef(kTableJoin);
  $$->join = new JoinDefinition();
  $$->join->type = (JoinType)$2;
  $$->join->left = $1;
  $$->join->right = $4;
  auto left_col = Expr::makeColumnRef(strdup($7->name));
  if ($7->alias) {
    left_col->alias = strdup($7->alias);
  }
  if ($1->getName()) {
    left_col->table = strdup($1->getName());
  }
  auto right_col = Expr::makeColumnRef(strdup($7->name));
  if ($7->alias) {
    right_col->alias = strdup($7->alias);
  }
  if ($4->getName()) {
    right_col->table = strdup($4->getName());
  }
  $$->join->condition = Expr::makeOpBinary(left_col, kOpEquals, right_col);
  delete $7;
};

opt_join_type : INNER { $$ = kJoinInner; }
| LEFT OUTER { $$ = kJoinLeft; }
| LEFT { $$ = kJoinLeft; }
| RIGHT OUTER { $$ = kJoinRight; }
| RIGHT { $$ = kJoinRight; }
| FULL OUTER { $$ = kJoinFull; }
| OUTER { $$ = kJoinFull; }
| FULL { $$ = kJoinFull; }
| CROSS { $$ = kJoinCross; }
| /* empty, default */ { $$ = kJoinInner; };

join_condition : expr;

/******************************
 * Misc
 ******************************/

opt_semicolon : ';' | /* empty */
    ;

ident_commalist : IDENTIFIER {
  $$ = new std::vector<char*>();
  $$->push_back($1);
}
| ident_commalist ',' IDENTIFIER {
  $1->push_back($3);
  $$ = $1;
};

// clang-format off
%%
    // clang-format on
    /*********************************
 ** Section 4: Additional C code
 *********************************/

    /* empty */
