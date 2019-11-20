#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

#define size_of_attribute(Struct, Attribute) sizeof( ((Struct*)0)->Attribute )

#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100

// 输入缓存
typedef struct
{
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

// 元命令结果 
typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

// 准备结果
typedef enum
{
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

// 语句类型
typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT  
} StatementType;

// 语句
typedef struct
{
    StatementType type;  // 语句类型
    Row row_to_insert;   // 插入行的结构
} Statement;

// 分页器
typedef struct
{
    int file_descriptor;
    uint32_t file_length; 
    void* pages[TABLE_MAX_PAGES];
} Pager;

// 行
typedef struct
{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

// 表
typedef struct
{
    Pager *pager;
    uint32_t num_rows;
} Table;

// 命令执行结果
typedef enum
{
    EXECUTE_SUCCESS,
    EXECUTE_ERROR
} ExecuteResult;

// 游标
typedef struct
{
    Table *table;       // 只需要行数
    uint32_t row_num;   // 游标
    bool end_of_table;  // 是否到表尾
} Cursor;
