#include "main.h"

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

// 公共节点头部布局
// 1. 节点类型
// 2. 是否是根节点
// 3. 父节点指针
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// 叶子节点头部布局
// 单元的数量
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// 叶子节点体布局
// 1. 键
// 2. 数据
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

InputBuffer* new_input_buffer();
void print_prompt();
void read_input(InputBuffer* input_buffer);
void close_input_buffer(InputBuffer* input_buffer);
Table* db_open(const char* filename);
void db_close(Table* table);
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table* table);
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement);
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement);
void serialize_row(Row* source, void* destination);
void deserialize_row(void* source, Row* destination);
void* get_page(Pager* pager, uint32_t page_num);
void* row_slot(Table* table, uint32_t row_num);
ExecuteResult execute_insert(Statement* statement, Table* table);
void print_row(Row* row);
ExecuteResult execute_select(Statement* statement, Table* table);
Pager* pager_open(const char* filename);
void pager_flush(Pager* pager, uint32_t i);
void free_table(Table* table);
ExecuteResult execute_statement(Statement* statement, Table* table);
Cursor* table_start(Table* table);
Cursor* table_end(Table* table);
void cursor_advance(Cursor* cursor);
void print_constants();
void print_leaf_node(void* node);

uint32_t* leaf_node_num_cells(void* node);
uint32_t* leaf_node_cell(void* node, uint32_t cell_num);
uint32_t* leaf_node_key(void* node, uint32_t cell_num);
void* leaf_node_value(void* node, uint32_t cell_num);
void initialize_leaf_node(void* node);
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value);

// 创建输入缓存
InputBuffer* new_input_buffer()
{
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

// 命令前显示
void print_prompt()
{
    printf("db > ");
}

// 读取一行输入到输入缓存
void read_input(InputBuffer* input_buffer)
{
    // lineptr：指向存放该行字符的指针，如果是NULL，则有系统帮助malloc，请在使用完成后free释放。
    // n：如果是由系统malloc的指针，请填0
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if(bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // 删除换行符
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

// 释放输入缓存
void close_input_buffer(InputBuffer* input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}

// 关闭数据库
void db_close(Table* table)
{
    Pager* pager = table->pager;

    // 1. 将完整页存入磁盘，并释放内存
    for(uint32_t i = 0; i < pager->num_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }

        pager_flush(pager, i); // 将整页存入磁盘

        free(pager->pages[i]); // 释放内存
        pager->pages[i] = NULL;
    }

    // 2. 关闭文件
    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    // 3. 释放内存
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}

// 执行元命令
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table* table)
{
    // 退出
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        // 1. 关闭数据库
        db_close(table);
        // 2. 释放输入缓存
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    }
    else if (strcmp(input_buffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }
    else if (strcmp(input_buffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_leaf_node(get_page(table->pager, 0));
        return META_COMMAND_SUCCESS;
    }
    else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

// 准备插入
// 从输入缓存中解析出插入数据
PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement)
{
    // 1. 设置类型
    statement->type = STATEMENT_INSERT;

    // 2. 分离输入
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    // 3. 校验输入
    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    // 4. 设置数据
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    return PREPARE_SUCCESS;
}

// 准备语句
// 根据输入首个词，确认不同操作，并分别进行解析
PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

// 序列化行
void serialize_row(Row* source, void* destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

// 反序列化行
void deserialize_row(void* source, Row* destination)
{
    memcpy(&(destination->id), source+ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source+USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source+EMAIL_OFFSET, EMAIL_SIZE);
}

// 根据页数获取页地址，如果未空，从磁盘中读
void* get_page(Pager* pager, uint32_t page_num)
{
    // 1. 页数是否超限
    if(page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    // 2. 如果内存中页内容为空，从磁盘中读取
    if(pager->pages[page_num] == NULL) {
        // 创建页内存
        void* page = malloc(PAGE_SIZE);
        // 计算磁盘文件中页数
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        // 从磁盘中读取数据
        if (page_num  <= num_pages) {
            // 移动到第几页
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            // 读取整页
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if(bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;

        // 如果获取的页数大于等于记录的页数，增加页
        if (page_num >= pager->num_pages) {
           pager->num_pages = page_num + 1;
        }
    }

    return pager->pages[page_num];
}

// 根据行数返回数据地址
void* cursor_value(Cursor* cursor)
{
    printf("cursor_value page_num:%d cell_num:%d\n", cursor->page_num, cursor->cell_num);
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

// 将行插入到表中
ExecuteResult execute_insert(Statement* statement, Table* table)
{
    Row* row_to_insert = &(statement->row_to_insert);
    // 2. 创建游标
    Cursor* cursor = table_end(table);

    void* node = get_page(table->pager, table->root_page_num);
    if ((*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS)) {
        return EXECUTE_ERROR;
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    // 5. 释放游标
    free(cursor);
    return EXECUTE_SUCCESS;
}

// 打印数据
void print_row(Row* row)
{
    printf("(%d %s %s)\n", row->id, row->username, row->email);
}

void print_constants()
{
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void print_leaf_node(void* node)
{
    uint32_t num_cells = *leaf_node_num_cells(node);
    printf("leaf (size %d)\n", num_cells);
    for (uint32_t i = 0; i < num_cells; i++) {
        uint32_t key = *leaf_node_key(node, i);
        printf("  - %d : %d\n", i, key);
    }
}

// 获取数据
ExecuteResult execute_select(Statement* statement, Table* table)
{
    Cursor* cursor = table_start(table);
    Row row;

    while(!cursor->end_of_table) {
        // 根据游标，反序列化
        deserialize_row(cursor_value(cursor), &row);
        // 打印
        print_row(&row);
        // 移动游标
        cursor_advance(cursor);
    }

    free(cursor);

    return EXECUTE_SUCCESS;
}

// 从文件中读取数据到内存
Pager* pager_open(const char* filename)
{
    // 1. 打开文件
    int fd = open(filename,
                  O_RDWR | O_CREAT,
                  S_IWUSR | S_IRUSR
                  ); // S_IWUSR  00200 user has write permission, 00400 user has read permission
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    // 2. 移动到文件尾
    off_t file_length = lseek(fd, 0, SEEK_END);
    printf("pager_open file length:%lld\n", file_length);
    // 整页保存，大小只能为4k的倍数
    if (file_length % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }

    // 3. 初始化分页器
    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

// 打开数据库
// 打开文件，初始化分页器
// 使用分页器初始化表
Table* db_open(const char* filename)
{
    // 从文件中初始化分页器
    Pager* pager = pager_open(filename);

    Table* table = malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;

    if(pager->num_pages == 0) {
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }

    return table;
}

// 将分页器数据存入磁盘
// pager:       分页器
// page_num:    第几页
void pager_flush(Pager* pager, uint32_t page_num)
{
    printf("pager_flush page_num:%d\n", page_num);
    // 1. 判断数据是否为空
    if(pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    // 2. 移动到第n页头
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    // 3. 写入数据
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

// 释放表
void free_table(Table* table)
{
    for (uint32_t i = 0; i < TABLE_MAX_ROWS; i ++) {
        free(table->pager->pages);
    }
    free(table->pager);
    free(table);
}

// 根据类型执行操作
ExecuteResult execute_statement(Statement* statement, Table* table)
{
    switch(statement->type) {
        case (STATEMENT_INSERT):
            printf("This is where we would do an insert.\n");
            printf("> insert %d %s %s\n", statement->row_to_insert.id, statement->row_to_insert.username, statement->row_to_insert.email);
            return execute_insert(statement, table);
            break;
        case (STATEMENT_SELECT):
            printf("This is where we would do a select.\n");
            return execute_select(statement, table);
            break;
    }
}

// 创建开始游标
Cursor* table_start(Table* table)
{
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;

    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);
    return cursor;
}

// 创建结束游标
Cursor* table_end(Table* table)
{
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;

    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->cell_num = num_cells;
    cursor->end_of_table = true;
    return cursor;
}

// 向前移动游标
void cursor_advance(Cursor* cursor)
{
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
        cursor->end_of_table = true;
    }
}

// 叶子节点单元的数量
uint32_t* leaf_node_num_cells(void* node)
{
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

// 获取单元
uint32_t* leaf_node_cell(void* node, uint32_t cell_num)
{
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

// 获取键
uint32_t* leaf_node_key(void* node, uint32_t cell_num)
{
    return leaf_node_cell(node, cell_num);
}

// 获取数据
void* leaf_node_value(void* node, uint32_t cell_num)
{
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

// 初始化节点
void initialize_leaf_node(void* node)
{
    *leaf_node_num_cells(node) = 0;
}

// 叶节点插入
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    // 1. 获取页
    void* node = get_page(cursor->table->pager, cursor->page_num);
    // 2. 判断是否满
    uint32_t num_cells = *leaf_node_num_cells(node);
    printf("leaf_node_insert cursor cell_num: %d num_cells:%d\n", cursor->cell_num, num_cells);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        printf("Need to implement splitting a leaf node.\n");
        exit(EXIT_FAILURE);
    }
    // 3. 插入
    if (cursor->cell_num < num_cells) {
        // 如果是中间插入，数据往后移动
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i-1), LEAF_NODE_CELL_SIZE);
        }
    }

    // 4. 单元数量加一
    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}


int main(int argc, char* argv[])
{
    Table* table = db_open("sqlit.db");

    InputBuffer* input_buffer = new_input_buffer();

    printf("--- row size --- \n");
    printf("--- id_size: %d\n", ID_SIZE);
    printf("--- username_size: %d\n", USERNAME_SIZE);
    printf("--- email_size: %d\n", EMAIL_SIZE);
    printf("--- int size: %lu\n", sizeof(int));
    printf("--- char size: %lu\n", sizeof(char));
    printf("--- Row size: %lu\n", sizeof(Row));
    printf("--- ROW_SIZE: %u\n", ROW_SIZE);

    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.') {
            switch(do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'.\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
                continue;
            default:
                break;
        }

        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_ERROR):
                printf("Error: Table full.\n");
                break;

        }
    }
}
