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
void pager_flush(Pager* pager, uint32_t i, uint32_t page_size);
void free_table(Table* table);
ExecuteResult execute_statement(Statement* statement, Table* table);

InputBuffer* new_input_buffer()
{
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void print_prompt()
{
    printf("db > ");
}

void read_input(InputBuffer* input_buffer)
{
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if(bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // 删除换行符
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer)
{
    free(input_buffer->buffer);
    free(input_buffer);
}

void db_close(Table* table)
{
    Pager* pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    printf("db_close num_full_pages:%d table num rows:%d\n", num_full_pages, table->num_rows);

    for(uint32_t i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;

    printf("db_close num_additional_rows:%d\n", num_additional_rows);

    if (num_additional_rows > 0) {
        uint32_t page_num = num_full_pages;
        if (pager->pages[page_num] != NULL) {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

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

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table* table)
{
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        db_close(table);
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement)
{
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

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
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);
    return PREPARE_SUCCESS;
}

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

void serialize_row(Row* source, void* destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination)
{
    memcpy(&(destination->id), source+ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source+USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source+EMAIL_OFFSET, EMAIL_SIZE);
}

void* get_page(Pager* pager, uint32_t page_num)
{
    if(page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if(pager->pages[page_num] == NULL) {
        void* page = malloc(PAGE_SIZE);

        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        // 从磁盘中读取数据
        if (page_num  <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if(bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;
    }

    return pager->pages[page_num];
}

void* row_slot(Table* table, uint32_t row_num)
{
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = get_page(table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

ExecuteResult execute_insert(Statement* statement, Table* table)
{
    if( table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_ERROR;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    void* address = row_slot(table, table->num_rows);
    serialize_row(row_to_insert, address);
    table->num_rows += 1;
    printf("execute_insert table num_rows:%d\n", table->num_rows);
    return EXECUTE_SUCCESS;
}

void print_row(Row* row)
{
    printf("(%d %s %s)\n", row->id, row->username, row->email);
}

ExecuteResult execute_select(Statement* statement, Table* table)
{
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

Pager* pager_open(const char* filename)
{
    int fd = open(filename,
                  O_RDWR | O_CREAT,
                  S_IWUSR | S_IRUSR
                  ); // S_IWUSR  00200 user has write permission, 00400 user has read permission
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;

    printf("pager_open file length:%d\n", pager->file_length);

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

Table* db_open(const char* filename)
{
    Pager* pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;
    printf("db_open num_rows: %d\n", num_rows);
    Table* table = malloc(sizeof(Table));
    table->num_rows = num_rows;
    table->pager = pager;
    return table;
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size)
{
    printf("pager_flush page_num:%d size:%d\n", page_num, size);
    if(pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);

    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void free_table(Table* table)
{
    for (uint32_t i = 0; i < TABLE_MAX_ROWS; i ++) {
        free(table->pager->pages);
    }
    free(table->pager);
    free(table);
}

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
