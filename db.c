#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>

typedef enum
{

    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
} ExecuteResult;
typedef enum
{

    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum
{

    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
} PrepareResult;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT,
} StatementType;

typedef struct
{

    char *buffer;
    size_t buffer_length;
    ssize_t input_lenght;
} InputBuffer;

#define COLUMN_USERNAME_SIZE 32 // username varchar(32)
#define COLUMN_EMAIL_SIZE 255   // email varchar(255)

// 用來計算 strcut 中特定 attribute 所佔的大小
// 原理就是因為在 C 中， 建立 struct 實體時，會使用一塊連續的記憶體空間
// 所以 (Strcut*)0 代表一個 Strcut pointer ，但是指向 0 ，這樣用 -> Attribute 就可以知道 pointer 的偏移量，進而計算出大小
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

typedef struct
{

    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

// 先計算 Row 結構中每個屬性的大小
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);

// 定義偏移量，這樣才能建立一個連續的記憶體空間存放
const uint32_t ID_OFFSET = 0;                                  // 以 id 作為起點
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;          // 計算 username 的起點
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE; // 計算 email 的起點

const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; // 計算一行 row 的大小

typedef struct
{

    StatementType type;
    Row row_to_insert; // 只會用在 insert statement
} Statement;

#define TABLE_MAX_PAGE 100                                      // table 最大的 page 數量
const uint32_t PAGE_SIZE = 4096;                                // 一個 page 的大小 4 KB
const uint32_t ROWS_PRE_PAGE = PAGE_SIZE / ROW_SIZE;            // 一個 page 中能容納的 rows 數量
const uint32_t TABLE_MAX_ROWS = ROWS_PRE_PAGE * TABLE_MAX_PAGE; // 一個 table 中能容納的 rows 數量

typedef struct
{

    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGE];
} Table;

/**
 * \brief 初始化 InputBuffer 實例
 *
 * \return InputBuffer*
 */
InputBuffer *new_input_buffer()
{

    InputBuffer *input_buffer = (InputBuffer *)malloc(sizeof(InputBuffer));

    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_lenght = 0;

    return input_buffer;
}

void print_prompt()
{

    printf("db > ");
}

void read_input(InputBuffer *input_buffer)
{

    ssize_t bytes_read = getline(
        &(input_buffer->buffer),
        &(input_buffer->buffer_length),
        stdin);

    // 如果 getline 函示錯誤，就會回傳負值
    if (bytes_read <= 0)
    {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // 忽略換行符號 (\n)
    input_buffer->buffer_length = bytes_read - 1;

    // 因為 getline() 回傳的長度不會計算 \0 符號
    // 所以 index - 1 就是結尾的換行符號
    input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer *input_buffer)
{

    // getline() 函示產生的動態記憶體空間
    free(input_buffer->buffer);

    // 我們自己產生的動態記憶體空間
    free(input_buffer);
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer)
{

    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {

        printf("Bye~\n");
        exit(EXIT_SUCCESS);
    }
    else
    {

        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{

    // 能夠支援 insert X X X 的語法
    if (strncmp(input_buffer->buffer, "insert", 6) == 0)
    {

        statement->type = STATEMENT_INSERT;

        // 取得 insert 1 username email 語句當中的 (1, username, email)
        // 並儲存至 statement -> row_to_insert 結構中
        int args_assigned = sscanf(
            input_buffer->buffer, "insert %d %s %s",
            &(statement->row_to_insert.id),
            statement->row_to_insert.username,
            statement->row_to_insert.email);

        // 錯誤處理
        if (args_assigned < 3)
        {
            return PREPARE_SYNTAX_ERROR;
        }

        return PREPARE_SUCCESS;
    }

    if (strcmp(input_buffer->buffer, "select") == 0)
    {

        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void *row_slot(Table *table, uint32_t row_num)
{

    uint32_t page_num = row_num / ROWS_PRE_PAGE; // 取得 row 是在哪個 page 中

    void *page = table->pages[page_num]; // 取得指向該 page 的 pointer

    // allocate 新的 page 空間，如果 page 還沒被 allocate
    if (page == NULL)
    {

        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }

    uint32_t row_offset = row_num % ROWS_PRE_PAGE; // 取得在 page 中該 row 的 offset
    uint32_t byte_offset = row_offset * ROW_SIZE;  // 計算 row offset 需要偏移多少 bytes

    // 回傳該 row 的 pointer
    return page + byte_offset;
}

void print_row(Row *row)
{

    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

/**
 * \brief 將一行 row 的資料複製到連續緊湊的記憶體空間
 *
 * \param source
 * \param destination
 */
void serialize_row(Row *source, void *destination)
{

    // 將 soruce 的資料複製到 destination 中
    // 是以連續緊湊的方式
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

/**
 * \brief 取得在連續緊湊的記憶體空間中的一行 row，並複製到 destintaion 中
 *
 * \param source
 * \param destination
 */
void deserialize_row(void *source, Row *destination)
{

    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

ExecuteResult execute_insert(Statement *statement, Table *table)
{

    // 資料存滿時
    if (table->num_rows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &(statement->row_to_insert);

    serialize_row(row_to_insert, row_slot(table, table->num_rows));

    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *Statement, Table *table)
{

    Row row;

    for (uint32_t i = 0; i < table->num_rows; i++)
    {

        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table)
{

    switch (statement->type)
    {

    case (STATEMENT_INSERT):
        return execute_insert(statement, table);

    case (STATEMENT_SELECT):
        return execute_select(statement, table);
    }
}

Table *new_table()
{

    Table *table = (Table *)malloc(sizeof(Table));

    table->num_rows = 0;

    // initial page pointer array
    for (uint32_t i = 0; i < TABLE_MAX_PAGE; i++)
    {

        table->pages[i] = NULL;
    }

    return table;
}

void free_table(Table *table)
{

    // if page[i] == NULL; break
    for (uint32_t i = 0; table->pages[i]; i++)
    {

        free(table->pages[i]);
    }

    free(table);
}

int main(int argc, char *argv[])
{
    Table *table = new_table();
    InputBuffer *input_buffer = new_input_buffer();

    while (true)
    {

        print_prompt();

        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.')
        {

            switch (do_meta_command(input_buffer))
            {

            case (META_COMMAND_SUCCESS):
                continue;

            case (META_COMMAND_UNRECOGNIZED_COMMAND):
                printf("未定義的指令 '%s'\n", input_buffer->buffer);
                continue;
            }
        }

        Statement statement;

        switch (prepare_statement(input_buffer, &statement))
        {

        case (PREPARE_SUCCESS):
            break;

        case (PREPARE_SYNTAX_ERROR):
            printf("指令錯誤 無法解析指令\n");
            continue;

        case (PREPARE_UNRECOGNIZED_STATEMENT):
            printf("未定義的指令 '%s'\n", input_buffer->buffer);
            continue;
        }

        switch (execute_statement(&statement, table))
        {

        case (EXECUTE_SUCCESS):
            printf("Executed.\n");
            break;

        case (EXECUTE_TABLE_FULL):
            printf("Error: Table full.\n");
            break;
        }
    }
}