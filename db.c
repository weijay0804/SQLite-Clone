#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

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
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIV_ID,
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
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
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

    int file_descriptor;
    uint32_t file_lenght;
    void *pages[TABLE_MAX_PAGE];
} Pager;

typedef struct
{

    uint32_t num_rows;
    Pager *pager;
} Table;

typedef struct
{

    Table *table;
    uint32_t row_num;
    bool end_of_table;
} Cursor;

Cursor *table_start(Table *table)
{

    Cursor *cursor = malloc(sizeof(Cursor));

    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = (table->num_rows == 0);

    return cursor;
}

Cursor *table_end(Table *table)
{

    Cursor *cursor = malloc(sizeof(Cursor));

    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;

    return cursor;
}

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

void db_close(Table *table);

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table)
{

    if (strcmp(input_buffer->buffer, ".exit") == 0)
    {

        printf("Bye~\n");
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    else
    {

        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement)
{

    statement->type = STATEMENT_INSERT;

    // 取得 id username email 資料
    // 取得 insert 關鍵字
    char *keyword = strtok(input_buffer->buffer, " ");
    char *id_string = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL)
    {

        return PREPARE_SYNTAX_ERROR;
    }

    // 將 id 字串轉換成 int
    int id = atoi(id_string);

    if (id < 0)
    {
        return PREPARE_NEGATIV_ID;
    }

    // 檢查 username 和 email 的長度
    if (strlen(username) > COLUMN_USERNAME_SIZE)
    {

        return PREPARE_STRING_TOO_LONG;
    }

    if (strlen(email) > COLUMN_EMAIL_SIZE)
    {

        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement)
{

    // 能夠支援 insert X X X 的語法
    if (strncmp(input_buffer->buffer, "insert", 6) == 0)
    {
        return prepare_insert(input_buffer, statement);
    }

    if (strcmp(input_buffer->buffer, "select") == 0)
    {

        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void *get_page(Pager *pager, uint32_t page_num)
{

    // page number 超出 Table Max Page number
    if (page_num > TABLE_MAX_PAGE)
    {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGE);
        exit(EXIT_FAILURE);
    }

    // cache miss ， allocate memory 並且將 file load 進去
    if (pager->pages[page_num] == NULL)
    {

        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_lenght / PAGE_SIZE;

        // 如果最後一頁的資料沒有存滿，就將 num_pages + 1 ，因為上面在計算的時候會遺漏掉
        // 例如 PAGE_SIZE = 1024， file_length = 4096，這樣代表 4 個頁面都存滿了資料
        // 但如果 file_length = 5000，就代表 4 個頁面存滿了，但第五個頁面還沒存滿，這樣 num_pages = 4，會少一個頁面
        if (pager->file_lenght % PAGE_SIZE)
        {
            num_pages += 1;
        }

        // if cache miss ，將 disk 中的資料 load 到 memory
        if (page_num <= num_pages)
        {

            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);

            if (bytes_read == -1)
            {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;
    }

    return pager->pages[page_num];
}

void *cursor_value(Cursor *cursor)
{

    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PRE_PAGE; // 取得 row 是在哪個 page 中

    void *page = get_page(cursor->table->pager, page_num);

    uint32_t row_offset = row_num % ROWS_PRE_PAGE; // 取得在 page 中該 row 的 offset
    uint32_t byte_offset = row_offset * ROW_SIZE;  // 計算 row offset 需要偏移多少 bytes

    // 回傳該 row 的 pointer
    return page + byte_offset;
}

void cursor_advance(Cursor *cursor)
{

    cursor->row_num += 1;

    if (cursor->row_num >= cursor->table->num_rows)
    {
        cursor->end_of_table = true;
    }
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
    strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
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

    Cursor *cursor = table_end(table);

    serialize_row(row_to_insert, cursor_value(cursor));

    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *Statement, Table *table)
{

    Cursor *cursor = table_start(table);

    Row row;

    while (!(cursor->end_of_table))
    {

        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    free(cursor);

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

Pager *pager_open(const char *filename)
{

    int fd = open(filename,
                  O_RDWR |     // Read/Write mode
                      O_CREAT, // Create file if it does not exist
                  S_IWUSR |    // User write permission
                      S_IRUSR  // User read permission
    );

    if (fd == -1)
    {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    // 取得檔案的大小 ( bytes )
    off_t file_length = lseek(fd, 0, SEEK_END);

    // 跟 (Pager*)malloc(sizeof(Pager)) 一樣
    Pager *pager = malloc(sizeof(Pager));

    pager->file_descriptor = fd;
    pager->file_lenght = file_length;

    // 初始化 page cache
    for (uint32_t i = 0; i < TABLE_MAX_PAGE; i++)
    {

        pager->pages[i] = NULL;
    }

    return pager;
}

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size)
{

    if (pager->pages[page_num] == NULL)
    {
        printf("Tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1)
    {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);

    if (bytes_written == -1)
    {
        printf("Error writting: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

Table *db_open(const char *filename)
{

    Pager *pager = pager_open(filename);
    uint32_t num_rows = pager->file_lenght / ROW_SIZE;

    Table *table = (Table *)malloc(sizeof(Table));

    table->pager = pager;
    table->num_rows = num_rows;

    return table;
}

void db_close(Table *table)
{

    Pager *pager = table->pager;

    uint32_t num_full_pages = table->num_rows / ROWS_PRE_PAGE;

    for (uint32_t i = 0; i < num_full_pages; i++)
    {

        if (pager->pages[i] == NULL)
        {

            continue;
        }

        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // 可能會有沒有存滿一個 page 的資料
    // 切換到 B-Tree 就不需要處理這個
    uint32_t num_additional_row = table->num_rows % ROWS_PRE_PAGE;
    if (num_additional_row > 0)
    {
        uint32_t page_num = num_full_pages;

        if (pager->pages[page_num] != NULL)
        {

            pager_flush(pager, page_num, num_additional_row * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int result = close(pager->file_descriptor);
    if (result == -1)
    {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGE; i++)
    {

        void *page = pager->pages[i];

        if (page)
        {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}

int main(int argc, char *argv[])
{

    if (argc < 2)
    {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char *filename = argv[1];
    Table *table = db_open(filename);

    InputBuffer *input_buffer = new_input_buffer();

    while (true)
    {

        print_prompt();

        read_input(input_buffer);

        if (input_buffer->buffer[0] == '.')
        {

            switch (do_meta_command(input_buffer, table))
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

        case (PREPARE_STRING_TOO_LONG):
            printf("String is too long.\n");
            continue;

        case (PREPARE_NEGATIV_ID):
            printf("ID must be positive.\n");
            continue;

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