#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

typedef enum
{

    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum
{

    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
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

typedef struct
{

    StatementType type;
} Statement;

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
        return PREPARE_SUCCESS;
    }

    if (strcmp(input_buffer->buffer, "select") == 0)
    {

        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement)
{

    switch (statement->type)
    {

    case (STATEMENT_INSERT):

        printf("insert data...\n");
        break;

    case (STATEMENT_SELECT):

        printf("select data...\n");
        break;
    }
}

int main(int argc, char *argv[])
{
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

        case (PREPARE_UNRECOGNIZED_STATEMENT):
            printf("未定義的指令 '%s'\n", input_buffer->buffer);
            continue;
        }

        execute_statement(&statement);
        printf("Executed.\n");
    }
}