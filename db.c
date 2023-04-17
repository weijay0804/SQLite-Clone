#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

typedef struct
{

    char *buffer;
    size_t buffer_length;
    ssize_t input_lenght;
} InputBuffer;

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

int main(int argc, char *argv[])
{
    InputBuffer *input_buffer = new_input_buffer();

    while (true)
    {

        print_prompt();

        read_input(input_buffer);

        if (strcmp(input_buffer->buffer, ".exit") == 0)
        {

            close_input_buffer(input_buffer);
            printf("Bye~\n");
            exit(EXIT_SUCCESS);
        }
        else
        {
            printf("未定義的指令\n");
        }
    }
}