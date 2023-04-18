import subprocess
import unittest

class TestDatabase(unittest.TestCase):
    def run_script(self, commands):
        # 執行 C 程序
        process = subprocess.Popen("./db.out", stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # 寫入輸入
        for command in commands:
            process.stdin.write(f"{command}\n".encode())

        # 結束輸入
        process.stdin.close()

        # 讀取輸出
        output = process.stdout.read().decode().strip()

        # 讀取錯誤訊息
        error = process.stderr.read().decode().strip()

        # 關閉輸出資源
        process.stdout.close()
        process.stderr.close()

        # 確保程序已經結束
        process.wait()

        return (output, error)

    def test_database(self):

        script = [
            "insert 1 test test@test.com",
            "select",
            ".exit"
        ]

        ouptput, error = self.run_script(script)

        output_list = [
            "db > Executed.",
            "db > (1, test, test@test.com)",
            "Executed.",
            "db > Bye~"
        ]

        self.assertEqual(ouptput, "\n".join(output_list))

    def test_table_full(self):

        script = []

        for i in range(0, 1401):

            script.append(f"insert {i} test{i} test{i}@test.com")
        
        script.append(".exit")

        output, error = self.run_script(script)

        self.assertEqual(output.split("\n")[-2], "db > Error: Table full.")

    def test_insert_long_string(self):

        long_username = "a" * 32
        long_email = "a" * 255

        script = [
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit"
        ]

        output, erorr = self.run_script(script)

        ouput_list = [
            "db > Executed.",
            f"db > (1, {long_username}, {long_email})",
            "Executed.",
            "db > Bye~"
        ]

        self.maxDiff = None

        self.assertEqual(output, "\n".join(ouput_list))

        self.maxDiff = True

    def test_string_too_long(self):

        long_username = "a" * 33
        long_email = "a" * 256

        script = [
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit"
        ]

        output, erro = self.run_script(script)

        output_list = [
            "db > String is too long.",
            "db > Executed.",
            "db > Bye~"
        ]

        self.assertEqual(output, "\n".join(output_list))

    def test_id_must_be_positive(self):

        script = [
            "insert -1 test test@test.com",
            "select",
            ".exit"
        ]

        output, error = self.run_script(script)

        output_list = [
            "db > ID must be positive.",
            "db > Executed.",
            "db > Bye~"
        ]

        self.assertEqual(output, "\n".join(output_list))

if __name__ == "__main__":
    unittest.main()
