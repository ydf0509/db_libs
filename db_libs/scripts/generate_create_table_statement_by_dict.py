def generate_create_table_statement(data,table_name):
    columns = []
    for key, value in data.items():
        if isinstance(value, bool):
            columns.append(f"{key} BOOLEAN")
        elif isinstance(value, int):
            columns.append(f"{key} BIGINT(20)")
        elif isinstance(value, float):
            columns.append(f"{key} FLOAT")
        elif isinstance(value, str):
            columns.append(f"{key} VARCHAR(255)")
        elif isinstance(value, dict):
            columns.append(f"{key} JSON")
        else:
            columns.append(f"{key} VARCHAR(255)")

    columns_str = ', '.join(columns)
    create_table_statement = f"CREATE TABLE {table_name} ({columns_str})"

    return create_table_statement

if __name__ == '__main__':
    # 给定的字典数据
    data = {
        "_id": "test_user_custom_result:4b42c7f2-7dda-4201-9cdc-dc61789ff72c",
        "function": "f",
        "host_name": "LAPTOP-7V78BBO2",
        "host_process": "LAPTOP-7V78BBO2 - 49552",
        "insert_minutes": "2024-02-18 16:32",
        "insert_time": "2024-02-18 16:32:20",
        "insert_time_str": "2024-02-18 16:32:20",
        "msg_dict": {
            "extra": {
                "publish_time": 1708245138.8229,
                "publish_time_format": "2024-02-18 16:32:18",
                "task_id": "test_user_custom_result:4b42c7f2-7dda-4201-9cdc-dc61789ff72c"
            },
            "x": 29
        },
        "params": {
            "x": 29
        },
        "params_str": '{"x": 29}',
        "process_id": 49552,
        "publish_time": 1708245138.8229,
        "publish_time_str": "2024-02-18 16:32:18",
        "queue_name": "test_user_custom",
        "result": 290,
        "run_times": 1,
        "script_name": "test_user_custom_record_process_info_func.py",
        "script_name_long": "D:\\codes\\funboost\\test_frame\\test_user_custom_record_process_info_func\\test_user_custom_record_process_info_func.py",
        "success": True,
        "task_id": "test_user_custom_result:4b42c7f2-7dda-4201-9cdc-dc61789ff72c",
        "thread_id": 25580,
        "time_cost": 0.003,
        "time_end": 1708245140.4220266,
        "time_start": 1708245140.4190009,
        "total_thread": 8,
        "utime": "2024-02-18 08:32:20",
    }

    # 生成建表语句
    create_table_statement = generate_create_table_statement(data,table_name = "your_table_namexx")

    # 打印建表语句
    print(create_table_statement)