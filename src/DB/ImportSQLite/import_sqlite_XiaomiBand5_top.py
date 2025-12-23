import csv
import glob
import os
import sqlite3
import json
import time

from src.DB.SQLite_util import SQLite_util
from src.config.configClass import app_config


class fitness_data_top:
    """
    一个用于管理健康数据的仓库类，严格遵循业务逻辑与数据库执行分离的模式。
    它依赖于一个 Database 对象来执行所有SQL操作。
    """

    def __init__(self, database: SQLite_util):
        """
        初始化仓库。

        Args:
            database (Database): 一个已经实例化的 Database 对象。
        """
        self.db = database

    def create_tables(self):
        """
        如果表不存在，则创建所需的 'fitness_data_top_main' 和 'fitness_data_top_ext' 表。
        """
        with self.db.transaction() as cursor:
            # 主数据表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fitness_data_top_main (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT, -- 记录的全局唯一ID，用作外键
            
                -- === 核心元数据 (来自CSV外层) ===
                Uid INTEGER NOT NULL,                        -- 用户唯一标识
                Sid TEXT,                                    -- 数据来源ID (例如设备序列号)
                Tag TEXT NOT NULL,                           -- 数据分类标签 (例如 'daily_report')
                Key TEXT NOT NULL,                           -- 数据的具体键 (例如 'steps', 'sleep', 决定了其他列的含义)
                Time INTEGER NOT NULL,                       -- 记录日期的时间戳 (Unix Timestamp)
                UpdateTime INTEGER,                          -- 记录的最后更新时间戳
                
                -- === 所有来自 Value JSON 的字段 (与JSON键名完全一致) ===
                
                -- 步数 (当 Key='steps' 时)
                steps INTEGER,                               -- 总步数
                distance INTEGER,                            -- 总距离 (单位: 米)
                
                -- 卡路里 (当 Key='calories' 时)
                goal REAL,                                   -- 设定的目标值
                
                -- 站立 (当 Key='valid_stand' 时)
                count INTEGER,                         -- 有效站立次数
                
                -- 心率 (当 Key='heart_rate' 时)
                avg_rhr INTEGER,                             -- 当日平均静息心率
                max_hr INTEGER,                              -- 当日最高心率
                min_hr INTEGER,                              -- 当日最低心率
                abnormal_hr_count INTEGER,                   -- 异常心率提醒次数
                warm_up_hr_zone_duration INTEGER,            -- 心率处于“热身”区间的总时长 (分钟)
                fat_burning_hr_zone_duration INTEGER,        -- 心率处于“燃脂”区间的总时长 (分钟)
                aerobic_hr_zone_duration INTEGER,            -- 心率处于“有氧耐力”区间的总时长 (分钟)
                anaerobic_hr_zone_duration INTEGER,          -- 心率处于“无氧极限”区间的总时长 (分钟)
                extreme_hr_zone_duration INTEGER,            -- 心率处于“极限”区间的总时长 (分钟)
            
                -- 压力 (当 Key='stress' 时)
                avg_stress INTEGER,                          -- 当日平均压力值
                max_stress INTEGER,                          -- 当日最高压力值
                min_stress INTEGER,                          -- 当日最低压力值
                relax INTEGER,                      -- 处于“放松”状态的总时长 (分钟)
                mild INTEGER,                       -- 处于“轻度”压力的总时长 (分钟)
                moderate INTEGER,                   -- 处于“中度”压力的总时长 (分钟)
                severe INTEGER,                     -- 处于“严重”压力的总时长 (分钟)
            
                -- 睡眠 (当 Key='sleep' 时)
                sleep_score INTEGER,                         -- 睡眠总得分
                total_duration INTEGER,                -- 【新增】总睡眠时长 (分钟)
                sleep_deep_duration INTEGER,                 -- 【新增】深睡总时长 (分钟)
                sleep_light_duration INTEGER,                -- 【新增】浅睡总时长 (分钟)
                sleep_rem_duration INTEGER,                  -- 【新增】REM睡眠总时长 (分钟)
                sleep_awake_duration INTEGER,                -- 【新增】清醒总时长 (分钟)
                avg_spo2 INTEGER,                            -- 睡眠期间的平均血氧饱和度
                long_sleep_evaluation INTEGER,               -- 长睡眠评价代码
                day_sleep_evaluation INTEGER,                -- 日间小睡评价代码
                sleep_stage INTEGER,                            -- 可能是对睡眠规律性或模式的评级代码。

                
                -- daily_fitness (当 Key='daily_fitness' 时)
                date_time INTEGER,                           -- 【新增】详细的日期时间戳
                timeInZero INTEGER,                          -- 【新增】整点时间戳
                
                -- daily_mark (当 Key='daily_mark' 时)
                has_data INTEGER,                            -- 【新增】是否有数据的标记 (通常为 0 或 1)
            
                -- === 【冲突字段】 - 合并为通用列 ===
                avg_hr REAL,                                 -- 通用平均心率列。当Key='heart_rate'时, 代表当日平均心率；当Key='sleep'时, 代表睡眠期间的平均心率。
                calories REAL,                               -- 通用卡路里列。当Key='steps'时, 代表步数活动消耗的卡路里；当Key='calories'时, 代表当天完成的总卡路里。
                
                -- 联合唯一约束，保持不变，是UPSERT的关键
                UNIQUE (Uid, Time, Key)
            );
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_top_main_time_key ON fitness_data_top_main(Time, Key);
            """)

            # 子项目表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fitness_data_top_ext (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,      -- 子项目的全局唯一ID
                main_record_id INTEGER NOT NULL,                -- 指向主表的外键
                item_json TEXT NOT NULL,                        -- 存储子项目的JSON对象
                FOREIGN KEY (main_record_id) 
                    REFERENCES fitness_data_top_main(record_id) 
                    ON DELETE CASCADE
            );
            """)
        print(f"XiaomiBand5  按天聚合顶层表  结构已在数据库中准备就绪。")

    def save_batch_records(self, batch_data):
        """
        批量保存多条记录，

        在一个事务中，完整地保存一条主记录及其所有关联的子项目。
        此函数精确地执行了“UPSERT主表 -> 获取ID -> 删除旧子表 -> 插入新子表”的原子操作。

        Args:
            batch_data (list): 包含(record_data, fitness_data_down_ext_list)元组的列表
            record_data (dict): 主记录的字段数据。
            fitness_data_top_ext_list (list, optional): 子项目字典的列表。

        """
        if not batch_data:
            return

        with self.db.transaction() as cursor:
            for record_data, fitness_data_down_ext_list in batch_data:
                # 步骤 1: UPSERT主表记录
                columns = ', '.join(record_data.keys())
                placeholders = ', '.join(['?'] * len(record_data))
                values = list(record_data.values())
                update_assignments = ', '.join([f"{col} = excluded.{col}" for col in record_data.keys()])
                sql_upsert = f"INSERT INTO fitness_data_top_main ({columns}) VALUES ({placeholders}) ON CONFLICT(Uid, Time, Key) DO UPDATE SET {update_assignments};"
                cursor.execute(sql_upsert, values)

                # 步骤 2: 获取刚刚操作过的主记录的 record_id
                cursor.execute("SELECT record_id FROM fitness_data_top_main WHERE Uid=? AND Time=? AND Key=?",
                               (record_data['Uid'], record_data['Time'], record_data['Key']))
                main_record_id = cursor.fetchone()[0]

                # 步骤 3: 删除所有与该主记录关联的旧子项目
                cursor.execute("DELETE FROM fitness_data_top_ext WHERE main_record_id = ?", (main_record_id,))

                # 步骤 4: 批量插入所有新的子项目
                if fitness_data_down_ext_list:
                    items_to_insert = [(main_record_id, json.dumps(item)) for item in fitness_data_down_ext_list]
                    cursor.executemany("INSERT INTO fitness_data_top_ext (main_record_id, item_json) VALUES (?, ?)",
                                       items_to_insert)


    def import_XiaomiBand5_top(self):
        start = time.perf_counter()

        # db_instance = SQLite_util(app_config.SQLitePath)
        try:
            # repo = fitness_data_top(db_instance)
            self.create_tables()

            CSV_FILENAME_PATTERN = '*_MiFitness_hlth_center_aggregated_fitness_data.csv'

            search_path = os.path.join(app_config.MiSmartBand5, CSV_FILENAME_PATTERN)
            files_found = glob.glob(search_path)

            if not files_found:
                print(f"错误：在文件夹 '{app_config.MiSmartBand5}' 中没有找到匹配 '{CSV_FILENAME_PATTERN}' 的文件。")
            else:
                source_file = files_found[0]
                print(f"成功找到源文件：'{source_file}'，开始处理...")

                BATCH_SIZE = 10000  # 批处理大小，可以根据系统性能调整
                batch = []

                with (open(source_file, mode='r', encoding='utf-8') as csvfile):
                    reader = csv.DictReader(csvfile)
                    total_rows = 0
                    processed_rows = 0
                    kisp_rows = 0

                    for row in reader:
                        total_rows += 1
                        try:
                            main_record = {
                                'Uid': int(row['Uid']), 'Sid': row['Sid'], 'Tag': row['Tag'],
                                'Key': row['Key'], 'Time': int(row['Time']), 'UpdateTime': int(row['UpdateTime'])
                            }

                            if main_record['Key'] == 'intensity' or main_record['Tag'] == 'daily_mark':
                                # 当key为intensity时，value值为duration:数字，表示运动时间，或者value为has_data:true，库里面不要这个数据
                                kisp_rows += 1
                                continue


                            value_json = json.loads(row['Value'])

                            main_record.update(value_json)

                            if main_record['Key'] == 'heart_rate':
                                if main_record.get('latest_hr', None) is not None:
                                    # 当key为heart_rate时，value的json字段中有一个嵌套字段latest_hr，记录的是当天最后一次心率，这里直接去掉
                                    main_record.pop('latest_hr')

                            if main_record['Key'] == 'goal':
                                if main_record.get('sidList', None) is not None:
                                    # 当key=goal，设备字段，去除
                                    main_record.pop('sidList')


                            fitness_data_top_ext = None
                            temp_dict = {}

                            # 一对多字段单独存储
                            if 'segment_details' in main_record:
                                fitness_data_top_ext = main_record.pop('segment_details')
                            elif 'goal_items' in main_record:
                                fitness_data_top_ext = main_record.pop('goal_items')
                            else:
                                # 将二级字典数据字段展开
                                for key in main_record.keys():
                                    value = main_record.get(key)
                                    if isinstance(value, dict):
                                        temp_dict.update({key: value})
                                for key in temp_dict.keys():
                                    main_record.pop(key)
                                    main_record.update(temp_dict.get(key))

                            batch.append((main_record, fitness_data_top_ext))

                            # 当批处理列表达到设定大小时，批量保存
                            if len(batch) >= BATCH_SIZE:
                                self.save_batch_records(batch)
                                processed_rows += len(batch)
                                batch = []  # 清空批处理列表
                                print(f"已处理 {processed_rows}/{total_rows} 行数据")

                        except (json.JSONDecodeError, KeyError, ValueError) as e:
                            print(f"警告：处理第 {total_rows} 行时数据格式有误，已跳过。错误: {e}")
                        except Exception as e:
                            print(f"main_record2: {main_record}")
                            print(f"value_json: {value_json}")
                            print(f"警告：处理第 {total_rows} 行时发生未知错误：{e}，已跳过。")

                    # 处理剩余的记录
                    if batch:
                        self.save_batch_records(batch)
                        processed_rows += len(batch)
                        print(f"已处理 {processed_rows}/{total_rows} 行数据")

                print(f"\n处理完成！共扫描 {total_rows} 行，处理 {processed_rows + kisp_rows} 行。"
                      f"成功处理并存入数据库 {processed_rows} 行。跳过 {kisp_rows} 行。")

        except sqlite3.Error as e:
            print(f"\n数据库操作失败: {e}")
        except FileNotFoundError:
            print(f"错误：指定的文件夹路径不存在 '{app_config.MiSmartBand5}'")
        except Exception as e:
            print(f"\n发生了未预料的错误: {e}")
        finally:
            # 这里的 hasattr(db_instance, 'conn') 是一个安全检查
            if hasattr(self.db, 'conn') and self.db.conn:
                end = time.perf_counter()
                print(f"执行时间：{(end - start) :.6f} 秒")  # 转换为毫秒显示，更易读
                self.db.close()
                print("数据库连接已关闭。")


# --- 数据导入脚本 ---
if __name__ == '__main__':

    db_instance = SQLite_util(app_config.SQLitePath)
    repo = fitness_data_top(db_instance)

    repo.import_XiaomiBand5_top()
