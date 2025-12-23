import csv
import glob
import os
import sqlite3
import json
import time

from src.DB.SQLite_util import SQLite_util
from src.config.configClass import app_config


class fitness_data_middle:
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
        如果表不存在，则创建所需的 'fitness_data_middle_main' 和 'fitness_data_middle_ext' 表。
        """
        with self.db.transaction() as cursor:
            # 主数据表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fitness_data_middle_main (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT, -- 记录的全局唯一ID，用作外键
            
                -- 基础标识字段（通用，全表适用）
                uid INTEGER NOT NULL,               -- 用户ID，小米账户唯一标识符（通用字段）
                did TEXT NOT NULL,                  -- 设备ID，数据来源设备唯一标识符（通用字段）       
                tag TEXT NOT NULL,                  -- 数据一级分类（如days/weekly_statistics/times等，通用字段）
                key TEXT NOT NULL,                  -- 数据二级分类（如watch_sleep_report/huami_pai_record等，通用字段）
                time INTEGER NOT NULL,              -- 记录生成时间，秒级Unix时间戳（通用字段）
                last_modify INTEGER NOT NULL,       -- 记录最后修改时间，秒级Unix时间戳（通用字段）
                metric INTEGER DEFAULT 0,           -- 达标标识，0=未达标/无目标，1=达标（通用字段）
                
                -- 每日步数报告  key=watch_steps_report
                steps INTEGER,                      -- 单日总步数
                distance INTEGER,                   -- 单日总距离（单位：米）
                actSteps INTEGER,                   -- 实际步数，这是一个dict，需要解包
                goal INTEGER,                       -- 单日步数目标（6000步）
                calories INTEGER,                     -- 单日总卡路里消耗（单位：卡）
                activeDuration INTEGER,              -- 活跃时长（单位：分钟）

                -- 每日步数记录  key=watch_steps_record
                -- 一对多的数组
                
                -- 每日睡眠报告  key=watch_sleep_report
                bedtime INTEGER,                      -- 入睡时间戳
                wake_up_time INTEGER,                 -- 醒来时间戳
                night_duration INTEGER,               -- 夜间睡眠总时长（单位：分钟）
                sleep_deep_duration INTEGER,          -- 深度睡眠时长（单位：分钟）
                sleep_light_duration INTEGER,         -- 浅度睡眠时长
                sleep_rem_duration INTEGER,           -- REM睡眠时长
                sleep_awake_duration INTEGER,         -- 夜间清醒时长
                total_score INTEGER,                  -- 睡眠总评分
                friendly_score INTEGER,               -- 睡眠友好评分
                daytime_duration INTEGER,             -- 日间睡眠总时长（单位：分钟）

                -- 夜间睡眠记录  key=watch_night_sleep_record
                duration INTEGER,                      -- 夜间睡眠总时长（单位：分钟）
                -- 其他字段为一对多

                -- 每日心率报告  key=watch_hrm_report
                avg_hrm INTEGER,                      -- 日均心率（单位：次/分钟）
                -- 下面的这两个字段需要解包二级字典，并且要做字典映射
                max_hrm_time INTEGER,                 -- 最高心率出现时间戳
                max_hrm_hrm INTEGER,                  -- 最高心率值（单位：次/分钟）
                
                min_hrm_time INTEGER,                 -- 最高心率出现时间戳
                min_hrm_hrm INTEGER,                  -- 最高心率值（单位：次/分钟）
                
                rhr_avg INTEGER,                      -- 日均静息心率（单位：次/分钟）
               
                -- 华米 PAI 记录  key=huami_pai_record
                daily_pai REAL,                       -- 当日PAI值（个人活动智能指数）
                total_pai REAL,                       -- 累计PAI值
                high_zone_pai REAL,                   -- 高强度运动区间PAI值
                medium_zone_pai REAL,                 -- 中强度运动区间PAI值
                low_zone_pai REAL,                    -- 低强度运动区间PAI值
                
                -- 每日压力报告  key=watch_stress_report
                avg_stress INTEGER,                      -- 日均压力值（0-100，18为轻度压力） 
                                                         -- key = hlth_status 也用这个字段
                                                        
                -- 下面字段需要二级解包并字典映射，以及这个key其他的数据都不要保存
                max_stress_stress INTEGER,               -- 当日最高压力值
                max_stress_time INTEGER,                 -- 最高压力出现时间戳
                min_stress_stress INTEGER,               -- 当日最低压力值
                min_stress_time INTEGER,                 -- 最低压力出现时间戳          
                
                -- 周健身报告  key=fitness_report
                sports_hr_max INTEGER,                    -- 本周最高心率（单位：次/分钟）
                sport_times INTEGER,                      -- 本周运动次数
                sport_times_diff INTEGER,                 -- 与上周运动次数的差值（0表示无变化）
                sport_days INTEGER,                       -- 本周运动天数
                sport_days_diff INTEGER,                  -- 与上周运动天数的差值
                
                sports_duration_int_value INTEGER,         -- 本周运动总时长（单位：秒） 解包映射
                running_int_value INTEGER,                -- 本周运动距离（单位：米） 解包映射
                
                steps_summary_int_value INTEGER,          -- 本周总步数,这个字段需要解包映射
                step_goal_achieve INTEGER,                -- 本周步数目标达成次数
                
                calorie_summary_int_value INTEGER,        -- 本周总卡路里消耗
                
                -- rank_percent_summary，
                -- all_fitness_goal_achieved_count，
                -- allFitnessGoalAchivevedDay
                -- all_fitness_goal_achieved_count_diff，
                -- avg_achieved_fitness_goal_summary
                -- cal_goal_achieve
                -- 字段跳过
                
                -- sleep_report字典下解包
                avg_sleep_score INTEGER,                    -- 日均睡眠评分（0-100）
                max_sleep_score INTEGER,                    -- 本周最高睡眠评分
                avg_sleep_duration INTEGER,                 -- 日均睡眠总时长（单位：分钟）
                avg_deep_sleep_duration INTEGER,            -- 日均深度睡眠时长（单位：分钟）
                avg_deep_sleep_rate INTEGER,                -- 日均深度睡眠占比（百分比）
                -- sleep_stage，
                -- sleep_evaluation 
                -- 字段跳过
                week_lastest_bed_time INTEGER,               -- 本周最晚入睡时间（时间戳）
                week_lastest_bed_time_zone INTEGER,          -- 最晚入睡时间对应的时区（+8时区）
                week_earliest_wake_up_time INTEGER,          -- 本周最早醒来时间（时间戳）
                week_earliest_wake_up_zone INTEGER,          -- 最早醒来时间对应的时区（+8时区）
                
                -- hlth_status字典下解包
                avg_rhr INTEGER,                            -- 日均静息心率（单位：次/分钟）
                max_hr INTEGER,                             -- 本周最高心率（单位：次/分钟）
                --avg_stress INTEGER,                         -- 日均压力值（0-100，越低压力越小）
                max_stress INTEGER,                         -- 本周最高压力值
               
                -- version字段跳过

                -- 联合唯一约束，保持不变，是UPSERT的关键
                UNIQUE (uid, time, key)
                );
                """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_middle_main_time_key ON fitness_data_middle_main(time, key);
            """)

            # 子项目表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fitness_data_middle_ext (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT,      -- 子项目的全局唯一ID
                    main_record_id INTEGER NOT NULL,                -- 指向主表的外键
                    item_json TEXT NOT NULL,                        -- 存储子项目的JSON对象
                    FOREIGN KEY (main_record_id) 
                        REFERENCES fitness_data_middle_main(record_id) 
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
                sql_upsert = f"INSERT INTO fitness_data_middle_main ({columns}) VALUES ({placeholders}) ON CONFLICT(uid, time, key) DO UPDATE SET {update_assignments};"
                cursor.execute(sql_upsert, values)

                # 步骤 2: 获取刚刚操作过的主记录的 record_id
                cursor.execute("SELECT record_id FROM fitness_data_middle_main WHERE uid=? AND time=? AND key=?",
                               (record_data['uid'], record_data['time'], record_data['key']))
                main_record_id = cursor.fetchone()[0]

                # 步骤 3: 删除所有与该主记录关联的旧子项目
                cursor.execute("DELETE FROM fitness_data_middle_ext WHERE main_record_id = ?", (main_record_id,))

                # 步骤 4: 批量插入所有新的子项目
                if fitness_data_down_ext_list:
                    items_to_insert = [(main_record_id, json.dumps(item)) for item in fitness_data_down_ext_list]
                    cursor.executemany("INSERT INTO fitness_data_middle_ext (main_record_id, item_json) VALUES (?, ?)",
                                       items_to_insert)



# --- 数据导入脚本 (最终健壮版) ---
if __name__ == '__main__':

    start = time.perf_counter()

    db_instance = SQLite_util(app_config.SQLitePath)
    try:
        repo = fitness_data_middle(db_instance)
        repo.create_tables()

        CSV_FILENAME_PATTERN = '*_MiFitness_user_fitness_data_records.csv'
        DATA_FOLDER_PATH = "C:/Users/28484/Desktop/20251209_1311597473_MiFitness_c3_data_copy (1)"

        search_path = os.path.join(DATA_FOLDER_PATH, CSV_FILENAME_PATTERN)
        files_found = glob.glob(search_path)

        if not files_found:
            print(f"错误：在文件夹 '{DATA_FOLDER_PATH}' 中没有找到匹配 '{CSV_FILENAME_PATTERN}' 的文件。")
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
                            'uid': int(row['uid']), 'did': row['did'], 'tag': row['tag'],
                            'key': row['key'], 'time': int(row['time']),
                            'last_modify': int(row['last_modify']),'metric': int(row['metric'])
                        }


                        if not ((main_record['tag'] == 'days' and main_record['key'] == 'watch_steps_report') or \
                            (main_record['tag'] == 'days' and main_record['key'] == 'watch_steps_record') or \
                            (main_record['tag'] == 'days' and main_record['key'] == 'watch_sleep_report') or \
                            (main_record['tag'] == 'days' and main_record['key'] == 'watch_night_sleep_record') or \
                            (main_record['tag'] == 'days' and main_record['key'] == 'watch_hrm_report') or \
                            (main_record['tag'] == 'days' and main_record['key'] == 'huami_pai_record') or \
                            (main_record['tag'] == 'days' and main_record['key'] == 'watch_stress_report') or \
                            (main_record['tag'] == 'weekly_statistics' and main_record['key'] == 'fitness_report')) :
                            kisp_rows += 1
                            continue

                        value_json = json.loads(row['value'])

                        if isinstance(value_json, dict) and main_record['key'] == 'watch_steps_report':
                            # 提取actSteps中键"0"对应的值，替换原actSteps
                            value_json["actSteps"] = value_json["actSteps"]["0"]


                        if isinstance(value_json, list) and main_record['key'] == 'watch_steps_record':
                            temp_value_json = []
                            # 遍历每个字典元素，修改actSteps的值
                            for idx,item in enumerate(value_json):
                                # 提取actSteps中键"0"对应的值，替换原actSteps
                                item["actSteps"] = item["actSteps"]["0"]

                                temp_value_json.append(item)
                            value_json = temp_value_json

                        if main_record['key'] == 'watch_hrm_report':
                            # 二级解包，手动映射，这种方式很蠢，但先完成吧
                            value_json.update({'max_hrm_time':value_json['max_hrm']['time']})
                            value_json.update({'max_hrm_hrm':value_json['max_hrm']['hrm']})

                            value_json.update({'min_hrm_time':value_json['min_hrm']['time']})
                            value_json.update({'min_hrm_hrm':value_json['min_hrm']['hrm']})

                            value_json.pop('max_hrm')
                            value_json.pop('min_hrm')

                        if main_record['key'] == 'watch_stress_report':
                            # 二级解包，映射，去掉不要的字段
                            value_json_temp = {}
                            value_json_temp.update({'max_stress_stress': value_json['max_stress']['stress']})
                            value_json_temp.update({'max_stress_time': value_json['max_stress']['time']})

                            value_json_temp.update({'min_stress_stress': value_json['min_stress']['stress']})
                            value_json_temp.update({'min_stress_time': value_json['min_stress']['time']})

                            value_json_temp.update({'avg_stress': value_json['avg_stress']})

                            value_json = value_json_temp

                        if main_record['key'] == 'fitness_report':
                            value_json.update({'steps_summary_int_value': value_json['steps_summary']['int_value']})
                            value_json.update({'calorie_summary_int_value': value_json['calorie_summary']['int_value']})

                            if 'sports_duration' in value_json:
                                value_json.update({'sports_duration_int_value': value_json['sports_duration']['int_value']})
                                value_json.pop('sports_duration')
                            if  'sports_value' in value_json:
                                value_json.update({'running_int_value': value_json['sports_value']['running']['int_value']})
                                value_json.pop('sports_value')

                            # 解包，映射
                            value_json.pop('steps_summary')
                            value_json.pop('calorie_summary')




                            # 跳过字段
                            if 'rank_percent_summary' in value_json:
                                value_json.pop('rank_percent_summary')

                            if 'all_fitness_goal_achieved_count' in value_json:
                                value_json.pop('all_fitness_goal_achieved_count')

                            if 'allFitnessGoalAchivevedDay' in value_json:
                                value_json.pop('allFitnessGoalAchivevedDay')

                            if 'all_fitness_goal_achieved_count_diff' in value_json:
                                value_json.pop('all_fitness_goal_achieved_count_diff')

                            if 'avg_achieved_fitness_goal_summary' in value_json:
                                value_json.pop('avg_achieved_fitness_goal_summary')

                            if 'cal_goal_achieve' in value_json:
                                value_json.pop('cal_goal_achieve')

                            if 'version' in value_json:
                                value_json.pop('version')

                            if 'sleep_report' in value_json and isinstance(value_json['sleep_report'],dict):
                                temp_value_json={}
                                for key in value_json['sleep_report'].keys():

                                    # 剔除字段
                                    if key == 'sleep_stage' or key == 'sleep_evaluation' or key == 'avg_friendly_score' or key == 'max_friendly_score':
                                        continue

                                    # 只解包，不映射
                                    temp_value_json.update({key:value_json['sleep_report'].get(key)})
                                value_json.pop('sleep_report')
                                value_json.update(temp_value_json)

                            if 'hlth_status' in value_json and isinstance(value_json['hlth_status'], dict):
                                temp_value_json = {}

                                # 只解包，不映射
                                for key in value_json['hlth_status'].keys():
                                    temp_value_json.update({key: value_json['hlth_status'].get(key)})
                                value_json.pop('hlth_status')
                                value_json.update(temp_value_json)

                        if main_record['key'] == 'watch_steps_record':
                            for idx, item in enumerate(value_json):
                                main_record.update(item)
                        else:
                            main_record.update(value_json)

                        fitness_data_top_ext = None
                        temp_dict = {}

                        # 一对多字段单独存储
                        if 'items' in main_record:
                            fitness_data_top_ext = main_record.pop('items')


                        batch.append((main_record, fitness_data_top_ext))

                        # 当批处理列表达到设定大小时，批量保存
                        if len(batch) >= BATCH_SIZE:
                            repo.save_batch_records(batch)
                            processed_rows += len(batch)
                            batch = []  # 清空批处理列表
                            print(f"已处理 {processed_rows}/{total_rows} 行数据")

                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        print(f"main_record1: {main_record}")
                        print(f"value_json: {value_json}")
                        print(f"警告：处理第 {total_rows} 行时数据格式有误，已跳过。错误: {e}")
                    except Exception as e:
                        print(f"main_record2: {main_record}")
                        print(f"value_json: {value_json}")
                        print(f"警告：处理第 {total_rows} 行时发生未知错误：{e}，已跳过。")

                # 处理剩余的记录
                if batch:
                    repo.save_batch_records(batch)
                    processed_rows += len(batch)
                    print(f"已处理 {processed_rows}/{total_rows} 行数据")

            print(f"\n处理完成！共扫描 {total_rows} 行，处理 {processed_rows + kisp_rows} 行。"
                  f"成功处理并存入数据库 {processed_rows} 行。跳过 {kisp_rows} 行。")

    except sqlite3.Error as e:
        print(f"\n数据库操作失败: {e}")
    except FileNotFoundError:
        print(f"错误：指定的文件夹路径不存在 '{DATA_FOLDER_PATH}'")
    except Exception as e:
        print(f"\n发生了未预料的错误: {e}")
    finally:
        # 这里的 hasattr(db_instance, 'conn') 是一个安全检查
        if 'db_instance' in locals() and hasattr(db_instance, 'conn') and db_instance.conn:
            end = time.perf_counter()
            print(f"执行时间：{(end - start) :.6f} 秒")  # 转换为毫秒显示，更易读
            db_instance.close()
            print("数据库连接已关闭。")