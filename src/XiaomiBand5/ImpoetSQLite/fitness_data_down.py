import csv
import glob
import json
import os
import sqlite3
import time

from src.config.config import load_config
from src.db.DB import DB


class fitness_data_down:
    def __init__(self, db: DB):
        self.db = db

    def create_fitness_data_down_main(self):
        """
        创建用于存储【每日汇总数据】的表。
        """
        with self.db.transaction() as cursor:
            # 主表: fitness_data_main
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fitness_data_down_main (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT, -- 本次运动记录的唯一ID，用作外键
            
                -- === 核心元数据 (来自CSV外层) ===
                Uid INTEGER NOT NULL,                        -- 用户唯一标识
                Sid TEXT,                                    -- 数据来源ID (例如设备序列号)
                Key TEXT NOT NULL,                           -- 运动类型 (例如 'run', 'walk', 'cycle')
                Time INTEGER NOT NULL,                       -- 记录产生的时间戳 (Unix Timestamp)
                UpdateTime INTEGER,                          -- 记录的最后更新时间戳
            
               -- key=headset（耳机相关数据）字段
                end_time INTEGER,                              -- 耳机使用/连接结束时间（时间戳）
                high BOOLEAN,                                  -- 是否为高优先级/高功耗模式（布尔值）
                start_time INTEGER,                            -- 耳机使用/连接开始时间（时间戳）        
                
                -- key=steps（步数数据）字段
                -- time INTEGER,                                -- 步数记录时间（时间戳）
                steps INTEGER,                                  -- 记录的步数（整数）
                distance REAL,                                  -- 对应步数的行走距离（单位：米）
                calories REAL,                                  -- 对应步数消耗的卡路里（单位：千卡）
                
                -- key=calories（卡路里数据）字段
                -- 注：time和calories与steps中字段名重复，此处仍使用原始字段名
                -- calories_time INTEGER, -- 为避免冲突可考虑此命名方式，若无需则保留time
                -- calories_total REAL,
                
                -- key=dynamic（动态运动数据）字段
                -- 注：calories、distance、end_time、
                -- start_time、steps与其他key字段名重复
                type INTEGER,                                  -- 运动类型编码（如5代表特定运动）      
                
                -- key=valid_stand（有效站立数据）字段
                -- 注：end_time、start_time与headset等key字段名重复
                
                -- key=heart_rate（心率数据）字段
                bpm INTEGER,                                  -- 心率数值（单位：次/分钟）  
                -- 注：time与其他key字段名重复
                
                -- key=stress（压力值数据）字段
                stress INTEGER,                               -- 压力评分（0-100，数值越低压力越小）
                -- 注：time与其他key字段名重复
                
                -- key=pai（PAI健康指数数据）字段
                high_zone_pai REAL,                                 -- 高强度运动区间的PAI值
                low_zone_pai REAL,                                  -- 低强度运动区间的PAI值
                medium_zone_pai REAL,                               -- 中强度运动区间的PAI值
                daily_pai REAL,                                     -- 当日累计PAI值      
                date_time INTEGER,                                  -- 记录日期对应的时间戳（当日0点左右）
                total_pai REAL,                                     -- 总PAI值（综合各强度区间的累计指数）
                
                -- key=watch_daytime_sleep（日间睡眠数据）字段
                 bedtime INTEGER,                                  -- 日间入睡时间（0表示无明确固定入睡时间）  
                duration INTEGER,                                   -- 日间总睡眠时长（单位：分钟）
                timezone INTEGER,                                   -- 时区编码（对应特定时区偏移）
                wake_up_time INTEGER,                               -- 日间醒来时间（0表示无明确固定醒来时间）
                -- 注：date_time与pai中字段名重复
                
                -- key=sleep（综合睡眠数据）字段
                sleep_deep_duration INTEGER,                        -- 深度睡眠时长（单位：分钟）
                device_bedtime INTEGER,                             -- 设备记录的入睡时间
                device_wake_up_time INTEGER,                        -- 设备记录的醒来时间（时间戳）
                sleep_light_duration INTEGER,                       -- 浅度睡眠时长（单位：分钟）
                protoTime INTEGER,                                  -- 原始记录时间戳（与醒来时间一致）
                sleep_trace_duration INTEGER,                       -- 睡眠轨迹记录总时长（与总睡眠时长一致）
                awake_count INTEGER,                                -- 夜间清醒次数（整数）
                sleep_awake_duration INTEGER,                       -- 清醒总时长（单位：分钟）
                -- 注：bedtime、duration、timezone、wake_up_time与其他key字段名重复
                
                -- key=watch_night_sleep（夜间睡眠数据）字段
                sleep_rem_duration INTEGER,                        -- REM睡眠时长（快速眼动睡眠，单位：分钟） 
                -- 注：awake_count、sleep_awake_duration、
                -- bedtime、sleep_deep_duration、sleep_light_duration、
                -- duration、date_time、timezone、wake_up_time与其他key字段名重复
                
                -- key=resting_heart_rate（静息心率数据）字段
                -- 注：bpm、date_time与其他key字段名重复
                
                -- key=intensity（运动强度数据）字段
                -- 注：time与其他key字段名重复
                
                -- key=weight（体重数据）字段
                bmi REAL,                                   -- 身体质量指数（BMI）
                weight REAL,                                -- 体重数值（单位：千克）
                body_fat_rate REAL,                         -- 体脂率
                moisture_rate REAL,                         -- 身体水分率
                bone_mass REAL,                             -- 骨重
                basal_metabolism REAL,                      -- 基础代谢率
                muscle_rate REAL,                           -- 肌肉率
                protein_rate REAL,                          -- 蛋白质率
                visceral_fat INTEGER,                       -- 内脏脂肪等级
                -- 注：time与其他key字段名重复
                
                -- key=single_heart_rate（单次心率数据）字段
                -- 注：bpm、time与其他key字段名重复
                
                -- key=noise（噪音数据）字段
                decibel INTEGER,                            -- 环境噪音分贝值

                -- 联合唯一约束，防止同一用户在同一时间点重复记录
                UNIQUE (Uid, Time, Key)
            );
            """)
            # 从表: nested_items
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS fitness_data_down_ext (
                detail_id INTEGER PRIMARY KEY AUTOINCREMENT,    -- 每个运动分段的全局唯一ID
                main_record_id INTEGER NOT NULL,                -- 外键，指向 fitness_data_down_ext 表中的某条运动记录
                
                -- === 分段详情内容 ===
                item_json TEXT NOT NULL,                      -- 存储单个运动分段完整信息的JSON对象。
                                                                -- 例如: {"duration": 300, "distance": 1000, "pace": 5.0, ...}
                
                -- 定义外键约束，并设置级联删除
                FOREIGN KEY (main_record_id) 
                    REFERENCES fitness_data_down_main(record_id) 
                    ON DELETE CASCADE -- 当主运动记录被删除时，其所有分段详情也会被自动删除
            );
            """)
        print("【传感器底层】的表结构已在数据库中准备就绪。")

        
    def save_batch_records(self, batch_data):
        """
        批量保存多条记录，所有操作在一个事务中完成，提高处理大量数据时的性能。
        
        Args:
            batch_data (list): 包含(record_data, fitness_data_down_ext_list)元组的列表
        """
        if not batch_data:
            return
            
        with self.db.transaction() as cursor:
            for record_data, fitness_data_down_ext_list in batch_data:
                # 步骤 1: UPSERT主表记录
                columns = ', '.join(record_data.keys())
                if 'total_score' in record_data:
                    print(f"total_score: {record_data['total_score']}")
                placeholders = ', '.join(['?'] * len(record_data))
                values = list(record_data.values())
                update_assignments = ', '.join([f"{col} = excluded.{col}" for col in record_data.keys()])
                sql_upsert = f"INSERT INTO fitness_data_down_main ({columns}) VALUES ({placeholders}) ON CONFLICT(Uid, Time, Key) DO UPDATE SET {update_assignments};"
                cursor.execute(sql_upsert, values)

                # 步骤 2: 获取刚刚操作过的主记录的 record_id
                cursor.execute("SELECT record_id FROM fitness_data_down_main WHERE Uid=? AND Time=? AND Key=?",
                               (record_data['Uid'], record_data['Time'], record_data['Key']))
                main_record_id = cursor.fetchone()[0]

                # 步骤 3: 删除所有与该主记录关联的旧子项目
                cursor.execute("DELETE FROM fitness_data_down_ext WHERE main_record_id = ?", (main_record_id,))

                # 步骤 4: 批量插入所有新的子项目
                if fitness_data_down_ext_list:
                    items_to_insert = [(main_record_id, json.dumps(item)) for item in fitness_data_down_ext_list]
                    cursor.executemany("INSERT INTO fitness_data_down_ext (main_record_id, item_json) VALUES (?, ?)",
                                      items_to_insert)


# --- 【全新】用于测试“单次运动记录”功能的测试用例 ---
if __name__ == '__main__':

    start = time.perf_counter()

    config = load_config()
    db_instance = DB(config.database.path)
    try:
        repo = fitness_data_down(db_instance)
        repo.create_fitness_data_down_main()

        CSV_FILENAME_PATTERN = '*_MiFitness_hlth_center_fitness_data.csv'
        DATA_FOLDER_PATH = "C:/Users/28484/Desktop/20251209_1311597473_MiFitness_c3_data_copy (1)"

        search_path = os.path.join(DATA_FOLDER_PATH, CSV_FILENAME_PATTERN)
        files_found = glob.glob(search_path)

        if not files_found:
            print(f"错误：在文件夹 '{DATA_FOLDER_PATH}' 中没有找到匹配 '{CSV_FILENAME_PATTERN}' 的文件。")
        else:
            source_file = files_found[0]
            print(f"成功找到源文件：'{source_file}'，开始处理...")

            # 使用批处理，每次处理10000条记录，在连续处理35万条记录后速度大幅下降
            BATCH_SIZE = 1  # 批处理大小，可以根据系统性能调整
            batch = []
            
            with (open(source_file, mode='r', encoding='utf-8') as csvfile):
                reader = csv.DictReader(csvfile)
                total_rows = 0
                processed_rows = 0

                # 目前效率有点低，连续处理35万行数据后数据库速度明显下降，
                # 可能是因为数据库事务提交的频率 too many times

                # 第一步：跳过前skip_lines行
                # for _ in range(1580000):
                #     try:
                #         # 每次调用next()跳过一行，直到跳过指定行数
                #         next(reader)
                #         print(f"跳过 1580000 行")
                #     except StopIteration:
                #         # 若文件行数不足，直接退出（避免报错）
                #         print(f"文件行数不足 1580000 行")
                #         break

                for row in reader:
                    total_rows += 1
                    try:
                        main_record = {
                            'Uid': int(row['Uid']), 'Sid': row['Sid'],
                            'Key': row['Key'], 'Time': int(row['Time']), 'UpdateTime': int(row['UpdateTime'])
                        }


                        value_json = json.loads(row['Value'])
                        main_record.update(value_json)
                        #
                        # if main_record['Key'] == 'heart_rate':
                        #     if main_record.get('latest_hr', None) is not None:
                        #         # 当key为heart_rate时，value的json字段中有一个嵌套字段latest_hr，记录的是当天最后一次心率，这里直接去掉
                        #         main_record.pop('latest_hr')
                        #
                        # if main_record['Key'] == 'goal':
                        #     if main_record.get('sidList', None) is not None:
                        #         main_record.pop('sidList')

                        fitness_data_down_ext = None


                        if 'time' in main_record:
                            # value的json中会有time字段，和通用字段重合了，这里直接去掉
                            main_record.pop('time')
                        if 'total_score' in main_record:
                            # 睡眠数据中，可能会有睡眠评分数据，去掉
                            main_record.pop('total_score')
                        if 'friendly_score' in main_record:
                            main_record.pop('friendly_score')

                        if 'items' in main_record:
                            fitness_data_down_ext = main_record.pop('items')


                        batch.append((main_record, fitness_data_down_ext))
                        
                        # 当批处理列表达到设定大小时，批量保存
                        if len(batch) >= BATCH_SIZE:
                            repo.save_batch_records(batch)
                            processed_rows += len(batch)
                            batch = []  # 清空批处理列表
                            print(f"已处理 {processed_rows}/{total_rows} 行数据")

                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        print(f"main_record2: {main_record}")
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

            print(f"\n处理完成！共扫描 {total_rows} 行，成功处理并存入数据库 {processed_rows} 行。")

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