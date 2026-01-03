import csv
import glob
import json
import os
import sqlite3
import time
import datetime
from typing import Dict

from src.DB.SQLite_util import SQLite_util
from src.config.configClass import app_config


class XiaoMiBand5:
    def __init__(self, db: SQLite_util):
        self.db = db
        # 预生成 p1-p288 的列名，避免在循环中重复生成
        self.p_cols = [f"p{i}" for i in range(1, 289)]

    def create_XiaoMiBand5_table(self):
        with self.db.transaction() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS XiaoMiBand5 (
                dataTime TEXT PRIMARY KEY,	        -- 时间到天，YYYY-MM-DD
                sleep_rem_duration INTEGER,	        -- 快速眼动时长，分钟
                sleep_awake_count INTEGER,	        -- 夜间醒来次数
                sleep_awake_duration INTEGER,	    -- 清醒总时长
                sleep_bedtime TEXT,	                -- 入睡时间
                sleep_wake_up_time TEXT,	        -- 醒来时间
                sleep_deep_duration INTEGER,	    -- 深度睡眠总时长
                sleep_light_duration INTEGER,	    -- 浅度睡眠总时长
                sleep_duration INTEGER,	            -- 总睡眠时长
                steps INTEGER,	                    -- 单日总步数
                steps_distance INTEGER,	            -- 单日总距离
                steps_activerDuration INTEGER,	    -- 活动持续时间
                calories REAL,	                    -- 单日总卡路里
                hrm_avg INTEGER, 	                -- 平均心率
                hrm_max_time TEXT,	                -- 最高心率出现时间
                hrm_max INTEGER,	                -- 最高心率
                hrm_min_time TEXT,	                -- 最低心率出现时间
                hrm_min INTEGER,	                -- 最低心率
                resting_heart_rate INTEGER,	        -- 静息心率
                weight REAL,	                    -- 体重
                bmi REAL	                        -- bmi指数
                -- 联合唯一约束，防止同一用户在同一时间点重复记录
                -- UNIQUE (Uid, Time, Key)
            );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_XiaoMiBand5_dataTime ON XiaoMiBand5(dataTime);")

            # 动态生成288张表的SQL
            p_sql = ", ".join([f"{p} REAL" for p in self.p_cols])
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS XiaoMiBand5_288 (
                dataTime TEXT,
                key TEXT,
                                    -- steps	            步数
                                    -- steps_distance	    行走距离，米
                                    -- calories	            卡路里
                                    -- valid_stand	        站立次数
                                    -- heart_rate	        心率
                                    -- stress	            压力（0-100）
                                    -- sleep_detailed	    睡眠记录
                {p_sql},
                UNIQUE (dataTime, key)
            );
            """)
        print("XiaoMiBand5/XiaoMiBand5_288的表结构已准备就绪。")

    def _batch_upsert(self, day_rows, day_288_rows):
        """一次性事务写入，比循环 upsert 快几百倍"""
        with self.db.transaction() as cursor:
            # 写入 Day 表
            if day_rows:
                sql_day = """
                    INSERT INTO XiaoMiBand5 (
                        dataTime, sleep_rem_duration, sleep_awake_count, sleep_awake_duration, 
                        sleep_bedtime, sleep_wake_up_time, sleep_deep_duration, sleep_light_duration, 
                        sleep_duration, steps, steps_distance, steps_activerDuration, calories, 
                        hrm_avg, hrm_max_time, hrm_max, hrm_min_time, hrm_min, resting_heart_rate, weight, bmi
                    ) VALUES (
                        :dataTime, :sleep_rem_duration, :sleep_awake_count, :sleep_awake_duration, 
                        :sleep_bedtime, :sleep_wake_up_time, :sleep_deep_duration, :sleep_light_duration, 
                        :sleep_duration, :steps, :steps_distance, :steps_activerDuration, :calories, 
                        :hrm_avg, :hrm_max_time, :hrm_max, :hrm_min_time, :hrm_min, :resting_heart_rate, :weight, :bmi
                    ) ON CONFLICT(dataTime) DO UPDATE SET
                        sleep_rem_duration=excluded.sleep_rem_duration, sleep_awake_count=excluded.sleep_awake_count,
                        sleep_awake_duration=excluded.sleep_awake_duration, sleep_bedtime=excluded.sleep_bedtime,
                        sleep_wake_up_time=excluded.sleep_wake_up_time, sleep_deep_duration=excluded.sleep_deep_duration,
                        sleep_light_duration=excluded.sleep_light_duration, sleep_duration=excluded.sleep_duration,
                        resting_heart_rate=excluded.resting_heart_rate, weight=excluded.weight, bmi=excluded.bmi
                """
                cursor.executemany(sql_day, list(day_rows))

            # 写入 288 表
            if day_288_rows:
                p_cols_str = ", ".join(self.p_cols)
                p_placeholders = ", ".join([f":{p}" for p in self.p_cols])
                p_updates = ", ".join([f"{p}=excluded.{p}" for p in self.p_cols])
                sql_288 = f"""
                    INSERT INTO XiaoMiBand5_288 (dataTime, key, {p_cols_str})
                    VALUES (:dataTime, :key, {p_placeholders})
                    ON CONFLICT(dataTime, key) DO UPDATE SET {p_updates}
                """
                cursor.executemany(sql_288, list(day_288_rows))

    def get_nearest_5min_point(self, timestamp):
        """逻辑保持不变，但优化内部性能"""
        """
        计算Unix时间戳对应的时间，距离当天最近的5分钟整点（如08:00、08:05、08:10...）
        :param timestamp: 秒级Unix时间戳
        :return: 包含最近时间的字典（datetime对象、字符串、时间戳、区间序号）
        """
        # 1. 将时间戳转为本地时区的datetime对象（核心：保留可计算性）
        target_dt = datetime.datetime.fromtimestamp(timestamp)

        # 2. 计算当天0点的datetime对象（基准时间）
        day_start = target_dt.replace(hour=0, minute=0, second=0, microsecond=0)

        # 3. 计算目标时间距离当天0点的总分钟数
        delta_minutes = (target_dt - day_start).total_seconds() / 60

        # 4. 按5分钟区间计算最近的点（关键逻辑）
        interval = 5
        remainder = delta_minutes % interval # 余数：距离上一个区间点的分钟数

        if remainder < interval / 2:
            # 余数<2.5分钟：取上一个5分钟点
            nearest_minutes = delta_minutes - remainder
        else:
            # 余数≥2.5分钟：取下一个5分钟点
            nearest_minutes = delta_minutes + (interval - remainder)

        # 防止超过当天总分钟数（24*60=1440）
        nearest_minutes = min(nearest_minutes, 1440)

        # 5. 计算最近点的datetime对象
        nearest_dt = day_start + datetime.timedelta(minutes=nearest_minutes)

        # 6. 计算额外信息：区间序号（全天288个点，从0开始：0=00:00,1=00:05,...,287=23:55）
        interval_index = int(nearest_minutes / interval)
        if interval_index == 0: interval_index = 1

        return {
            "date_str": target_dt.strftime("%Y-%m-%d"),  # 易读的字符串 %Y-%m-%d
            "interval_index": interval_index,            # 全天288个点的序号（1-288）
            "datetime_obj": nearest_dt
        }

    def _get_init_288_dict(self, dataTime, key):
        """快速生成初始化的288点字典"""
        d = {'dataTime': dataTime, 'key': key}
        d.update({p: 0 for p in self.p_cols})
        return d


    def _get_init_day_dict(self, date_str):
        return {
            'dataTime': date_str, 'sleep_rem_duration': 0, 'sleep_awake_count': 0,
            'sleep_awake_duration': 0, 'sleep_bedtime': '1970-01-01',
            'sleep_wake_up_time': '1970-01-01', 'sleep_deep_duration': 0, 'sleep_light_duration': 0,
            'sleep_duration': 0, 'steps': 0, 'steps_distance': 0, 'steps_activerDuration': 0, 'calories': 0,
            'hrm_avg': 0, 'hrm_max_time': '1970-01-01', 'hrm_max': 0, 'hrm_min_time': '1970-01-01',
            'hrm_min': 0, 'resting_heart_rate': 0, 'weight': 0, 'bmi': 0
        }


    def _fill_sleep_to_288_map(self, items, day_288_map, current_date_str):
        """
        将睡眠片段填充到288个5分钟区间的字典中
        :param items: 睡眠片段列表（每个元素是含start_time/end_time/state的字典）
        :param day_288_map: 总的288点字典，python中字典是一个可变对象，所以不需要返回值，传过来的字典的引用
        :param sleep_288_next: 下一天的288点字典
        :return: 填充后的288点字典
        """

        for item in items:
            # 提取当前片段的开始时间戳、结束时间戳和睡眠状态码
            start_ts = item["start_time"]
            end_ts = item["end_time"]
            state = item["state"]

            # 调用工具函数，计算开始时间戳对应的：日期字符串、在288个点中的索引、时间对象
            start_info = self.get_nearest_5min_point(start_ts)
            # 同上，计算结束时间的信息
            end_info = self.get_nearest_5min_point(end_ts)

            # 提取具体的索引位置（1-288）和具体的日期（YYYY:MM:DD）
            s_idx = start_info['interval_index']  # 开始点索引
            e_idx = end_info['interval_index']  # 结束点索引
            s_date = start_info['date_str']  # 开始日期
            e_date = end_info['date_str']  # 结束日期

            # 确定受此睡眠片段影响的日期列表
            target_dates = [s_date]
            # 如果开始日期和结束日期不一样（即跨天了），把结束日期也加进待处理列表
            if e_date != s_date:
                target_dates.append(e_date)

            # 循环处理每一个受影响的日期（通常是一天，跨天则是两天）
            for d_str in target_dates:
                # 以 (日期, 键名) 作为唯一标识查找或创建该天的 288 字典数据
                dict_key = (d_str, 'sleep_detailed')
                if dict_key not in day_288_map:
                    # 如果该日期的数据还没初始化，则生成一个全是 0 的 288 字典
                    day_288_map[dict_key] = self._get_init_288_dict(d_str, 'sleep_detailed')

                # 获取该日期对应的字典引用
                target_row = day_288_map[dict_key]

                # 计算在该特定日期内，循环填充的起点和终点
                # 如果是开始日期，从计算出的索引开始；否则（即跨天后的第二天）从 1 号点开始
                loop_start = s_idx if d_str == s_date else 1
                # 如果是结束日期，到计算出的索引结束；否则（即跨天的第一天）填充到最后一刻 288
                loop_end = e_idx if d_str == e_date else 288

                # 修正边界逻辑：如果是跨天片段的第一天，必须强制填充到 288
                if d_str == s_date and e_date != s_date:
                    loop_end = 288

                # 执行填充：将对应的 p1...p288 字段设置为当前的睡眠状态码
                for i in range(loop_start, loop_end + 1):
                    target_row[f"p{i}"] = state

    def import_XiaomiBand5(self):
        start_perf = time.perf_counter()

        # 核心优化：使用字典存储结果，实现 O(1) 查找
        day_map = {}  # key: dataTime (str)
        day_288_map = {}  # key: (dataTime, key_name) (tuple)

        try:
            self.create_XiaoMiBand5_table()
            CSV_FILENAME_PATTERN = '*_MiFitness_hlth_center_fitness_data.csv'
            search_path = os.path.join(app_config.MiSmartBand5, CSV_FILENAME_PATTERN)
            files_found = glob.glob(search_path)

            if not files_found:
                print(f"未找到文件")
                return

            source_file = files_found[0]

            # 预先获取总行数以计算进度（对200万行而言这很快）
            print(f"底层表，正在预估总行数...")
            with open(source_file, 'r', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f) - 1
            print(f"开始处理 {total_rows} 行数据...")

            with open(source_file, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)

                for row_idx, row in enumerate(reader):
                    Key = row['Key']
                    unixTime = int(row['Time'])
                    value_json = json.loads(row['Value'])

                    time_info = self.get_nearest_5min_point(unixTime)
                    date_str = time_info['date_str']
                    idx = time_info['interval_index']
                    p_key = f"p{idx}"

                    # --- 288表逻辑 ---
                    if Key == 'watch_daytime_sleep':
                        items = value_json.get('items', [])
                        self._fill_sleep_to_288_map(items, day_288_map, date_str)

                    elif Key == 'steps':
                        for sub_k in ['steps', 'distance', 'calories']:
                            dk = (date_str, sub_k)
                            if dk not in day_288_map:
                                day_288_map[dk] = self._get_init_288_dict(date_str, sub_k)
                            day_288_map[dk][p_key] += value_json.get(sub_k, 0)

                    elif Key == 'valid_stand':
                        dk = (date_str, 'valid_stand')
                        if dk not in day_288_map:
                            day_288_map[dk] = self._get_init_288_dict(date_str, 'valid_stand')

                        # 保持原逻辑：计算当前已有的非0点数
                        current_count = sum(1 for p in self.p_cols if day_288_map[dk][p] != 0)
                        start_t = value_json.get('start_time', 0)
                        end_t = value_json.get('end_time', 0)

                        # 借用填充函数，state 为 count + 1
                        self._fill_sleep_to_288_map(
                            [{'start_time': start_t, 'end_time': end_t, 'state': current_count + 1}],
                            day_288_map, date_str)

                    elif Key == 'heart_rate':
                        dk = (date_str, 'heart_rate')
                        if dk not in day_288_map:
                            day_288_map[dk] = self._get_init_288_dict(date_str, 'heart_rate')
                        day_288_map[dk][p_key] = value_json.get('bpm', 0)

                    elif Key == 'stress':
                        dk = (date_str, 'stress')
                        if dk not in day_288_map:
                            day_288_map[dk] = self._get_init_288_dict(date_str, 'stress')
                        day_288_map[dk][p_key] = value_json.get('stress', 0)

                    # --- day表逻辑 ---
                    elif Key == 'watch_night_sleep':
                        # 处理 288 的睡眠细节
                        items = value_json.get('items', [])
                        self._fill_sleep_to_288_map(items, day_288_map, date_str)

                        # 处理 day 表
                        if date_str not in day_map:
                            day_map[date_str] = self._get_init_day_dict(date_str)

                        day_map[date_str].update({
                            'sleep_awake_count': value_json.get('awake_count', 0),
                            'sleep_awake_duration': value_json.get('sleep_awake_duration', 0),
                            'sleep_bedtime': datetime.datetime.fromtimestamp(int(value_json.get('bedtime', 0))).strftime("%Y-%m-%d %H:%M:%S"),
                            'sleep_wake_up_time': datetime.datetime.fromtimestamp(int(value_json.get('wake_up_time', 0))).strftime("%Y-%m-%d %H:%M:%S"),
                            'sleep_deep_duration': value_json.get('sleep_deep_duration', 0),
                            'sleep_light_duration': value_json.get('sleep_light_duration', 0),
                            'sleep_duration': value_json.get('duration', 0),
                            'sleep_rem_duration': value_json.get('sleep_rem_duration', 0)
                        })

                    elif Key == 'resting_heart_rate':
                        if date_str not in day_map: day_map[date_str] = self._get_init_day_dict(date_str)
                        day_map[date_str]['resting_heart_rate'] = value_json.get('bpm', 0)

                    elif Key == 'weight':
                        if date_str not in day_map: day_map[date_str] = self._get_init_day_dict(date_str)
                        day_map[date_str]['weight'] = value_json.get('weight', 0)
                        day_map[date_str]['bmi'] = value_json.get('bmi', 0)

                    # 每1万行打印进度
                    if (row_idx + 1) % 10000 == 0:
                        now_perf = time.perf_counter()
                        speed = (row_idx + 1) / (now_perf - start_perf)
                        print(f"已处理 {row_idx + 1}/{total_rows} 行，当前速度：{speed:.2f} 行/秒")





            CSV_FILENAME_PATTERN = '*_MiFitness_user_fitness_data_records.csv'
            search_path = os.path.join(app_config.MiSmartBand5, CSV_FILENAME_PATTERN)
            files_found = glob.glob(search_path)

            if not files_found:
                print(f"未找到文件")
                return

            source_file = files_found[0]

            # 预先获取总行数以计算进度（对200万行而言这很快）
            print(f"中层表，正在预估总行数...")
            with open(source_file, 'r', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f) - 1
            print(f"开始处理 {total_rows} 行数据...")

            with open(source_file, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)

                for row_idx, row in enumerate(reader):
                    Key = row['key']
                    unixTime = int(row['time'])


                    time_info = self.get_nearest_5min_point(unixTime)
                    date_str = time_info['date_str']

                    # --- day表逻辑 ---
                    if Key == 'watch_steps_report':
                        value_json = json.loads(row['value'])

                        # 处理 day 表
                        if date_str not in day_map:
                            day_map[date_str] = self._get_init_day_dict(date_str)

                        day_map[date_str].update({
                            'steps': value_json.get('steps', 0),
                            'steps_distance': value_json.get('distance', 0),
                            'step_active_duration': value_json.get('activeDuration', 0),
                            'calories': value_json.get('calories', 0),
                        })
                    elif Key == 'watch_hrm_report':
                        value_json = json.loads(row['value'])

                        # 处理 day 表
                        if date_str not in day_map:
                            day_map[date_str] = self._get_init_day_dict(date_str)

                        day_map[date_str].update({
                            'hrm_avg': value_json.get('avg_hrm', 0),
                            'hrm_max_time': datetime.datetime.fromtimestamp(
                                int(value_json.get('max_hrm', {'time': 0}).get('time'))).strftime("%Y-%m-%d %H:%M:%S"),
                            'hrm_max': value_json.get('max_hrm', {'hrm': 0}).get('hrm'),
                            'hrm_min_time': datetime.datetime.fromtimestamp(
                                int(value_json.get('min_hrm', {'time': 0}).get('time'))).strftime("%Y-%m-%d %H:%M:%S"),
                            'hrm_min': value_json.get('min_hrm', {'hrm': 0}).get('hrm'),
                        })


            # --- 批量写入数据库 ---
            print(f"解析完成，正在写入数据库...")
            self._batch_upsert(day_map.values(), day_288_map.values())

        except Exception as e:
            print(f"\n发生了未预料的错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if hasattr(self.db, 'conn') and self.db.conn:
                end = time.perf_counter()
                print(f"执行总时间：{(end - start_perf) :.2f} 秒")
                self.db.close()
                print("数据库连接已关闭。")




if __name__ == '__main__':
    db_instance = SQLite_util(app_config.SQLitePath)
    repo = XiaoMiBand5(db_instance)
    repo.import_XiaomiBand5()