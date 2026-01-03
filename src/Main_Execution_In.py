from src.DB.ImportSQLite.import_sqlite_Gcores_categories import GcoresCategoriesRepository
from src.DB.ImportSQLite.import_sqlite_Gcores_radios import GcoresRadiosRepository
from src.DB.ImportSQLite.import_sqlite_Gcores_user import GcoresUserRepository
from src.DB.ImportSQLite.import_sqlite_XiaoMiBand5 import XiaoMiBand5
from src.DB.ImportSQLite.import_sqlite_book import BookRepository
from src.DB.ImportSQLite.import_sqlite_douban import DoubanRepository
from src.DB.ImportSQLite.import_sqlite_flomo import FlomoRepository
from src.DB.ImportSQLite.import_sqlite_flyme import FlymeRepository
from src.DB.ImportSQLite.import_sqlite_qianji import QianjiRepository
from src.DB.SQLite_util import SQLite_util
from src.Script.Scanner_File.scannerFile import Scanner
from src.Script.Script_Repeat.douban import get_douban_list
from src.Script.Script_Repeat.flomo import get_flomo_list
from src.Script.Script_Repeat.gcores import get_gcores_list
from src.config.configClass import app_config
from src.utils.file_move import file_move


class MainExecution:
    def __init__(self):
        self.db_execution = SQLite_util(app_config.SQLitePath)



    def jsonExecution(self):
        '''
            读json，处理小米运动数据，钱迹，flomo（备用）

        '''
        fileMove = file_move()

        try:
            result = fileMove.unzip_all_zip_files(app_config.Decompress)
            print(f"\n成功解压的文件夹路径: {result}")
        except Exception as e:
            print(f"解压出错: {str(e)}")



        # 小米运动文件移动
        fileMove.move_matched_files(
            source_root=app_config.Decompress,
            target_dir=app_config.MiSmartBand5,
            file_names=["*_MiFitness_hlth_center_fitness_data.csv",
                        "*_MiFitness_user_fitness_data_records.csv",
                        "*_MiFitness_hlth_center_aggregated_fitness_data.csv"],
            single_file_move=True
        )
        # 钱迹文件移动
        fileMove.move_matched_files(
            source_root=app_config.Decompress,
            target_dir=app_config.qianji.path,
            file_names=["QianJi_日常生活*"],
            single_file_move=True
        )

        # 钱迹文件改名，移动到end文件夹
        app_config.rename_and_move_file(
            source_dir=app_config.qianji.path,  # 源文件夹（仅当前层级）
            target_dir=app_config.Data_End,  # 目标文件夹
            match_pattern="QianJi_日常生活*",  # 模糊匹配规则（如test开头的txt文件）
            new_full_name="qianji.json"  # 新完整名称（含后缀）
        )

        # flomo文件移动
        # fileMove.move_matched_files(
        #     source_root=app_config.Decompress,
        #     target_dir=app_config.Flomo.file_path,
        #     file_names=["*的笔记*"],
        #     single_file_move=True
        # )

        # flomo数据入库
        # repository = FlomoRepository(self.db_execution)
        # repository.import_flomo_SQLite()

        # 钱迹数据入库
        repository = QianjiRepository(self.db_execution)
        repository.import_Qianji_SQLite()

        # 小米运动数据入库
        repo = XiaoMiBand5(self.db_execution)
        repo.import_XiaomiBand5()

    def RepeatExecution(self):
        '''
            可高频重复执行的脚本，flomo，豆瓣
        '''


        # 豆瓣
        get_douban_list()
        # flomo
        get_flomo_list()

        # 豆瓣数据入库
        repository = DoubanRepository(self.db_execution)
        repository.import_douban_SQLite()

        # flomo数据入库
        repository = FlomoRepository(self.db_execution)
        repository.import_flomo_SQLite()


    def GcoresAndScannerExecution(self):
        '''
            机核数据获取与入库
        '''

        # 机核
        get_gcores_list()

        # 机核数据
        repository_Categories = GcoresCategoriesRepository(self.db_execution)
        repository_Radios = GcoresRadiosRepository(self.db_execution)
        repository_User = GcoresUserRepository(self.db_execution)

        # 机核专题数据入库
        repository_Categories.import_GcoresCategories_SQLite()
        # 机核电台数据入库
        repository_Radios.import_GcoresRadios_SQLite()
        # 机核用户数据入库
        repository_User.import_GcoresUser_SQLite()

        # 文件扫描
        scanner = Scanner()

        scanner.scan(full=True)

    def OneExecution(self):
        '''
            一次性执行的脚本，读书三个平台，flyme笔记
        '''

        repository = BookRepository(self.db_execution)
        repository.import_book_SQLite()

        repository = FlymeRepository(self.db_execution)
        repository.import_flyme_SQLite()


if __name__ == "__main__":
    execution = MainExecution()


    # # 需要下载数据的脚本（小米运动，钱迹，flomo数据入库）
    # execution.jsonExecution()
    #
    # # 高频重复执行的脚本（flomo，豆瓣）
    # execution.RepeatExecution()

    # 较重的脚本（机核数据，扫描本地）
    execution.GcoresAndScannerExecution()

    # 一次性执行的脚本（读书三个平台，flyme笔记）
    execution.OneExecution()


