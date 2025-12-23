from src.DB.ImportSQLite.import_sqlite_Gcores_categories import GcoresCategoriesRepository
from src.DB.ImportSQLite.import_sqlite_Gcores_radios import GcoresRadiosRepository
from src.DB.ImportSQLite.import_sqlite_Gcores_user import GcoresUserRepository
from src.DB.ImportSQLite.import_sqlite_XiaomiBand5_down import fitness_data_down
from src.DB.ImportSQLite.import_sqlite_XiaomiBand5_middle import fitness_data_middle
from src.DB.ImportSQLite.import_sqlite_XiaomiBand5_top import fitness_data_top
from src.DB.ImportSQLite.import_sqlite_douban import DoubanRepository
from src.DB.ImportSQLite.import_sqlite_flomo import FlomoRepository
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
        pass



if __name__ == "__main__":

    db_execution = SQLite_util(app_config.SQLitePath)

    scanner = Scanner()

    fileMove = file_move()

    # TODO: 步骤1，将小米运动和flomo压缩包放到 data/zip 文件夹下,压缩包有密码时，压缩密码重命名为压缩包名

    # TODO: 步骤2，解压
    try:
        result = fileMove.unzip_all_zip_files(app_config.Decompress)
        print(f"\n成功解压的文件夹路径: {result}")
    except Exception as e:
        print(f"执行出错: {str(e)}")

    # TODO: 步骤3，移动文件到目标文件夹
    # flomo文件移动
    fileMove.move_matched_files(
        source_root=app_config.Decompress,
        target_dir=app_config.Flomo.file_path,
        file_names=["*的笔记*"],
        single_file_move=True
    )
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
    pass

    # TODO: 步骤4，执行自动化脚本获取数据（豆瓣，flomo，机核，（钱迹暂无））
    # 豆瓣
    get_douban_list()
    # flomo
    get_flomo_list()
    # 机核
    get_gcores_list()
    # 钱迹
    pass
    # 本地图片，视频，音频，笔记扫描，入库，full=True全量更新

    scanner.scan(full=True)

    # TODO: 步骤5，入库SQLite
    # 豆瓣数据入库
    repository = DoubanRepository(db_execution)
    repository.import_douban_SQLite()
    # flomo数据入库
    repository = FlomoRepository(db_execution)
    repository.import_flomo_SQLite()

    # 机核数据
    repository_Categories = GcoresCategoriesRepository(db_execution)
    repository_Radios = GcoresRadiosRepository(db_execution)
    repository_User = GcoresUserRepository(db_execution)

    # 机核专题数据入库
    repository_Categories.import_GcoresCategories_SQLite()
    # 机核电台数据入库
    repository_Radios.import_GcoresRadios_SQLite()
    # 机核用户数据入库
    repository_User.import_GcoresUser_SQLite()

    # 钱迹数据入库
    repository = QianjiRepository(db_execution)
    repository.import_Qianji_SQLite()

    # 小米运动数据入库
    repo_top = fitness_data_top(db_execution)
    repo_middle = fitness_data_middle(db_execution)
    repo_down = fitness_data_down(db_execution)

    repo_top.import_XiaomiBand5_top()
    repo_middle.import_XiaomiBand5_middle()
    repo_down.import_XiaomiBand5_down()







