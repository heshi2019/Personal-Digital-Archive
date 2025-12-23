import fnmatch
import os
import shutil
import zipfile
from typing import Tuple, List, Dict, Any, Set

from src.config.configClass import app_config


class file_move:
    def smart_extract_zip(self, zip_path: str, extract_dir: str):
        """
        智能解压ZIP文件，自动处理单根文件夹冗余问题
        :param zip_path: ZIP压缩包路径
        :param extract_dir: 目标解压目录（最终文件/文件夹会放在此目录下）
        """
        # 确保目标解压目录存在
        os.makedirs(extract_dir, exist_ok=True)

        # 临时解压目录（用于先分析结构）
        temp_extract = os.path.join(extract_dir, "_temp_extract")
        os.makedirs(temp_extract, exist_ok=True)

        try:
            # 第一步：解压到临时目录
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # 尝试无密码解压，如需密码可添加pwd参数
                zf.extractall(temp_extract)

            # 第二步：分析临时目录下的文件结构
            temp_contents = os.listdir(temp_extract)
            # 过滤掉MAC系统的隐藏文件（避免干扰结构判断）
            temp_contents = [f for f in temp_contents if not f.startswith('__MACOSX')]

            # 情况1：临时目录下只有一个文件夹（单根文件夹）
            if len(temp_contents) == 1 and os.path.isdir(os.path.join(temp_extract, temp_contents[0])):
                single_folder = os.path.join(temp_extract, temp_contents[0])
                # 将单根文件夹内的所有内容移动到最终解压目录
                for item in os.listdir(single_folder):
                    src = os.path.join(single_folder, item)
                    dst = os.path.join(extract_dir, item)
                    # 处理移动冲突（覆盖/跳过，此处选择覆盖）
                    if os.path.exists(dst):
                        if os.path.isdir(dst):
                            shutil.rmtree(dst)
                        else:
                            os.remove(dst)
                    shutil.move(src, extract_dir)

            # 情况2：临时目录下是多个文件/文件夹（无单根）
            else:
                # 直接将所有内容移动到最终解压目录
                for item in temp_contents:
                    src = os.path.join(temp_extract, item)
                    dst = os.path.join(extract_dir, item)
                    if os.path.exists(dst):
                        if os.path.isdir(dst):
                            shutil.rmtree(dst)
                        else:
                            os.remove(dst)
                    shutil.move(src, extract_dir)

        finally:
            # 清理临时目录
            if os.path.exists(temp_extract):
                shutil.rmtree(temp_extract)


    def unzip_all_zip_files(self, folder_path: str) -> tuple:
        """
        解压指定文件夹下所有ZIP压缩包，解压到同名文件夹，密码尝试使用压缩包名称

        Args:
            folder_path: 包含ZIP压缩包的文件夹路径

        Returns:
            元组，包含所有成功解压后的文件夹路径

        Raises:
            NotADirectoryError: 传入的路径不是有效目录
            FileNotFoundError: 传入的路径不存在
        """

        # 存储成功解压的路径
        extracted_paths: List[str] = []

        # 遍历文件夹下所有文件
        for file_name in os.listdir(folder_path):
            # 筛选出ZIP文件（不区分大小写）
            if file_name.lower().endswith('.zip'):
                # 构建压缩包完整路径
                zip_file_path = os.path.join(folder_path, file_name)
                # 获取压缩包名称（不含扩展名）作为解压文件夹名和密码
                zip_name = os.path.splitext(file_name)[0]
                # 构建解压目标文件夹路径
                extract_folder = os.path.join(folder_path, zip_name)

                # 判断文件夹是否存在
                if os.path.exists(extract_folder):
                    # 存在则递归删除整个文件夹（包括里面的所有文件/子文件夹）
                    shutil.rmtree(extract_folder)
                else:
                    # 不存在则创建文件夹（多层级也能创建，exist_ok=True避免重复创建报错）
                    os.makedirs(extract_folder, exist_ok=True)

                print(f"开始解压: {zip_file_path} -> {extract_folder}")

                # 智能解压（核心解决层级冗余问题）
                try:
                    with zipfile.ZipFile(zip_file_path, 'r') as zf:
                        # 先试无密码，再试压缩包名作为密码
                        try:
                            self.smart_extract_zip(zip_file_path, extract_folder)
                        except RuntimeError as e:
                            if "password" in str(e).lower():
                                # 带密码的情况：先修改smart_extract_zip支持密码，再解压
                                def extract_with_pwd():
                                    temp_extract = os.path.join(extract_folder, "_temp_extract")
                                    os.makedirs(temp_extract, exist_ok=True)
                                    zf.extractall(temp_extract, pwd=zip_name.encode('utf-8'))
                                    return temp_extract

                                temp_extract = extract_with_pwd()
                                # 复用结构分析逻辑
                                temp_contents = [f for f in os.listdir(temp_extract) if not f.startswith('__MACOSX')]
                                if len(temp_contents) == 1 and os.path.isdir(os.path.join(temp_extract, temp_contents[0])):
                                    single_folder = os.path.join(temp_extract, temp_contents[0])
                                    for item in os.listdir(single_folder):
                                        shutil.move(os.path.join(single_folder, item), extract_folder)
                                else:
                                    for item in temp_contents:
                                        shutil.move(os.path.join(temp_extract, item), extract_folder)
                                shutil.rmtree(temp_extract)
                            else:
                                raise e
                    extracted_paths.append(extract_folder)
                    print(f"成功解压: {zip_file_path} -> {extract_folder}")
                except Exception as e:
                    print(f"解压失败: {zip_file_path}, 错误: {str(e)}")
        return tuple(extracted_paths)

    def move_matched_files(
            self,
            source_root: str,
            target_dir: str,
            file_names: List[str],
            single_file_move: bool = True
    ) -> None:
        """
        按规则移动匹配的文件（遍历源目录所有子目录）
        规则1（单文件匹配）：list长度=1 且 single_file_move=True → 移动匹配到的该文件
        规则2（多文件匹配）：list长度>1 → 所有匹配文件需在同一父目录 → 移动该父目录下所有文件（不含子目录）

        Args:
            source_root: 源根目录（遍历所有子目录匹配文件）
            target_dir: 目标目录（不存在则自动创建）
            file_names: 要匹配的文件名列表（支持精确匹配，如需通配符可扩展）
            single_file_move: 单文件匹配时是否移动（布尔值，多文件匹配时无效）

        Raises:
            FileNotFoundError: 源目录不存在
            ValueError: 无匹配文件 / 多文件匹配时文件不在同一父目录
            RuntimeError: 文件移动失败
        """
        # 基础校验：源目录是否存在
        if not os.path.exists(source_root):
            raise FileNotFoundError(f"源根目录不存在: {source_root}")
        # 确保目标目录存在
        os.makedirs(target_dir, exist_ok=True)

        # 第一步：遍历所有目录，收集匹配文件的「父目录-文件」映射
        matched_files: Dict[str, Set[str]] = {}  # key: 父目录路径, value: 该目录下匹配的文件集合
        for root, _, files in os.walk(source_root):
            # 筛选当前目录下匹配的文件（精确匹配，如需通配符可替换为fnmatch.fnmatch）
            # current_matched = [f for f in files if f in file_names]
            current_matched = [f for f in files if any(fnmatch.fnmatch(f, pattern) for pattern in file_names)]

            if current_matched:
                if root not in matched_files:
                    matched_files[root] = set()
                matched_files[root].update(current_matched)

        # 校验：是否找到匹配文件
        if not matched_files:
            raise ValueError(f"在源目录[{source_root}]及其子目录中，未找到匹配的文件: {file_names}")

        # 第二步：分支处理匹配逻辑
        if len(file_names) == 1:
            # 分支1：单文件匹配 → 移动匹配到的文件（取第一个匹配项，可扩展为移动所有匹配项）
            if not single_file_move:
                print("单文件匹配模式，但single_file_move=False，不执行移动")
                return

            # 取第一个匹配的文件（如需移动所有匹配的单文件，可遍历matched_files.items()）
            parent_dir, files_set = next(iter(matched_files.items()))
            target_file = list(files_set)[0]
            source_file_path = os.path.join(parent_dir, target_file)
            target_file_path = os.path.join(target_dir, target_file)

            # 处理目标文件已存在的情况（覆盖）
            if os.path.exists(target_file_path):
                if os.path.isdir(target_file_path):
                    shutil.rmtree(target_file_path)  # 若目标是目录则删除
                else:
                    os.remove(target_file_path)  # 若目标是文件则删除

            # 执行移动
            try:
                shutil.move(source_file_path, target_file_path)
                print(f"单文件移动完成: {source_file_path} → {target_file_path}")
            except Exception as e:
                raise RuntimeError(f"移动文件失败: {source_file_path}") from e

        else:
            # 分支2：多文件匹配 → 校验所有匹配文件是否在同一父目录
            if len(matched_files) > 1:
                raise ValueError(
                    f"多文件匹配时，匹配文件分布在多个目录（不符合同一父目录要求）：\n"
                    f"匹配目录列表: {list(matched_files.keys())}\n"
                    f"匹配文件列表: {[(k, list(v)) for k, v in matched_files.items()]}"
                )

            # 确认唯一的父目录
            parent_dir, matched_files_set = next(iter(matched_files.items()))
            # 校验：是否匹配到所有指定的文件（可选，根据需求可注释）
            # missing_files = set(file_names) - matched_files_set
            # if missing_files:
            #     raise ValueError(
            #         f"在父目录[{parent_dir}]中，未匹配到所有指定文件：\n"
            #         f"缺失文件: {missing_files}\n"
            #         f"已匹配文件: {list(matched_files_set)}"
            #     )

            # 移动该父目录下的所有文件（不含子目录）到目标目录
            print(f"多文件匹配成功，父目录: {parent_dir}，开始移动该目录下所有文件（不含子目录）")
            for file_name in os.listdir(parent_dir):
                file_path = os.path.join(parent_dir, file_name)
                # 仅处理文件，跳过子目录
                if os.path.isfile(file_path):
                    target_file_path = os.path.join(target_dir, file_name)
                    # 处理目标文件已存在的情况（覆盖）
                    if os.path.exists(target_file_path):
                        if os.path.isdir(target_file_path):
                            shutil.rmtree(target_file_path)
                        else:
                            os.remove(target_file_path)
                    # 执行移动
                    try:
                        shutil.move(file_path, target_file_path)
                        print(f"移动完成: {file_path} → {target_file_path}")
                    except Exception as e:
                        raise RuntimeError(f"移动文件失败: {file_path}") from e


# 示例使用
if __name__ == "__main__":

    # try:
    #     result = file_move().unzip_all_zip_files(app_config.Decompress)
    #     print(f"\n成功解压的文件夹路径: {result}")
    # except Exception as e:
    #     print(f"执行出错: {str(e)}")




    # 示例1：仅移动匹配到的单个文件（match_filenames仅1个元素）
    file_move().move_matched_files(
        source_root=app_config.Decompress,
        target_dir=app_config.Flomo.file_path,
        file_names=["*的笔记*"],
        single_file_move=True
    )

    file_move().move_matched_files(
        source_root=app_config.Decompress,
        target_dir=app_config.MiSmartBand5,
        file_names=["*_MiFitness_hlth_center_fitness_data.csv",
                    "*_MiFitness_user_fitness_data_records.csv",
                    "*_MiFitness_hlth_center_aggregated_fitness_data.csv"],
        single_file_move=True
    )
