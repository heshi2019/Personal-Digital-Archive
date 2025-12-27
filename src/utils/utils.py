import fnmatch
import json
import os
import shutil
from typing import Optional



class Utils:
    # 单例实例
    _instance = None

    def __init__(self):
        pass

    # 单例模式实现
    def __new__(cls, path=None):
        if not cls._instance:
            # 创建实例
            instance = super().__new__(cls)

            cls._instance = instance
        return cls._instance

    def save(self,path,fileName,data,status):
        os.makedirs(path, exist_ok=True)
        output_path = os.path.join(path, fileName)

        with open(output_path, "w", encoding='utf-8') as f:
            if status == "json":
                f.write(data)
            else:
                f.write(json.dumps(data, indent=4, ensure_ascii=False))


    def copy_and_rename_file(
            source_dir: str,
            target_dir: str,
            match_pattern: str,
            new_full_name: str
    ) -> Optional[str]:
        """
        在指定文件夹当前层级模糊匹配单个文件，复制时直接重命名（含后缀）到目标文件夹
        （仅匹配当前层级，不递归子目录；源文件保留，仅复制到目标目录并改名）

        Args:
            source_dir: 源文件夹路径（仅匹配当前层级）
            target_dir: 目标文件夹路径（不存在则自动创建）
            match_pattern: 模糊匹配规则（支持*通配符，如"test*.txt"、"data.csv"）
            new_full_name: 新的完整文件名（含后缀，如"new_report.xlsx"、"final_data.json"）

        Returns:
            Optional[str]: 成功复制后文件的最终路径；无匹配文件/处理失败返回None

        Raises:
            FileNotFoundError: 源文件夹不存在
            ValueError: 匹配规则/新文件名为空
        """
        # 1. 基础参数校验
        if not os.path.exists(source_dir):
            raise FileNotFoundError(f"源文件夹不存在: {source_dir}")
        if not match_pattern:
            raise ValueError("模糊匹配规则不能为空")
        if not new_full_name:
            raise ValueError("新文件名（含后缀）不能为空")

        # 2. 确保目标文件夹存在
        os.makedirs(target_dir, exist_ok=True)

        # 3. 在源文件夹当前层级模糊匹配第一个符合规则的文件
        matched_file = None
        for file_name in os.listdir(source_dir):
            file_path = os.path.join(source_dir, file_name)
            # 仅匹配文件（跳过文件夹）+ 通配符模糊匹配
            if os.path.isfile(file_path) and fnmatch.fnmatch(file_name, match_pattern):
                matched_file = file_path
                break  # 只取第一个匹配的文件

        if not matched_file:
            print(f"❌ 未在 [{source_dir}] 中匹配到符合规则 [{match_pattern}] 的文件")
            return None

        # 4. 构造目标文件最终路径（复制时直接改名）
        final_target_path = os.path.join(target_dir, new_full_name)

        # 处理目标路径已有同名文件/文件夹（避免覆盖，先删除）
        if os.path.exists(final_target_path):
            if os.path.isdir(final_target_path):
                shutil.rmtree(final_target_path)  # 删除同名文件夹
            else:
                os.remove(final_target_path)  # 删除同名文件

        # 5. 复制文件并直接改名（核心逻辑：一步完成复制+改名）
        try:
            shutil.copy2(matched_file, final_target_path)  # copy2保留文件元数据（比copy更友好）
            print(f"✅ 操作成功！\n源文件: {matched_file}\n目标文件: {final_target_path}")
            return final_target_path
        except Exception as e:
            print(f"❌ 复制失败: {matched_file} → {final_target_path}，错误: {str(e)}")
            return None




# 创建全局单例实例
app_Utils = Utils()



