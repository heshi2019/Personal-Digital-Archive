import requests
import json
import os
import time  # 导入时间模块用于计时


class OllamaClient:
    def __init__(self, model="gemma3:4b", base_url="http://localhost:11434"):
        """
        初始化 Ollama 客户端
        :param model: 模型名称 (例如 llama3, qwen2, mistral)
        :param base_url: Ollama 的 API 地址
        """
        self.model = model
        self.url = f"{base_url}/api/generate"

    def read_markdown(self, file_path):
        """读取本地 Markdown 文件"""
        if not os.path.exists(file_path):
            return f"错误：文件 {file_path} 不存在。"

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            return f"读取文件时出错: {str(e)}"

    def chat_with_file(self, prompt, file_path=None):

        if file_path:
            """
            读取文件内容并连同用户提问发送给 Ollama
            """
            file_content = self.read_markdown(file_path)

            # 构建增强后的 Prompt
            full_prompt = (
                f"以下是一份 Markdown 文件的内容：\n\n"
                f"---START---\n{file_content}\n---END---\n\n"
                f"基于以上内容，请回答以下问题：\n{prompt}"
            )
        else:
            full_prompt = (
                f"""角色：你是一个人工智能助手，帮助我训练深度思维。
                输入：关键词、主题或概念。
                过程：
                - 使用深度和广度的标准评估关键词、主题或概念，提供高质量、有价值的问题，探索人类认知、情感和行为的各个方面。
                - 先问一些从简单到复杂的问题，然后逐步深入，帮助我进行深入探索。
                - 提供有助于总结和回顾思考的问题，以便更全面、更深入、更灵活地理解。
                最后，请您对这个关键词、主题或概念发表您的意见和理解。
                输出：
                - 由简入繁的问题：用于帮助我逐步深入探索。
                - 更深入的问题：用于深入探讨关键词、主题或概念的各个方面。
                - 总结和复习时可参考的问题：用于帮助我形成更全面、更深入、更灵活的理解。
                - 您对该关键词、主题或概念的理解和见解。所有对话和指示均需以中文提供。
                我的第一句话是：{prompt}
                """
            )

        return self._send_request(full_prompt)

    def _send_request(self, prompt):
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False
        }

        try:
            start_time = time.time()  # 记录开始时间
            response = requests.post(self.url, json=payload)
            end_time = time.time()  # 记录结束时间
            
            # 计算并打印耗时
            elapsed_time = end_time - start_time
            print(f"大模型处理耗时: {elapsed_time:.2f} 秒")
            
            # 如果报错，打印具体的服务器返回信息
            if response.status_code != 200:
                print(f"服务器返回错误内容: {response.text}")

            response.raise_for_status()
            return response.json().get("response", "未收到有效回复")
        except requests.exceptions.HTTPError as e:
            return f"HTTP 错误: {e}"
        except requests.exceptions.ConnectionError:
            return "连接失败，请检查 Ollama 是否已启动"
        except Exception as e:
            return f"发生错误: {str(e)}"


# ==========================================
# 快速测试部分
# ==========================================

"""
NAME                             ID              SIZE      MODIFIED
qwen3-vl:8b                      901cae732162    6.1 GB    13 minutes ago
deepseek-r1:8b                   6995872bfe4c    5.2 GB    25 minutes ago
qwen3-vl:4b                      1343d82ebee3    3.3 GB    37 minutes ago
qwen3:8b                         500a1f067a9f    5.2 GB    40 minutes ago
gemini-3-flash-preview:latest    ebade0d31690    -         31 hours ago
gemma3:4b                        a2af6cc3eb7f    3.3 GB    4 months ago
gpt-oss:20b                      f2b8351c629c    13 GB     4 months ago
"""
if __name__ == "__main__":
    # 1. 初始化客户端（请确保你本地已下载了 llama3 或修改为你的模型名）
    client = OllamaClient(model="gpt-oss:20b")

    # 2. 准备一个临时测试用的 Markdown 文件
    test_file = r""

    print(f"--- 正在读取文件 {test_file} 并向 Ollama 提问,模型为 {client.model} ---")

    # 3. 提问
    question = "总结下文档中的主要内容"
    
    total_start_time = time.time()  # 记录总开始时间
    result = client.chat_with_file(question, test_file)
    total_end_time = time.time()  # 记录总结束时间
    
    # 计算并打印总耗时
    total_elapsed_time = total_end_time - total_start_time
    print(f"总处理耗时: {total_elapsed_time:.2f} 秒")

    # question = "解释 HTTP 和 HTTPS 的核心区别，用通俗的话讲清楚"
    # result = client.chat_with_file(question)

    # 4. 输出结果
    print("\nOllama 的回答：")
    print("-" * 20)
    print(result)
    print("-" * 20)
