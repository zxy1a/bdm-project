import pandas as pd

# 要合并的文件列表
file_list = [
    "dblp-en-0.json",
    "dblp-en-1.json",
    "dblp-en-2.json",
    "dblp-en-3.json"
]

# 创建一个空的Pandas DataFrame用于合并
combined_pandas_df = pd.DataFrame()

# 循环读取每个文件并合并
for file_path in file_list:
    # 读取JSON文件到Pandas DataFrame
    df = pd.read_json(file_path, orient='records', lines=True)

    # 合并到总的DataFrame
    combined_pandas_df = pd.concat([combined_pandas_df, df], ignore_index=True)

# 构造最终输出文件路径
output_file_path = "combined_data_en.json"

# 将合并后的Pandas DataFrame保存为单个JSON文件
combined_pandas_df.to_json(output_file_path, orient='records', lines=True)
