from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# 创建 SparkSession
spark = SparkSession.builder \
    .appName("dblp") \
    .getOrCreate()

# 要处理的文件列表
file_list = [
    "dblp-ref-0.json",
    "dblp-ref-1.json",
    "dblp-ref-2.json",
    "dblp-ref-3.json"
]

# 循环处理每个文件
for file_path in file_list:
    # 读取JSON文件
    df = spark.read.json(file_path)

    # 过滤abstract字段包含"Language: en"的记录
    filtered_df = df.filter(col("abstract").endswith("Language: en"))

    # 将过滤后的DataFrame转换为Pandas DataFrame
    filtered_pandas_df = filtered_df.toPandas()

    # 构造输出文件路径
    output_file_path = file_path.replace("ref", "en")

    # 将Pandas DataFrame保存为单个JSON文件
    filtered_pandas_df.to_json(output_file_path, orient='records', lines=True)

# 停止SparkSession
spark.stop()

