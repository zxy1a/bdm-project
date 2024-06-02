# 导入必要的库
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when, desc, length

# 创建 SparkSession 并配置资源
spark = SparkSession.builder \
    .appName("dblp") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 读取JSON 文件
data_path = "dblp-ref-1.json"
df = spark.read.json(data_path)

# 显示数据集的 schema
df.printSchema()

# 显示前几行数据
df.show(5, truncate=False)

# 查看数据的统计信息
# df.describe().show()

# 检查缺失值
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# 查看引用次数的分布
df.select("n_citation").describe().show()

# 按年份查看数据分布
df.groupBy("year").count().orderBy(desc("count")).show()

# 查看 venue 的分布情况
df.groupBy("venue").count().orderBy(desc("count")).show(10)

# 检查标题和摘要的长度分布
df.withColumn("title_length", length(col("title"))) \
  .withColumn("abstract_length", length(col("abstract"))) \
  .select("title_length", "abstract_length") \
  .describe().show()

# 查看特定列的值
df.select("title", "abstract", "authors", "n_citation", "references", "venue", "year", "id").show(5, truncate=False)

spark.stop()
