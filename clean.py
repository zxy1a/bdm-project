# 导入必要的库
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, concat_ws
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover

# 创建 SparkSession 并配置资源
spark = SparkSession.builder \
    .appName("Text Cleaning and Clustering") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# 读取JSON 文件
data_path = "combined_data_en.json"
df = spark.read.json(data_path)

# 转换为小写并移除标点符号
df = df.withColumn("cleaned_title", lower(col("title")))
df = df.withColumn("cleaned_title", regexp_replace(col("cleaned_title"), r'[!()\-\[\]{};:\'",<>./?@#$%^&*_~]', ''))
df = df.withColumn("cleaned_abstract", lower(col("abstract")))
df = df.withColumn("cleaned_abstract", regexp_replace(col("cleaned_abstract"), r'[!()\-\[\]{};:\'",<>./?@#$%^&*_~]', ''))

# 合并标题和摘要
df = df.withColumn("combined_text", concat_ws(" ", col("cleaned_title"), col("cleaned_abstract")))

# Tokenization
regex_tokenizer = RegexTokenizer(inputCol="combined_text", outputCol="words", pattern="\\W")
df = regex_tokenizer.transform(df)

# 自定义停用词列表
custom_stopwords = ['doi', 'preprint', 'copyright', 'peer', 'reviewed', 'org', 'https', 'et', 'al', 'author',
                    'figure', 'rights', 'reserved', 'permission', 'used', 'using', 'biorxiv', 'medrxiv', 'license',
                    'fig', 'fig.', 'al.', 'Elsevier', 'PMC', 'CZI', 'www']

# 停用词移除器
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
# 将自定义停用词添加到默认的停用词列表中
stopwords_remover.setStopWords(stopwords_remover.getStopWords() + custom_stopwords)
df = stopwords_remover.transform(df)

# 选择需要保存的列
df_result = df.select("id", "title", "filtered_words")

# 将处理后的数据保存到一个新的 JSON 文件，使用 overwrite 模式
output_path = "cleaned_data.json"
df_result.write.mode("overwrite").json(output_path)


# 停止 SparkSession
spark.stop()
