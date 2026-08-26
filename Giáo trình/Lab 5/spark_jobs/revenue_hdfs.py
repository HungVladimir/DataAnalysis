"""So sánh CÔNG BẰNG: chạy Spark BÊN TRONG cụm Docker, đọc thẳng từ HDFS.

Cùng dữ liệu, cùng phép tính, cùng phần cứng và cùng lớp giả lập với job
MapReduce của Buổi 04 — nên chênh lệch thời gian đo được là chênh lệch THẬT
giữa xử lý trong bộ nhớ và xử lý qua đĩa.

Chạy:
    docker cp lab5/spark_jobs/revenue_hdfs.py bigdata-spark-master:/tmp/
    docker exec bigdata-spark-master /opt/bitnami/spark/bin/spark-submit \
        --master "local[*]" /tmp/revenue_hdfs.py
"""
import time

from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://hadoop-namenode:9000/user/bigdata/lab4/input"

t_khoi_dong = time.perf_counter()
spark = (SparkSession.builder
         .appName("Lab5_RevenueOnHDFS")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
t_khoi_dong = time.perf_counter() - t_khoi_dong

# --- Bài toán 1: doanh thu theo ngành hàng (đối chiếu D7 của Buổi 04) --------
t0 = time.perf_counter()
df = spark.read.csv(f"{HDFS}/transactions.csv", header=True, inferSchema=True)
ket_qua = (df.withColumn("revenue", F.col("quantity") * F.col("unit_price"))
             .groupBy("category")
             .agg(F.count("*").alias("so_giao_dich"),
                  F.sum("revenue").alias("doanh_thu"))
             .orderBy(F.desc("doanh_thu"))
             .collect())
t_revenue = time.perf_counter() - t0

# --- Bài toán 2: WordCount (đối chiếu D6 của Buổi 04) ------------------------
DAU_CAU  = str.maketrans({c: " " for c in '.,;:!?"()[]'})
HA_ASCII = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")

t0 = time.perf_counter()
wc = (spark.sparkContext.textFile(f"{HDFS}/wordcount_corpus.txt")
      .flatMap(lambda d: d.translate(DAU_CAU).translate(HA_ASCII).split())
      .map(lambda tu: (tu, 1))
      .reduceByKey(lambda a, b: a + b))
so_tu_khac_nhau = wc.count()
top = wc.takeOrdered(5, key=lambda x: (-x[1], x[0]))
t_wordcount = time.perf_counter() - t0

print("=" * 64)
print("SPARK CHẠY TRONG CỤM DOCKER, ĐỌC THẲNG TỪ HDFS")
print("=" * 64)
print(f"Khởi tạo SparkSession : {t_khoi_dong:6.2f} s")
print(f"Doanh thu theo ngành  : {t_revenue:6.2f} s   (Buổi 04 mất ~30 s)")
print(f"WordCount             : {t_wordcount:6.2f} s   (Buổi 04 mất ~30 s)")
print("-" * 64)
for r in ket_qua:
    print(f"  {r['category']:<12}{r['so_giao_dich']:>9,}{r['doanh_thu']:>18,}")
print("-" * 64)
print(f"  Số từ khác nhau: {so_tu_khac_nhau}   (kỳ vọng 373)")
print(f"  Top 5: {top}")
print("=" * 64)

spark.stop()
