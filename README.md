# take_note
takenotes
######################
from pyspark.sql.functions import col
from pyspark import StorageLevel

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "100")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

df_filtered = (
    df_transformedlast
    .filter(col("Year") >= 2024)
    .persist(StorageLevel.DISK_ONLY)
)

total_count = df_filtered.count()

year_data = (
    df_filtered
    .groupBy("Year")
    .count()
    .orderBy("Year")
    .collect()
)

years = [(row["Year"], row["count"]) for row in year_data]

for idx, (year, count) in enumerate(years):
    if count > 50_000_000:
        num_parts = 500
        max_records = 50000
    elif count > 20_000_000:
        num_parts = 300
        max_records = 80000
    elif count > 5_000_000:
        num_parts = 100
        max_records = 100000
    else:
        num_parts = 50
        max_records = 150000
    
    df_year = df_filtered.filter(col("Year") == year)
    
    (
        df_year
        .repartition(num_parts)
        .write
        .format("delta")
        .mode("overwrite" if idx == 0 else "append")
        .option("overwriteSchema", "true" if idx == 0 else "false")
        .option("replaceWhere", f"Year = {year}")
        .option("maxRecordsPerFile", max_records)
        .option("optimizeWrite", "true")
        .partitionBy("Year")
        .saveAsTable("lh_aeon_reportingdb_silver.purchasing_detail")
    )

df_filtered.unpersist()
#########################3
