import pyspark.sql.functions as f

def freq(df, colname, round_n=None):
    freq = df.groupBy(colname).agg(f.count("*").alias("Absolute"))
    freq = freq.withColumn("Relative", f.col("Absolute") / df.count())

    if round_n:
        freq = freq.withColumn("Relative", f.round('Relative', round_n))
        
    return freq