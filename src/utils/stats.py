import pyspark.sql.functions as f
from pyspark.sql.window import Window

def freq(df, colname, round_n=None):
    freq = df.groupBy(colname).agg(f.count("*").alias("Absolute"))
    freq = freq.withColumn("Relative", f.col("Absolute") / df.count())

    if round_n:
        freq = freq.withColumn("Relative", f.round('Relative', round_n))
        
    return freq

def cum_sum(df, colname, order_by):
    index = df.columns
    index.remove(colname)

    windowSpec = Window.orderBy(order_by)
    total = df.agg(f.sum(colname).alias("total")).collect()[0].total

    cumsum_abs_colname = f"cumsum_abs({colname})"
    cumsum_rel_colname = f"cumsum_rel({colname})"

    cumsum_abs = df.withColumn(cumsum_abs_colname, f.sum("count").over(windowSpec))
    cumsum_rel = df.withColumn(cumsum_rel_colname, f.sum("count").over(windowSpec) / total).select(
        index + [cumsum_rel_colname])

    cumsum = cumsum_abs.join(cumsum_rel, on=index, how="outer")

    return cumsum.select(df.columns + [cumsum_abs_colname, cumsum_rel_colname])