import pyspark.sql.functions as f
from pyspark.sql.window import Window
from src.utils import dflib

def freq(df, colname, round_n=None):
    freq = df.groupBy(colname).agg(f.count("*").alias("Absolute"))
    freq = freq.withColumn("Relative", f.col("Absolute") / df.count())

    if round_n:
        freq = freq.withColumn("Relative", f.round('Relative', round_n))
        
    return freq

def groupby_freq(df, groupby_cols, freq_on_col, round_n=None):
    if not isinstance(groupby_cols, list):
        groupby_cols = [groupby_cols]

    groups = df.groupBy(groupby_cols + [freq_on_col]).agg(f.count("*").alias("Absolute"))
    partial_count = groups.groupBy(groupby_cols).agg(f.sum("Absolute").alias("partial_count"))

    result_df = groups.fillna("NULL").join(partial_count.fillna("NULL"), on=groupby_cols, how="inner") \
                                     .withColumn("Relative", f.col("Absolute") / f.col("partial_count")) \
                                     .drop("partial_count")

    if round_n:
        result_df = result_df.withColumn("Relative", f.round('Relative', round_n))

    return result_df.sort(groupby_cols + [freq_on_col])

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


def row_mean(df, colnames, row_mean_colname, index_colname):
    sum_df = df.select([index_colname] + colnames) \
        .withColumn('sum',
                    sum([f.coalesce(f.col(colname), f.lit(0)) for colname in colnames])) \
        .select([index_colname, "sum"])

    count_not_nulls_df = df.select([index_colname] + colnames) \
        .withColumn('count_not_nulls', sum(df[colname].isNotNull().cast('float') for colname in colnames)) \
        .select([index_colname, 'count_not_nulls'])

    mean_df = sum_df.join(count_not_nulls_df, on=index_colname, how="inner")

    mean_df = mean_df.withColumn(row_mean_colname, f.col('sum') / f.col('count_not_nulls')).select(
        [index_colname, row_mean_colname])
    return mean_df

def describe(df, colname, round_n=None):
    describe_df = df.agg(f.min(df[colname]).alias("min"),
                      f.percentile_approx(df[colname], 0.25).alias("q_25"),
                      f.percentile_approx(df[colname], 0.50).alias("median"),
                      f.percentile_approx(df[colname], 0.75).alias("q_75"),
                      f.max(df[colname]).alias("max"),
                      f.mean(df[colname]).alias("mean"))

    if round_n:
        describe_df = dflib.round_cols(describe_df, describe_df.columns, round_n=2)

    return describe_df.withColumn("column", f.lit(colname))

def describe_cols(df, colnames, round_n=None):
    describe_cols_df = describe(df, colnames[0], round_n=round_n)

    for colname in colnames[1:]:
        describe_cols_df = describe_cols_df.union(describe(df, colname, round_n=round_n))

    return describe_cols_df

def team_history_result(df, which, team_goals_cols, team_opponent_goals_cols):

    for i in range(1, 11, 1):
        team_goals_colname = team_goals_cols[i-1]
        team_opponent_goals_colname = team_opponent_goals_cols[i-1]

        team_result_history = f"{which}_result_history_{i}"
        print(team_goals_colname)
        df = df.withColumn(
             team_result_history,
             f.col(team_goals_colname) - f.col(team_opponent_goals_colname)).withColumn(
                 team_result_history, f.when(f.col(team_result_history) > 0, "WON").otherwise(f.when(f.col(team_result_history) < 0, "LOST").otherwise(f.lit("DREW"))))
    return df