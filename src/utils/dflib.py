import pyspark.sql.functions as f
from pyspark.sql.types import StringType, DoubleType, IntegerType, LongType, BooleanType
from functools import reduce
from pyspark.sql import DataFrame
import pyspark.ml.functions as psf

def shape(df):
    return df.count(), len(df.columns)

def round_cols(df, colnames, round_n=2):
    if not isinstance(colnames, list):
        colnames = [colnames]

    for colname in colnames:
        df = df.withColumn(colname, f.round(colname, round_n))
    return df

def df_to_dict(df, colnames):
    lists_dict = {colname: [] for colname in colnames}

    for row in df.select(colnames).collect():
        for colname in colnames:
            lists_dict[colname].append(row[colname])

    return lists_dict

def sample(df, n=1):
    return df.orderBy(f.rand()).limit(n)

def rename_cols(df, rename_map):
    for key in rename_map.keys():
        df = df.withColumnRenamed(key, rename_map[key])
    return df

def append_suffix_cols(df, colnames, suffix):
    rename_map = {colname : colname + suffix for colname in colnames}
    return rename_cols(df, rename_map)

def split_coltypes(df, colnames="*", discard_colnames=[]):
    if colnames == "*":
        colnames = df.columns

    colnames = list(set(colnames) - set(discard_colnames))
    use_df = df.select(colnames)

    numer_features = [f.name for f in use_df.schema.fields if isinstance(f.dataType, DoubleType) | isinstance(f.dataType, LongType) | isinstance(f.dataType, IntegerType) | isinstance(f.dataType, BooleanType)]
    categ_features = [f.name for f in use_df.schema.fields if isinstance(f.dataType, StringType)]

    return numer_features, categ_features

def df_undersampling(df, target_colname):
    target_count = df.groupBy(target_colname).agg(f.count("*").alias("count"))
    min_classes_len = target_count.select(f.min("count").alias("min")).collect()[0].min

    target_values = [target_value[target_colname] for target_value in df.select(target_colname).distinct().collect()]

    class_df_list = []

    for target_value in target_values:
        class_df = df.filter(f.col(target_colname) == target_value)
        class_df_resampled = sample(class_df, n=min_classes_len)
        class_df_list.append(class_df_resampled)

    df_resampled = reduce(DataFrame.unionAll, class_df_list)

    return df_resampled

def filter_any_null(df, subset=None):
    if subset is None:
        subset = df.columns

    cols = [f.col(c) for c in subset]
    filter_expr = reduce(lambda a, b: a | b.isNull(), cols[1:], cols[0].isNull())
    return df.filter(filter_expr)

def dense_vector_to_columns(df, dense_vector_colname, new_colnames):
    return df.withColumn("xs", psf.vector_to_array(dense_vector_colname)).select(
        ["*"] + [f.col("xs")[i].alias(new_colnames[i]) for i in range(len(new_colnames))]).drop(*["xs"])

def proba_to_predicted_target(df, target_colname, proba_colnames):
    cond = "f.when" + ".when".join(
        ["(f.col('" + c + "') == f.col('" + target_colname + "'), f.lit('" + c + "'))" for c in
         proba_colnames])

    df = df.withColumn(target_colname,
                       f.greatest(*proba_colnames)).withColumn(target_colname, eval(cond))

    return df



