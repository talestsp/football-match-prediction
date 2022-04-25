import pyspark.sql.functions as f

def shape(df):
    return df.count(), len(df.columns)

def round_cols(df, colnames, round_n=2):
    for colname in colnames:
        df = df.withColumn(colname, f.round(colname, round_n))
    return df