import pyspark.sql.functions as f

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


