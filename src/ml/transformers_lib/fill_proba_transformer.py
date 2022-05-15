import pyspark.sql.functions as f


def join_uniform_proba(df, proba_dict):
    for key in proba_dict.keys():
        df = df.withColumn(key, f.lit(proba_dict[key]))

    return df


def join_global_frequency(df, proba_dict):
    for key in proba_dict.keys():
        df = df.withColumn(key, f.lit(proba_dict[key]))

    return df


def join_league_frequency(df, proba_df):
    df = df.join(proba_df, how="left", on="league_id")

    return df


def build(df, proba, strategy):

    if strategy == "uniform_proba":
        df = join_uniform_proba(df, proba)
    elif strategy == "global_frequency":
        df = join_global_frequency(df, proba)
    elif strategy == "league_frequency":
        df = join_league_frequency(df, proba)

    return df