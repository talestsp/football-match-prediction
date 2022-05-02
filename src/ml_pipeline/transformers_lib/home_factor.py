import pyspark.sql.functions as f
from src.utils import stats

def _role_factor(df, which_role):
    target_freq = stats.groupby_freq(df, groupby_cols="league_id", freq_on_col="target", round_n=4).drop(*["target", "Absolute"])
    role_freq = target_freq.filter(f.col("target") == which_role)
    role_freq = role_freq.withColumn(f"{which_role}_factor", f.col("Relative")).drop(*["Relative"])
    return role_freq

def build(df, already_calculated_features_df=None):
    '''
    Builds home_factor and draw_factor features.
    * home_factor represents the relative frequency of victories for home playing teams for each league.
    / draw_factor represents the relative frequency of draw result  for each league.
    In case this transformer is requeste to build for the test dataset it needs to receive these features from training
    dataset
    :param df:
    :param already_calculated_features_df:
    :return:
    '''
    if already_calculated_features_df:
        df = df.join(already_calculated_features_df, on="league_id", how="left")

    else:
        home_factor = _role_factor(df, which_role="home")
        draw_factor = _role_factor(df, which_role="draw")
        n_matches = df.groupBy("league_id").agg(f.count("*").alias("n_matches"))

        df = df.join(home_factor, on="league_id", how="left") \
               .join(draw_factor, on="league_id", how="left") \
               .join(n_matches, on="league_id", how="left")

    return df
