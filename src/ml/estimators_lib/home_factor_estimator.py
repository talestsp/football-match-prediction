from src.utils import stats
import pyspark.sql.functions as f


def _role_factor(df, which_role):
    target_freq = stats.groupby_freq(df, groupby_cols="league_id", freq_on_col="target", round_n=4).drop(*["target", "Absolute"])
    role_freq = target_freq.filter(f.col("target") == which_role)
    role_freq = role_freq.withColumn(f"{which_role}_factor", f.col("Relative")).drop(*["Relative"])
    return role_freq

def build(df, n_matches_min):
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

    n_matches = df.groupBy("league_id").agg(f.count("*").alias("n_matches"))
    home_factor = _role_factor(df, which_role="home")
    draw_factor = _role_factor(df, which_role="draw")

    role_factors = n_matches.join(home_factor, on="league_id", how="left") \
                            .join(draw_factor, on="league_id", how="left")

    return role_factors.filter(f.col("n_matches") >= n_matches_min)