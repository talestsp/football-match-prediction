import pyspark.sql.functions as f
from src.utils import stats, dflib

def _role_factor(df, which_role):
    target_freq = stats.groupby_freq(df, groupby_cols="league_id", freq_on_col="target", round_n=4).drop(*["target", "Absolute"])
    role_freq = target_freq.filter(f.col("target") == which_role)
    role_freq = role_freq.withColumn(f"{which_role}_factor", f.col("Relative")).drop(*["Relative"])

    return role_freq

def build(df):
    home_factor = _role_factor(df, which_role="home")
    draw_factor = _role_factor(df, which_role="draw")

    df = df.join(home_factor, on="league_id", how="left") \
           .join(draw_factor, on="league_id", how="left")

    return df
