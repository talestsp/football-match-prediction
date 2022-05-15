from pivot.utils import stats, dflib


def _uniform_proba():
    return {"home": 0.3333334, "away": 0.3333333, "draw": 0.3333333}


def _global_frequency(df):
    freq = stats.freq(df, colname="target").drop(*["Absolute"])
    freq_dicts = dflib.df_to_dict(freq, ["target", "Relative"])

    proba = {}
    for i in range(len(freq_dicts["target"])):
        key = freq_dicts["target"][i]
        values = freq_dicts["Relative"][i]
        proba[key] = values

    return proba


def _league_frequency(df):
    freq = stats.groupby_freq(df, groupby_cols=["league_id"], freq_on_col="target").drop(*["Absolute"])
    proba = freq.groupBy("league_id").pivot("target").mean("Relative")

    return proba


def build(df):
    uniform_proba = _uniform_proba()
    global_frequency = _global_frequency(df)
    league_frequency = _league_frequency(df)

    return {"uniform_proba": uniform_proba,
            "global_frequency": global_frequency,
            "league_frequency": league_frequency}
