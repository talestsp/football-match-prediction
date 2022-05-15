import pyspark.sql.functions as f
from pivot.utils import stats

def _team_history_mood(df):
    for which_team in ["home", "away"]:

        for i in range(1, 11):
            team_history_mood = f"{which_team}_history_mood_{i}"
            team_history_rating = f"{which_team}_team_history_rating_{i}"
            opponent_history_rating = f"{which_team}_team_history_opponent_rating_{i}"

            df = df.withColumn(team_history_mood,
                                       df[team_history_rating] - \
                                       df[opponent_history_rating])

    return df

def _team_history_mood_mean(df):
    home_history_mood_colnames = [f"home_history_mood_{i}" for i in range(1, 11)]
    home_match_mood_numeric = stats.row_mean(df=df, colnames=home_history_mood_colnames,
                                             row_mean_colname="home_history_mood_mean",
                                             index_colname="id")

    away_history_mood_colnames = [f"away_history_mood_{i}" for i in range(1, 11)]
    away_match_mood_numeric = stats.row_mean(df=df, colnames=away_history_mood_colnames,
                                             row_mean_colname="away_history_mood_mean",
                                             index_colname="id")

    df = df.join(home_match_mood_numeric, on="id", how="left")
    df = df.join(away_match_mood_numeric, on="id", how="left")

    return df

def _team_mood(df, neutral_numeric_threshold):
    df = df.withColumn("home_mood_diff", f.col("home_history_mood_mean") - f.col("away_history_mood_mean"))
    df = df.withColumn("away_mood_diff", f.col("away_history_mood_mean") - f.col("home_history_mood_mean"))

    df = df.withColumn("home_mood",
                               f.when(f.abs(df["home_mood_diff"]) < neutral_numeric_threshold, "NEUTRAL") \
                               .otherwise(f.when(df["home_mood_diff"] < 0, "PESSIMISTIC") \
                                          .otherwise(f.when(df["home_mood_diff"] > 0, "OPTIMISTIC"))))

    df = df.withColumn("away_mood",
                               f.when(f.abs(df["away_mood_diff"]) < neutral_numeric_threshold, "NEUTRAL") \
                               .otherwise(f.when(df["away_mood_diff"] < 0, "PESSIMISTIC") \
                                          .otherwise(f.when(df["away_mood_diff"] > 0, "OPTIMISTIC"))))

    return df

def build(df, neutral_numeric_threshold=0.25):
    '''
    Builds the following features: `home_mood_diff`, `away_mood_diff`, `home_history_mood_mean` and `away_history_mood_mean`.

    For `home_history_mood_mean` and `away_history_mood_mean` is calculated, for each previous match, the difference
    between the team and its previous opponents ratings. Then the mean is calculated over these differences

    For `home_mood_diff` and `away_mood_diff` the difference between the team and its current opponent
    team_history_mood_mean's is calculated.

    :param df:
    :param neutral_numeric_threshold:
    :return:
    '''
    df = _team_history_mood(df)
    df = _team_history_mood_mean(df).drop(*[
        'home_history_mood_1', 'home_history_mood_2', 'home_history_mood_3', 'home_history_mood_4',
        'home_history_mood_5', 'home_history_mood_6', 'home_history_mood_7', 'home_history_mood_8',
        'home_history_mood_9', 'home_history_mood_10', 'away_history_mood_1', 'away_history_mood_2',
        'away_history_mood_3', 'away_history_mood_4', 'away_history_mood_5', 'away_history_mood_6',
        'away_history_mood_7', 'away_history_mood_8', 'away_history_mood_9', 'away_history_mood_10'])
    df = _team_mood(df, neutral_numeric_threshold)

    return df