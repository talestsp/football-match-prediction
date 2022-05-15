import pyspark.sql.functions as f
from src.dao import columns
from src.utils import stats

def _add_team_history_result(df, which, team_goals_cols, team_opponent_goals_cols):
    for i in range(1, 11, 1):
        team_goals_colname = team_goals_cols[i - 1]
        team_opponent_goals_colname = team_opponent_goals_cols[i - 1]

        team_result_history = f"{which}_result_history_{i}"

        df = df.withColumn(
            team_result_history,
            f.col(team_goals_colname) - f.col(team_opponent_goals_colname)).withColumn(
            team_result_history, f.when(f.col(team_result_history) > 0, 1).otherwise(
                f.when(f.col(team_result_history) < 0, -1).otherwise(f.lit(0))))
    return df

def _team_history_result_mean(df):
    home_result_history_colnames = [f"home_result_history_{i}" for i in range(1, 11, 1)]
    away_result_history_colnames = [f"away_result_history_{i}" for i in range(1, 11, 1)]

    home_result_history_mean = stats.row_mean(df, home_result_history_colnames, "home_result_history_mean",
                                              index_colname="id")
    away_result_history_mean = stats.row_mean(df, away_result_history_colnames, "away_result_history_mean",
                                              index_colname="id")

    df = df.join(home_result_history_mean, on="id", how="left")\
           .join(away_result_history_mean, on="id", how="left")

    return df

def build(df):
    '''
    Builds `home_result_history_mean` and `away_result_history_mean` which represents the team previous 10 results.
    Each one of these results (victory, draw or defeat) is replaced by 1, 0 and -1, respectively.
    Then the mean is calculated over these values for both home and away teams in the match.

    :param df:
    :return:
    '''
    df = _add_team_history_result(df, which="home",
                                  team_goals_cols=columns.home_team_history_goal_cols,
                                  team_opponent_goals_cols=columns.home_team_history_opponent_goal_cols)

    df = _add_team_history_result(df, which="away",
                                  team_goals_cols=columns.away_team_history_goal_cols,
                                  team_opponent_goals_cols=columns.away_team_history_opponent_goal_cols)

    df = _team_history_result_mean(df)

    df = df.drop(*['away_result_history_1', 'away_result_history_2', 'away_result_history_3',
                   'away_result_history_4', 'away_result_history_5', 'away_result_history_6',
                   'away_result_history_7', 'away_result_history_8', 'away_result_history_9',
                   'away_result_history_10'])

    df = df.drop(*['home_result_history_1', 'home_result_history_2', 'home_result_history_3',
              'home_result_history_4', 'home_result_history_5', 'home_result_history_6',
              'home_result_history_7', 'home_result_history_8',  'home_result_history_9',
              'home_result_history_10'])

    return df

