from pyspark.ml import Transformer
from src.ml_pipeline.transformers_lib import team_mood_diff
from src.ml_pipeline.transformers_lib import home_factor
from src.ml_pipeline.transformers_lib import team_history_result


class TeamMoodDiffTransformer(Transformer):
    def __init__(self, neutral_numeric_threshold, colnames="*"):
        self.neutral_numeric_threshold = neutral_numeric_threshold
        self.colnames = colnames

    def _transform(self, df):
        use_df = df.select(self.colnames)
        df_transformed = team_mood_diff.build(use_df, self.neutral_numeric_threshold)
        return df_transformed

class TeamHistoryResultTransformer(Transformer):
    def __init__(self, colnames="*"):
        self.colnames = colnames

    def _transform(self, df):
        use_df = df.select(self.colnames)
        df_transformed = team_history_result.build(use_df)
        return df_transformed

class HomeFactorTransformer(Transformer):
    def __init__(self, colnames="*"):
        self.colnames = colnames

    def _transform(self, df):
        use_df = df.select(self.colnames)
        df_transformed = home_factor.build(use_df)
        return df_transformed