from pyspark.ml import Transformer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.util import MLWritable, MLReadable
from pyspark.sql import functions as f
from src.ml.transformers_lib import team_history_result, team_mood_diff
from src.ml.transformers_lib import fill_proba_transformer
from src.utils import dflib, stats
from pyspark.sql.types import DoubleType

# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.Transformer.html#pyspark.ml.Transformer.transform
# https://stackoverflow.com/questions/49734374/pyspark-ml-pipelines-are-custom-transformers-necessary-for-basic-preprocessing
# https://www.oreilly.com/content/extend-spark-ml-for-your-own-modeltransformer-types/
# https://www.youtube.com/watch?v=iO4ebMzj7t8&ab_channel=ManningPublications

class TeamMoodDiffTransformer(Transformer, MLReadable, MLWritable):
    def __init__(self, neutral_numeric_threshold=0.5, colnames="*"):
        super().__init__()
        self.neutral_numeric_threshold = neutral_numeric_threshold
        self.colnames = colnames

    def _transform(self, df):
        print("TeamMoodDiffTransformer")
        use_df = df.select(self.colnames)
        df_transformed = team_mood_diff.build(use_df, self.neutral_numeric_threshold)
        return df_transformed

    def get_params(self):
        return {"neutral_numeric_threshold": self.neutral_numeric_threshold,
                "colnames": self.colnames}

class TeamHistoryResultTransformer(Transformer, MLReadable, MLWritable):
    def __init__(self, colnames="*"):
        super().__init__()
        self.colnames = colnames

    def _transform(self, df):
        print("TeamHistoryResultTransformer")
        use_df = df.select(self.colnames)
        df_transformed = team_history_result.build(use_df)
        return df_transformed

    def get_params(self):
        return {"colnames": self.colnames}

class HomeFactorTransformer(Transformer, MLReadable, MLWritable):
    def __init__(self, home_factor, draw_factor, n_matches, colnames="*"):
        super().__init__()
        self.home_factor = home_factor
        self.draw_factor = draw_factor
        self.n_matches = n_matches
        self.colnames = colnames

    def _transform(self, df):
        print("HomeFactorTransformer")

        df_transformed = df.join(self.home_factor, on="league_id", how="left") \
                           .join(self.draw_factor, on="league_id", how="left") \
                           .join(self.n_matches, on="league_id", how="left")

        return df_transformed

    def get_params(self):
        return {}


class SelectColumnsTransformer(Transformer, MLReadable, MLWritable):
    KEEP_COLNAMES = ['id', 'target', 'home_team_name', 'away_team_name', 'match_date', 'league_name', 'league_id']

    def __init__(self, subset_colnames="*", keep_colnames=KEEP_COLNAMES):
        super().__init__()
        self.subset_colnames = subset_colnames
        self.keep_colnames = keep_colnames

    def _transform(self, df, numer_features=None):
        print("SelectColumnsTransformer")

        if not numer_features:
            numer_features, categ_features = dflib.split_coltypes(df, colnames=self.subset_colnames)

        select_colnames = self.keep_colnames + numer_features

        if not "target" in df.columns:
            select_colnames.remove("target")

        return df

    def get_params(self):
        return {"keep_colnames": self.keep_colnames,
                "subset_colnames": self.subset_colnames}


class DropNaTransformer(Transformer, MLReadable, MLWritable):
    def __init__(self):
        super().__init__()
        pass

    def _drop_report(self, len_df, len_df_dropna):
        n_dropped_rows = len_df - len_df_dropna
        percent_dropped_rows = 100 * n_dropped_rows / len_df

        print(f'{len_df} before')
        print(f'{len_df_dropna} after')

        print(f'{n_dropped_rows} rows dropped ({percent_dropped_rows:.2f}%)')

    def _transform(self, df, drop_report=False):
        print("DropNaTransformer")
        df_dropped = df.dropna(how="any")

        if drop_report:
            len_df = df.count()
            len_df_dropna = df_dropped.count()
            self._drop_report(len_df, len_df_dropna)

        return df_dropped

    def get_params(self):
        return {}


class UndersamplingTransformer(Transformer, MLReadable, MLWritable):
    def __init__(self, target_colname):
        super().__init__()
        self.target_colname = target_colname

    def _transform(self, df):
        print("UndersamplingTransformer")
        return dflib.df_undersampling(df, self.target_colname)

    def get_params(self):
        return {}

class FillProbaTransformer(Transformer, MLReadable, MLWritable):

    def __init__(self, strategy, probas, labels, proba_vector_col, strategy_b_transformer=None):
        super().__init__()
        self.strategy = strategy
        self.probas = probas
        self.labels = labels
        self.proba_vector_col = proba_vector_col
        self.strategy_b_transformer = strategy_b_transformer

    def _transform(self, df):
        df_not_null = df.dropna(how="any", subset=[self.proba_vector_col])
        df_preds_null = dflib.filter_any_null(df=df, subset=[self.proba_vector_col])

        if df_preds_null.count() == 0:
            return df

        df_preds_null = fill_proba_transformer.build(df=df_preds_null,
                                                     proba=self.probas[self.strategy],
                                                     strategy=self.strategy)

        if not self.strategy_b_transformer is None:
            df_preds_null = self.strategy_b_transformer.transform(df_preds_null)

        df_preds_null = VectorAssembler(inputCols=self.labels,
                                        outputCol=self.proba_vector_col) \
                            .transform(df_preds_null.drop(*[self.proba_vector_col]))

        df = df_not_null.union(df_preds_null.select(df_not_null.columns))

        return df

    def get_params(self):
        return {"strategy": self.strategy}
