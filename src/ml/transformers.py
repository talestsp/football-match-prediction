from pyspark.ml import Transformer
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.util import MLWritable, MLReadable
from pyspark.sql import functions as f
from src.ml.transformers_lib import team_history_result, team_mood_diff, fill_proba_transformer
from src.utils import dflib, stats
import datetime as datetime_

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
        use_df = df.select(self.colnames)
        df_transformed = team_history_result.build(use_df)
        return df_transformed

    def get_params(self):
        return {}

class HomeFactorTransformer(Transformer, MLReadable, MLWritable):
    def __init__(self, role_factors, n_matches_min=1, colnames="*"):
        super().__init__()
        self.role_factors = role_factors
        self.n_matches_min = n_matches_min
        self.colnames = colnames

    def _transform(self, df):
        df_transformed = df.join(self.role_factors, on="league_id", how="left")

        return df_transformed

    def get_params(self):
        return {"n_matches_min": self.n_matches_min}


class SelectColumnsTransformer(Transformer, MLReadable, MLWritable):
    KEEP_COLNAMES = ['id', 'target', 'home_team_name', 'away_team_name', 'match_date', 'league_name', 'league_id']

    def __init__(self, subset_colnames="*", keep_colnames=KEEP_COLNAMES):
        super().__init__()
        self.subset_colnames = subset_colnames
        self.keep_colnames = keep_colnames

    def _transform(self, df, numer_features=None):
        if not numer_features:
            numer_features, categ_features = dflib.split_coltypes(df, colnames=self.subset_colnames)

        select_colnames = self.keep_colnames + numer_features

        if not "target" in df.columns:
            select_colnames.remove("target")

        df = df.select(select_colnames)
        return df

    def get_params(self):
        return {"keep_colnames": self.keep_colnames,
                "subset_colnames": self.subset_colnames}


class DropNaTransformer(Transformer, MLReadable, MLWritable):
    def __init__(self, subset=None, drop_report=False):
        super().__init__()
        self.subset = subset
        self.drop_report = drop_report

    def _drop_report(self, len_df, len_df_dropna):
        n_dropped_rows = len_df - len_df_dropna
        percent_dropped_rows = 100 * n_dropped_rows / len_df

        print(f'{len_df} before')
        print(f'{len_df_dropna} after')
        print(f'{n_dropped_rows} rows dropped ({percent_dropped_rows:.2f}%)')

    def _transform(self, df):
        if self.subset is None:
            subset = df.columns
        else:
            subset = self.subset

        df_dropped = df.dropna(how="any", subset=subset)

        if self.drop_report:
            len_df = df.count()
            len_df_dropna = df_dropped.count()
            self._drop_report(len_df, len_df_dropna)

        return df_dropped

    def get_params(self):
        return {"subset": self.subset}


class ProbaVectorToPrediction(Transformer, MLReadable, MLWritable):
    def __init__(self, target_transformer, prediction_col, dense_vector_colname):
        super().__init__()
        self.target_transformer = target_transformer
        self.prediction_col = prediction_col
        self.dense_vector_colname = dense_vector_colname

    def _transform(self, df):

        df = dflib.dense_vector_to_columns(df=df,
                                           dense_vector_colname=self.dense_vector_colname,
                                           new_colnames=self.target_transformer.labels)

        df = dflib.proba_to_predicted_target(df=df,
                                             target_colname=self.prediction_col + "_str",
                                             proba_colnames=self.target_transformer.labels)

        prediction_indexer = StringIndexer(inputCol=self.prediction_col + "_str",
                                           outputCol=self.prediction_col,
                                           stringOrderType=self.target_transformer.getStringOrderType()).fit(df)

        df = prediction_indexer.transform(df)
        return df

    def get_params(self):
        return {}


class UndersamplingTransformer(Transformer, MLReadable, MLWritable):
    def __init__(self, target_colname):
        super().__init__()
        self.target_colname = target_colname

    def _transform(self, df):
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
        if self.proba_vector_col in df.columns:
            df_not_null = df.dropna(how="any", subset=[self.proba_vector_col])
            df_preds_null = dflib.filter_any_null(df=df, subset=[self.proba_vector_col])

        else:
            df_not_null = None
            df_preds_null = df

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

        if df_not_null is None:
            df = df_preds_null
        else:
            df = df_not_null.union(df_preds_null.select(df_not_null.columns))

        return df

    def get_params(self):
        return {"strategy": self.strategy}

class DateFilterTransformer(Transformer, MLReadable, MLWritable):

    def __init__(self, dt_colname, from_dt=None, to_dt=None):
        super().__init__()
        self.dt_colname = dt_colname
        self.from_dt = from_dt
        self.to_dt = to_dt

    def __get_date(self, df, which):
        if which == "max":
            dt = df.select(f.max("match_date").alias("match_date")).collect()[0].match_date
        elif which == "min":
            dt = df.select(f.min("match_date").alias("match_date")).collect()[0].match_date

        if isinstance(dt, str):
            return dt
        elif isinstance(dt, datetime_.date):
            return dt.strftime(format="%Y-%m-%d %H:%M:%S")

    def _transform(self, df):
        if self.from_dt is None:
            from_dt = self.__get_date(df, "min")
        else:
            from_dt = self.from_dt

        if self.to_dt is None:
            to_dt = self.__get_date(df, "max")
        else:
            to_dt = self.to_dt

        return df.filter((f.col(self.dt_colname) >= from_dt) & (f.col(self.dt_colname) <= to_dt))

    def get_params(self):
        return {"dt_colname": self.dt_colname,
                "from_dt": self.from_dt,
                "to_dt": self.to_dt}