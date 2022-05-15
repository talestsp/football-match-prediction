from pyspark.ml import Estimator, Transformer
from pyspark.ml.util import MLWritable, MLReadable
from pyspark.ml.param.shared import HasInputCol, HasPredictionCol
from src.ml.transformers import FillProbaTransformer, HomeFactorTransformer
from src.ml.estimators_lib import fill_proba_estimator, home_factor_estimator
from src.utils import dflib

class FillProbaEstimator(Estimator, MLReadable, MLWritable):
    def __init__(self, strategy, labels, proba_vector_col, strategy_b=None):
        '''
        Fill missing probability results with one of the strategies: `uniform_proba`, `global_frequency` or `league_frequency`.
        If the chosen strategy is `league_frequency`, a secondary strategy must be set at parameter `strategy_b`to deal with missing league_id values in fitting stage.
        :param strategy: `uniform_proba`, `global_frequency` or `league_frequency`.
        :param labels:
        :param output_col:
        '''

        super().__init__()
        self.strategy = strategy
        self.labels = labels
        self.proba_vector_col = proba_vector_col
        self.strategy_b = strategy_b

        if strategy == "league_frequency" and strategy_b is None:
            raise Exception(f"Parameter strategy_b must be set when strategy is {strategy}")


    def _fit(self, df):
        probas = fill_proba_estimator.build(df)

        if self.strategy_b is None:
            fill_proba_b_transformer = None
        else:
            fill_proba_b_transformer = FillProbaTransformer(strategy=self.strategy_b,
                                                            probas=probas,
                                                            labels=self.labels,
                                                            proba_vector_col=self.proba_vector_col)

        fill_proba_transformer = FillProbaTransformer(strategy=self.strategy,
                                                      probas=probas,
                                                      labels=self.labels,
                                                      proba_vector_col=self.proba_vector_col,
                                                      strategy_b_transformer=fill_proba_b_transformer)

        return fill_proba_transformer



    def get_params(self):
        return {"proba_vector_col": self.proba_vector_col}

class HomeFactorEstimator(Estimator, MLReadable, MLWritable):
    def __init__(self):
        super().__init__()

    def _fit(self, df):
        home_factor, draw_factor, n_matches = home_factor_estimator.build(df)

        return HomeFactorTransformer(home_factor=home_factor, draw_factor=draw_factor, n_matches=n_matches)
