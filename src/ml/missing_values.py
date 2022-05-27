from pyspark.ml.feature import Imputer


def imputer(df_train, df_test, use_features, strategy='median'):
    imputer = Imputer(strategy=strategy, inputCols=use_features, outputCols=use_features).fit(df_train)
    df_test_imputed = imputer.transform(df_test)

    return df_test_imputed