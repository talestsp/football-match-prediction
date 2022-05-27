import pandas as pd
from src.dao import dao
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def build_overfitting_analysis_df_xgboost(grid_search_model):
    results_df = pd.DataFrame(grid_search_model.cv_results_)

    param_colnames = []

    for colname in results_df.columns:
        if colname.startswith("param_"):
            param_colnames.append(colname)

    use_columns = param_colnames + ["mean_train_score", "mean_test_score", "std_train_score", "std_test_score"]

    map_colnames = {"mean_train_score": "log_loss_train", "mean_test_score": "log_loss_cv",
                    "std_train_score": "std_train_log_loss", "std_test_score": "std_cv_log_loss"}

    results_df = results_df[use_columns].rename(columns=map_colnames)
    results_df[["log_loss_cv", "log_loss_train"]] = results_df[["log_loss_cv", "log_loss_train"]] * -1

    for colname in results_df.columns:
        if colname.startswith("param_"):
            results_df = results_df.rename(columns={colname: colname[6:]})

    return results_df


def build_overfitting_analysis_df_spark_ml(df_train, cv_model, cross_valid, evaluator):
    train_scores = {}
    for k in range(len(cv_model.subModels)):
        train_scores[k] = []

        for params_i in range(len(cv_model.subModels[k])):
            model_k_i = cv_model.subModels[k][params_i]
            preds_train = model_k_i.transform(df_train)
            score_k_i = evaluator.evaluate(preds_train)

            train_scores[k].append(score_k_i)

    all_params = []
    for params_i in cross_valid.getEstimatorParamMaps():
        params = {k.name: params_i[k] for k in params_i.keys()}
        all_params.append(params)

    scores = pd.DataFrame(train_scores).apply(lambda row: row.mean(), axis=1).to_frame(
        evaluator.getMetricName() + "_train")
    scores[evaluator.getMetricName() + "_cv"] = cv_model.avgMetrics

    of_df = scores.join(pd.DataFrame(all_params))
    of_df = of_df.rename(columns={"logLoss_cv": "log_loss_cv", "logLoss_train": "log_loss_train"})
    return of_df

def get_feature_importances(clf, features):
    package_data = dao.get_package_from(clf).split()

    if "xgboost" in package_data or "lightgbm" in package_data:
        importances = clf.feature_importances_

    elif "pyspark" in package_data:
        importances = clf.featureImportances.values

    feature_importances = pd.DataFrame(
        {"feature": features, "importance": importances}).set_index("feature").sort_values(
        "importance", ascending=False)

    return feature_importances


def get_metrics(predictions, labelCol="target_indexed", predictionCol="prediction", probabilityCol="proba"):

    evaluator = MulticlassClassificationEvaluator(labelCol=labelCol,
                                                  predictionCol=predictionCol,
                                                  probabilityCol=probabilityCol)

    accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
    # print("accuracy", accuracy)

    log_loss = evaluator.evaluate(predictions, {evaluator.metricName: "logLoss"})
    # print("log_loss", log_loss)

    precision_home = evaluator.evaluate(predictions,
                                        {evaluator.metricName: "precisionByLabel",
                                         evaluator.metricLabel: 0})
    # print("precision_home", precision_home)
    precision_draw = evaluator.evaluate(predictions,
                                        {evaluator.metricName: "precisionByLabel",
                                         evaluator.metricLabel: 1})
    # print("precision_draw", precision_draw)
    precision_away = evaluator.evaluate(predictions,
                                        {evaluator.metricName: "precisionByLabel",
                                         evaluator.metricLabel: 2})
    # print("precision_away", precision_away)

    recall_home = evaluator.evaluate(predictions,
                                        {evaluator.metricName: "recallByLabel",
                                         evaluator.metricLabel: 0})
    # print("recall_home", recall_home)
    recall_draw = evaluator.evaluate(predictions,
                                        {evaluator.metricName: "recallByLabel",
                                         evaluator.metricLabel: 1})
    # print("recall_draw", recall_draw)
    recall_away = evaluator.evaluate(predictions,
                                        {evaluator.metricName: "recallByLabel",
                                         evaluator.metricLabel: 2})
    # print("recall_away", recall_away)

    # f1_home = evaluator.evaluate(predictions,
    #                                     {evaluator.metricName: "fMeasureByLabel",
    #                                      evaluator.metricLabel: 0})
    # # print("f1_home", f1_home)
    # f1_draw = evaluator.evaluate(predictions,
    #                                     {evaluator.metricName: "fMeasureByLabel",
    #                                      evaluator.metricLabel: 1})
    # # print("f1_draw", f1_draw)
    # f1_away = evaluator.evaluate(predictions,
    #                                     {evaluator.metricName: "fMeasureByLabel",
    #                                      evaluator.metricLabel: 2})
    # # print("f1_away", f1_away)

    # metrics_dict = {"accuracy": accuracy, "log_loss": log_loss, "precision_home": precision_home,
    #                 "precision_draw": precision_draw, "precision_away": precision_away, "recall_home": recall_home,
    #                 "recall_draw": recall_draw, "recall_away": recall_away, "f1_home": f1_home,
    #                 "f1_draw": f1_draw, "f1_away": f1_away}

    metrics_dict = {"accuracy": accuracy, "log_loss": log_loss, "precision_home": precision_home,
                    "precision_draw": precision_draw, "precision_away": precision_away, "recall_home": recall_home,
                    "recall_draw": recall_draw, "recall_away": recall_away}


    return metrics_dict


