from pyspark.ml.evaluation import MulticlassClassificationEvaluator

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