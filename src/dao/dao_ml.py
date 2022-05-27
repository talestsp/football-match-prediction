import os
from src.dao import dao
import uuid
from datetime import datetime
from src.ml import metrics

RESULTS_DIR = dao.DATA_DIR + "results/"
MODELS_DIR = "models/"
PREDS_DIR = dao.DATA_DIR + "preds/"
PREDS_METADATA_DIR = PREDS_DIR + "metadata/"
FEATURE_SELECTION_DIR = dao.DATA_DIR + "feature_selection/"
MODELING_DIR = dao.DATA_DIR + "modeling/"


def save_result(result, id_data_build, id_result):
    filepath_results = RESULTS_DIR + "result-" + id_data_build + "-" + id_result + ".json"
    dao.save_json(result, filepath_results)

def load_result(id_data_build, id_result):
    filepath_results = RESULTS_DIR + "result-" + id_data_build + "-" + id_result + ".json"
    result = dao.load_json(filepath_results)

    return result

def load_all_results():
    all_results = []
    for filepath in os.listdir(RESULTS_DIR):
        if not filepath.endswith(".json"):
            continue

        result_json = dao.load_json(RESULTS_DIR + filepath)

        all_results.append(result_json)

    return all_results

def save_preds(preds_df, metadata):
    id_preds = str(uuid.uuid4())

    filepath_preds = PREDS_DIR + "preds_" + id_preds + ".csv"
    dao.save_data(preds_df, filepath_preds)

    filepath_metadata = PREDS_METADATA_DIR + "preds_metadata_" + id_preds + ".json"
    dao.save_json(metadata, filepath_metadata)

def save_feature_selection(anova_df, id_data, mutual_corr_max_treshold, cols_to_remove):
    id_selection = str(uuid.uuid4())
    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    selection_json = {}
    selection_json["id_selection"] = id_selection
    selection_json["id_data"] = id_data
    selection_json["datetime"] = dt
    selection_json["mutual_corr_max_treshold"] = mutual_corr_max_treshold
    selection_json["anova"] = anova_df.to_dict()
    selection_json["cols_to_remove"] = cols_to_remove
    
    dao.save_json(selection_json, filepath=FEATURE_SELECTION_DIR + id_selection + ".json")
    print(id_selection)
    return id_selection

def load_feature_selection(id_data=None):
    feature_selection_json_list = []
    for filename in os.listdir(FEATURE_SELECTION_DIR):
        if not filename.endswith(".json"):
            continue

        feature_selection_json = dao.load_json(FEATURE_SELECTION_DIR + filename)
        
        if id_data is None or id_data == feature_selection_json ["id_data"]:
            feature_selection_json_list.append(feature_selection_json)
         
    return feature_selection_json_list

def save_modeling_spark_ml(id_data, cv_model, features, overfitting_analysis_df, n_fold, pipeline_train,
                  grid_search_time):
    id_modeling = str(uuid.uuid4())
    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    best_score_cv = min(cv_model.avgMetrics)
    i_best_score_cv = cv_model.avgMetrics.index(best_score_cv)
    params = {k.name: cv_model.getEstimatorParamMaps()[i_best_score_cv][k] for k in
              cv_model.getEstimatorParamMaps()[i_best_score_cv]}
    score_train_for_best_score_cv = overfitting_analysis_df.loc[overfitting_analysis_df["log_loss_cv"].idxmin()]["log_loss_train"]

    clf = cv_model.bestModel
    feature_importances = metrics.get_feature_importances(clf, features)

    pipeline_train_stages = [type(stage).__name__ for stage in pipeline_train.stages]

    modeling_json = {}
    modeling_json["id_modeling"] = id_modeling
    modeling_json["datetime"] = dt
    modeling_json["id_data"] = id_data
    modeling_json["clf_name"] = type(clf).__name__
    modeling_json["clf_params"] = params
    modeling_json["overfitting_analysis_df"] = overfitting_analysis_df.to_dict()
    modeling_json["best_score_cv"] = best_score_cv
    modeling_json["best_score_cv_train"] = score_train_for_best_score_cv
    modeling_json["feature_importances"] = feature_importances.to_dict()
    modeling_json["pipeline_train_stages"] = pipeline_train_stages
    modeling_json["n_fold"] = n_fold
    modeling_json["grid_search_time"] = grid_search_time

    filepath = MODELING_DIR + id_modeling + ".json"
    print("saving")
    print(filepath)
    dao.save_json(modeling_json, filepath)

    return id_modeling

def save_modeling_xgboost(id_data, grid_search_model, features, overfitting_analysis_df, n_fold, pipeline_train,
                  grid_search_time):
    id_modeling = str(uuid.uuid4())
    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    best_score_cv = min(overfitting_analysis_df["log_loss_cv"])

    params = grid_search_model.best_params_
    score_train_for_best_score_cv = overfitting_analysis_df.loc[overfitting_analysis_df["log_loss_cv"].idxmin()]["log_loss_train"]

    clf = grid_search_model.best_estimator_
    feature_importances = metrics.get_feature_importances(clf, features)

    pipeline_train_stages = [type(stage).__name__ for stage in pipeline_train.stages]

    modeling_json = {}
    modeling_json["id_modeling"] = id_modeling
    modeling_json["datetime"] = dt
    modeling_json["id_data"] = id_data
    modeling_json["clf_name"] = type(clf).__name__
    modeling_json["clf_params"] = params
    modeling_json["overfitting_analysis_df"] = overfitting_analysis_df.to_dict()
    modeling_json["best_score_cv"] = best_score_cv
    modeling_json["best_score_cv_train"] = score_train_for_best_score_cv
    modeling_json["feature_importances"] = feature_importances.to_dict()
    modeling_json["pipeline_train_stages"] = pipeline_train_stages
    modeling_json["n_fold"] = n_fold
    modeling_json["grid_search_time"] = grid_search_time

    filepath = MODELING_DIR + id_modeling + ".json"
    print("saving")
    print(filepath)
    dao.save_json(modeling_json, filepath)

    return id_modeling

def load_modeling(id_modeling):
    result_json = dao.load_json(MODELING_DIR + id_modeling + ".json")
    return result_json

def load_all_modeling():
    all_results = []
    for filepath in os.listdir(MODELING_DIR):
        if not filepath.endswith(".json"):
            continue
        result_json = dao.load_json(MODELING_DIR + filepath)
        all_results.append(result_json)
    return all_results
