import os
from pivot.dao import dao
import uuid

RESULTS_DIR = dao.DATA_DIR + "results/"
MODELS_DIR = "models/"
PREDS_DIR = dao.DATA_DIR + "preds/"
PREDS_METADATA_DIR = PREDS_DIR + "metadata/"

def save_result(result, id_data_build, model, id_result):
    filepath_results = RESULTS_DIR + "result-" + id_data_build + "-" + id_result + ".json"
    dao.save_json(result, filepath_results)

    filepath_model = MODELS_DIR + "result-" + id_data_build + "-" + id_result
    # model.write().save(filepath_model)

def load_result(id_data_build, id_result):
    filepath_results = RESULTS_DIR + "result-" + id_data_build + "-" + id_result + ".json"
    result = dao.load_json(filepath_results)

    if not "labels" in result.keys():
        result["labels"] = "[]"
    else:
        result["labels"] = str(result["labels"])

    if "id_model" in result.keys():
        result["id_result"] = result["id_model"]
        del result["id_model"]

    return result

def load_all_results():
    all_results = []
    for filepath in os.listdir(RESULTS_DIR):
        if not filepath.endswith(".json"):
            continue

        result_json = dao.load_json(RESULTS_DIR + filepath)

        if not "labels" in result_json.keys():
            result_json["labels"] = "[]"
        else:
            result_json["labels"] = str(result_json["labels"])

        if "id_model" in result_json.keys():
            result_json["id_result"] = result_json["id_model"]
            del result_json["id_model"]


        all_results.append(result_json)

    return all_results

def save_preds(preds_df, metadata):
    id_preds = str(uuid.uuid4())

    filepath_preds = PREDS_DIR + "preds_" + id_preds + ".csv"
    dao.save_data(preds_df, filepath_preds)

    filepath_metadata = PREDS_METADATA_DIR + "preds_metadata_" + id_preds + ".json"
    dao.save_json(metadata, filepath_metadata)

