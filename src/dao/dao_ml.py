import os
from src.dao import dao

RESULTS_DIR = dao.DATA_DIR + "results/"
MODELS_DIR = "models/"

def save_result(result, id_data_build, model, id_model):
    filepath_results = RESULTS_DIR + "result-" + id_data_build + "-" + id_model + ".json"
    dao.save_json(result, filepath_results)

    filepath_model = MODELS_DIR + "result-" + id_data_build + "-" + id_model
    model.write().save(filepath_model)

def load_result(id_data_build, id_model):
    filepath_results = RESULTS_DIR + "result-" + id_data_build + "-" + id_model + ".json"
    result = dao.load_json(filepath_results)
    return result

def load_all_results():
    all_results = []
    for filepath in os.listdir(RESULTS_DIR):
        if not filepath.endswith(".json"):
            continue
        all_results.append(dao.load_json(RESULTS_DIR + filepath))

    return all_results
