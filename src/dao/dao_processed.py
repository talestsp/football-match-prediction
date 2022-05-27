import os
import pandas as pd
from src.dao import dao

PROCESSED_DATA_DIR = dao.DATA_DIR + "processed/"
METADATA_PROCESSED_DATA_DIR = PROCESSED_DATA_DIR + "metadata/"


def save_processed_data(df, which_dataset, id_data, sep=";"):
    df_filepath = PROCESSED_DATA_DIR + id_data + "_" + which_dataset + ".csv"
    dao.save_data(df, df_filepath, sep=sep)

def load_processed_data(which_dataset, id_data, spark, header=True, sep=";"):
    df_filepath = PROCESSED_DATA_DIR + id_data + "_" + which_dataset + ".csv"
    df = dao.load_parse_data(filepath=df_filepath, spark=spark, header=header, sep=sep)

    return df

def save_processed_metadata(metadata_json, id_data):
    metadata_filepath = METADATA_PROCESSED_DATA_DIR + id_data + ".json"
    dao.save_json(metadata_json, metadata_filepath)

def load_processed_metadata(id_data):
    metadata_filepath = METADATA_PROCESSED_DATA_DIR + id_data + ".json"
    metadata_json = dao.load_json(metadata_filepath)

    return metadata_json

def load_all_metadata():
    metadata_list = []

    for filename in os.listdir(METADATA_PROCESSED_DATA_DIR):
        if not filename.endswith(".json"):
            continue

        metadata_filepath = METADATA_PROCESSED_DATA_DIR + filename
        metadata_list.append(dao.load_json(metadata_filepath))

    return metadata_list

def most_recent_data_build_id():
    all_metadata_df = pd.DataFrame(load_all_metadata())
    all_metadata_df["datetime"] = pd.to_datetime(all_metadata_df["datetime"])
    return all_metadata_df.sort_values("datetime").iloc[-1]["id_data"]