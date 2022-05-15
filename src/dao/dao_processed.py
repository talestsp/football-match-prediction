from src.dao import dao

PROCESSED_DATA_DIR = dao.DATA_DIR + "processed/"
METADATA_PROCESSED_DATA_DIR = PROCESSED_DATA_DIR + "metadata/"


def save_processed_data(df, which_dataset, id, sep=";"):
    df_filepath = PROCESSED_DATA_DIR + id + "_" + which_dataset + ".csv"
    dao.save_data(df, df_filepath, sep=sep)

def load_processed_data(which_dataset, id, spark, header=True, sep=";"):
    df_filepath = PROCESSED_DATA_DIR + id + "_" + which_dataset + ".csv"
    df = dao.load_parse_data(filepath=df_filepath, spark=spark, header=header, sep=sep)

    return df

def save_processed_metadata(metadata_json, id):
    metadata_filepath = METADATA_PROCESSED_DATA_DIR + id + ".json"
    dao.save_json(metadata_json, metadata_filepath)

def load_processed_metadata(id):
    metadata_filepath = METADATA_PROCESSED_DATA_DIR + id + ".json"
    metadata_json = dao.load_json(metadata_filepath)

    return metadata_json