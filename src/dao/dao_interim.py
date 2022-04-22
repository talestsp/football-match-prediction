from src.dao import dao, dao_raw

INTERIM_DATA_DIR = dao.DATA_DIR + "interim/"
FILEPATH_TRAIN_TRAIN_IDS = INTERIM_DATA_DIR + "train_train_ids.csv"
FILEPATH_TRAIN_VALID_IDS = INTERIM_DATA_DIR + "train_valid_ids.csv"
SEP = ";"

def load_train_train_ids(spark, header=True):
    return dao.load_parse_data(FILEPATH_TRAIN_TRAIN_IDS, spark, header=header, sep=SEP)

def load_train_valid_ids(spark, header=True):
    return dao.load_parse_data(FILEPATH_TRAIN_VALID_IDS, spark, header=header, sep=SEP)

def save_train_train_ids(df):
    return dao.save_data(df, FILEPATH_TRAIN_TRAIN_IDS, sep=SEP)

def save_train_valid_ids(df):
    return dao.save_data(df, FILEPATH_TRAIN_VALID_IDS, sep=SEP)

def load_train_train_data(spark, header=True):
    train_train_ids = dao.load_parse_data(FILEPATH_TRAIN_TRAIN_IDS, spark, header=header, sep=SEP)
    df = dao_raw.load_parse_train_data(spark).join(train_train_ids, on="id", how="leftsemi")
    return df

def load_train_valid_data(spark, header=True):
    train_valid_ids = dao.load_parse_data(FILEPATH_TRAIN_VALID_IDS, spark, header=header, sep=SEP)
    df = dao_raw.load_parse_train_data(spark).join(train_valid_ids, on="id", how="leftsemi")
    return df
