from pivot.dao import dao, dao_raw

INTERIM_DATA_DIR = dao.DATA_DIR + "interim/"
FILEPATH_TRAIN_TRAIN_IDS = INTERIM_DATA_DIR + "train_train_ids.csv"
FILEPATH_TRAIN_VALID_IDS = INTERIM_DATA_DIR + "train_valid_ids.csv"
FILEPATH_FT_HOME_FACTOR_WHOLE_TRAIN = INTERIM_DATA_DIR + "home_factor_whole_train.csv"
SEP = ";"

def load_train_train_ids(spark, header=True):
    return dao.load_parse_data(FILEPATH_TRAIN_TRAIN_IDS, spark, header=header, sep=SEP)

def load_train_valid_ids(spark, header=True):
    return dao.load_parse_data(FILEPATH_TRAIN_VALID_IDS, spark, header=header, sep=SEP)

def save_train_train_ids(df):
    dao.save_data(df, FILEPATH_TRAIN_TRAIN_IDS, sep=SEP)

def save_train_valid_ids(df):
    dao.save_data(df, FILEPATH_TRAIN_VALID_IDS, sep=SEP)

def load_train_train_data(spark, header=True):
    train_train_ids = dao.load_parse_data(FILEPATH_TRAIN_TRAIN_IDS, spark, header=header, sep=SEP)
    df = dao_raw.load_parse_train_data(spark).join(train_train_ids, on="id", how="leftsemi")
    return df

def load_train_valid_data(spark, header=True):
    train_valid_ids = dao.load_parse_data(FILEPATH_TRAIN_VALID_IDS, spark, header=header, sep=SEP)
    df = dao_raw.load_parse_train_data(spark).join(train_valid_ids, on="id", how="leftsemi")
    return df

def load_home_factor_whole_train(spark, header=True):
    df = dao.load_data(filepath=FILEPATH_FT_HOME_FACTOR_WHOLE_TRAIN,
                       spark=spark, header=header, sep=SEP)
    return df

def save_home_factor_whole_train(df):
    dao.save_data(df.select(["league_id", "home_factor", "draw_factor", "n_matches"]),
                  FILEPATH_FT_HOME_FACTOR_WHOLE_TRAIN, sep=SEP)