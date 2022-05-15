from pivot.dao import dao

RAW_DATA_DIR = dao.DATA_DIR + "raw/"
FILEPATH_TRAIN = RAW_DATA_DIR + "train.csv"
FILEPATH_TEST = RAW_DATA_DIR + "test.csv"
FILEPATH_SCORES = RAW_DATA_DIR + "train_target_and_scores.csv"


def load_parse_train_data(spark, header=True):
    return dao.load_parse_data(FILEPATH_TRAIN, spark=spark, header=header)

def load_parse_test_data(spark, header=True):
    return dao.load_parse_data(FILEPATH_TEST, spark=spark, header=header)

def load_parse_scores_data(spark, header=True):
    return dao.load_parse_data(FILEPATH_SCORES, spark=spark, header=header)