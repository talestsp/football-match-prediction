import json
from pyspark.sql.types import StructField, StringType, StructType, DateType, BooleanType, IntegerType, FloatType

DATA_DIR = "data/"
FILEPATH_SCHEMA_JSON = "data/schema.json"

MAP_STRING_TYPE = {"StringType": StringType(),
                   "DateType": DateType(),
                   "BooleanType": BooleanType(),
                   "IntegerType": IntegerType(),
                   "FloatType": FloatType()}


def _read_1st_line(spark, filepath, sep):
    line1 = spark.read.format("csv").option("header", False).option("sep", sep).load(filepath).limit(1)
    return line1

def _str_to_class(spark_type):
    return MAP_STRING_TYPE[spark_type]

def _infer_cols_numeric_to_bool(spark, filepath):
    cols_numeric_to_bool = []
    for colname in header_list(spark, filepath):
        if "_is_" in colname:
            cols_numeric_to_bool.append(colname)
    return cols_numeric_to_bool

def save_json(json_data, filepath):
    with open(filepath, 'w') as outfile:
        json.dump(json_data, outfile)

def load_json(filepath):
    with open(filepath) as json_file:
        data = json.load(json_file)
        return data

def read_schema_json():
    return load_json(FILEPATH_SCHEMA_JSON)

def header_list(spark, filepath, sep=","):
    line1 = _read_1st_line(spark, filepath, sep=sep)
    return list(line1.collect()[0].asDict().values())

def build_schema(schema_dict, colnames_order, nullable=True):
    data_schema_list = []

    for colname in colnames_order:
        if colname in schema_dict.keys():
            spark_type = schema_dict[colname]
            struct_field = StructField(colname, _str_to_class(spark_type), nullable)
            data_schema_list.append(struct_field)

    return StructType(fields=data_schema_list)

def _parse_numeric_to_bool(df, colnames_to_parse):
    for colname in colnames_to_parse:
        df = df.withColumn(colname,
                           df[colname] == 1.0)

    return df

def load_data(filepath, spark, header=True, sep=","):
    colnames = header_list(spark, filepath, sep=sep)
    schema = build_schema(read_schema_json(), colnames)
    df = spark.read.csv(path=filepath, header=header, schema=schema, sep=sep)
    return df

def load_parse_data(filepath, spark, header=True, sep=","):
    df = load_data(filepath, spark, header=header, sep=sep)
    df = _parse_numeric_to_bool(df, colnames_to_parse=_infer_cols_numeric_to_bool(spark, filepath))
    return df

def save_data(df, filepath, sep=","):
    df.write.format('com.databricks.spark.csv').mode('overwrite').option("header", "true").option("sep", sep).save(filepath)