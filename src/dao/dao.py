import json
import sys
from pyspark.sql.types import StructField, StringType, StructType, DateType, BooleanType, IntegerType, FloatType

FILEPATH_SCHEMA_JSON = "data/schema.json"
MAP_STRING_TYPE = {"StringType": StringType(),
                   "DateType": DateType(),
                   "BooleanType": BooleanType(),
                   "IntegerType": IntegerType(),
                   "FloatType": FloatType()}

def read_json(filepath):
    with open(filepath) as json_file:
        data = json.load(json_file)
        return data

def read_schema_json():
    return read_json(FILEPATH_SCHEMA_JSON)

def _read_1st_line(filename):
    with open(filename, 'rb') as f:
        return f.readline().decode("utf-8")

def _str_to_class(spark_type):
    return MAP_STRING_TYPE[spark_type]

def header_list(filename, sep=","):
    return _read_1st_line(filename).replace("\n", "").split(sep)

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
                           df[colname].cast("boolean"))

    return df

def load_data(filepath, spark, header=True):
    colnames = header_list(filepath, sep=",")
    schema = build_schema(read_schema_json(), colnames)
    df = spark.read.csv(filepath, schema=schema, header=header)
    return df

def _infer_cols_numeric_to_bool(filepath):
    cols_numeric_to_bool = []
    for colname in header_list(filepath):
        if "_is_" in colname:
            cols_numeric_to_bool.append(colname)
    return cols_numeric_to_bool

def load_parse_data(filepath, spark, header=True):
    df = load_data(filepath, spark, header=header)
    df = _parse_numeric_to_bool(df, colnames_to_parse=_infer_cols_numeric_to_bool(filepath))

    return df

