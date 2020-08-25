import os
import re
import shutil
import warnings
import pandas as pd
import csv

from zipfile import ZipFile
from reco_utils.dataset.download_utils import maybe_download, download_path
from reco_utils.common.notebook_utils import is_databricks
from reco_utils.common.constants import (
    DEFAULT_USER_COL,
    DEFAULT_ITEM_COL,
    DEFAULT_RATING_COL,
)

try:
    from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        FloatType,
        DoubleType,
        LongType,
        StringType,
    )
    from pyspark.sql.functions import concat_ws, col
except ImportError:
    pass  # so the environment without spark doesn't break
	
DEFAULT_HEADER = (
    DEFAULT_USER_COL,
    DEFAULT_ITEM_COL,
    DEFAULT_RATING_COL
)

# Warning and error messages
WARNING_MOVIE_LENS_HEADER = """MovieLens rating dataset has 3 columns (user id, movie id and rating), but more than 3 column names are provided. Will only use the first four column names."""
WARNING_HAVE_SCHEMA_AND_HEADER = """Both schema and header are provided. The header argument will be ignored."""
ERROR_MOVIE_LENS_SIZE = "Invalid data size. Should be one of {100k, 1m, 10m, or 20m}"
ERROR_HEADER = "Header error. At least user and book column names should be provided"

def _get_schema(header, schema):
    if schema is None or len(schema) == 0:
        # Use header to generate schema
        if header is None or len(header) == 0:
            header = DEFAULT_HEADER
        elif len(header) > 3:
            warnings.warn(WARNING_MOVIE_LENS_HEADER)
            header = header[:3]

        schema = StructType()
        try:
            (
                schema.add(StructField(header[0], IntegerType()))
                .add(StructField(header[1], IntegerType()))
                .add(StructField(header[2], FloatType()))
            )
        except IndexError:
            pass
    else:
        if header is not None:
            warnings.warn(WARNING_HAVE_SCHEMA_AND_HEADER)

        if len(schema) > 3:
            warnings.warn(WARNING_MOVIE_LENS_HEADER)
            schema = schema[:3]

    return schema

def _maybe_download_and_extract(size, dest_path):
    dirs, _ = os.path.split(dest_path)
    if not os.path.exists(dirs):
        os.makedirs(dirs)

    _, rating_filename = os.path.split("BX-Book-Ratings.csv")
    rating_path = os.path.join(dirs, rating_filename)
    _, item_filename = os.path.split("BX-Books.csv")
    item_path = os.path.join(dirs, item_filename)

    if not os.path.exists(rating_path) or not os.path.exists(item_path):
        download_movielens(size, dest_path)
        extract_movielens(size, rating_path, item_path, dest_path)

    return rating_path, item_path

def download_movielens(size, dest_path):
    url = "http://www2.informatik.uni-freiburg.de/~cziegler/BX/BX-CSV-Dump.zip"
    dirs, file = os.path.split(dest_path)
    maybe_download(url, file, work_directory=dirs)

def extract_movielens(size, rating_path, item_path, zip_path):
    filename = "dataset_out_REDUCED.csv"
    with open(filename, "rb") as zf, open(rating_path, "wb") as f:
        shutil.copyfileobj(zf, f)
    with ZipFile(zip_path, "r") as z:
        with z.open("BX-Books.csv") as zf, open(item_path, "wb") as f:
            shutil.copyfileobj(zf, f)
            
def load_spark_df(
    spark,
    size="100k",
    header=None,
    schema=None,
    local_cache_path=None,
    dbutils=None,
    title_col=None,
    genres_col=None,
    year_col=None,
):

    size = size.lower()
    schema = _get_schema(header, schema)
    if len(schema) < 2:
        raise ValueError(ERROR_HEADER)

    book_col = schema[1].name

    with download_path(local_cache_path) as path:
        filepath = os.path.join(path, "BX-CSV-Dump.zip".format(size))
        datapath, item_datapath = _maybe_download_and_extract(size, filepath)
        spark_datapath = "file:///" + datapath  # shorten form of file://localhost/
        
        if is_databricks():
            if dbutils is None:
                raise ValueError(
                    """
                    To use on a Databricks, dbutils object should be passed as an argument.
                    E.g. load_spark_df(spark, dbutils=dbutils)
                """
                )

            # Move rating file to DBFS in order to load into spark.DataFrame
            dbfs_datapath = "dbfs:/tmp/" + datapath
            dbutils.fs.mv(spark_datapath, dbfs_datapath)
            spark_datapath = dbfs_datapath

        # pyspark's read csv currently doesn't support multi-character delimiter, thus we manually handle that
        separator = ";"
        if len(separator) > 1:
            raw_data = spark.sparkContext.textFile(spark_datapath)
            data_rdd = raw_data.map(lambda l: l.split(separator)).map(
                lambda c: [int(c[0]), int(c[1]), float(c[2]), int(c[3])][: len(schema)]
            )
            df = spark.createDataFrame(data_rdd, schema)
        else:
            df = spark.read.csv(
                spark_datapath,
                schema=schema,
                sep=separator,
                header=True,
            )

        # Cache and force trigger action since data-file might be removed.
        df.cache()
        df.count()

    return df