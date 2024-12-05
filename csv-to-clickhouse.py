import os
import glob
import pandas as pd
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SECURE = os.getenv("DB_SECURE")
dirpath = "./csvs/"  # path to dir with CSV-files


def csv2clickhouse(localpath, dbhost, dbport, dbname, dbuser, dbsecure):
    client = clickhouse_connect.get_client(
        host=dbhost, port=dbport, username=dbuser, secure=dbsecure
    )

    fileslist = [
        filename.rsplit("/", 1)[1] for filename in glob.glob(localpath + "*.csv")
    ]

    client.command(f"CREATE DATABASE IF NOT EXISTS {dbname}")

    for index, filename in enumerate(fileslist):
        print(f"NUMBER: {index} FILE: {filename[:-4]}")

        df = pd.read_csv(
            dirpath + filename,
            sep=";",
            index_col=False,
            dtype="unicode",
        )
        columnName = list(df.columns.values)
        columnType = [str(elem) for elem in df.dtypes]

        for i in range(len(columnType)):
            if columnType[i] == "int8":
                columnType[i] = "Int8"
            elif columnType[i] == "int16":
                columnType[i] = "Int16"
            elif columnType[i] == "int32":
                columnType[i] = "Int32"
            elif columnType[i] == "int64":
                columnType[i] = "Int64"
            elif columnType[i] == "int128":
                columnType[i] = "Int128"
            elif columnType[i] == "int256":
                columnType[i] = "Int256"

            elif columnType[i] == "uint8":
                columnType[i] = "UInt8"
            elif columnType[i] == "uint16":
                columnType[i] = "UInt16"
            elif columnType[i] == "uint32":
                columnType[i] = "UInt32"
            elif columnType[i] == "uint64":
                columnType[i] = "UInt64"
            elif columnType[i] == "uint128":
                columnType[i] = "UInt128"
            elif columnType[i] == "uint256":
                columnType[i] = "UInt256"

            elif columnType[i] == "float16":
                columnType[i] = "Float32"
            elif columnType[i] == "float32":
                columnType[i] = "Float32"
            elif columnType[i] == "float64":
                columnType[i] = "Float64"
            elif columnType[i] == "float128":
                columnType[i] = "Float128"

            elif columnType[i] == "bool":
                columnType[i] = "Boolean"

            elif columnType[i] == "datetime64":
                columnType[i] = "DateTime"

            else:
                columnType[i] = "String"

        columnNameTypes = dict(zip(columnName, columnType))

        query = ""
        for item in columnNameTypes:
            print(item, columnNameTypes[item])
            query += item + " Nullable(" + columnNameTypes[item] + "), "

        query = f"CREATE OR REPLACE TABLE {dbname}.{filename[:-4]} ({query[:-2]}) ENGINE = MergeTree() ORDER BY tuple()"

        print(query, end="\n\n")

        client.command(query)
        print(f"The table {dbname}.{filename[:-4]} created.", end="\n")

        print(
            f"Data is loaded into the table {dbname}.{filename[:-4]} uploaded...",
            end="\n",
        )
        print(df, end="\n\n")
        client.insert_df(f"{dbname}.{filename[:-4]}", df)
        print(f"Data into the table {dbname}.{filename[:-4]} has uploaded.", end="\n\n")


csv2clickhouse(dirpath, DB_HOST, DB_PORT, DB_NAME, "default", DB_SECURE)
