import oracledb
from SecretManager import *

dsnname = hostname + ":" + port + "/" + databasename

def oracledb_connection():
    oracle_connection = None
    try:
        oracle_connection = oracledb.connect(user = username, password = password, dsn = dsnname)
    except oracledb.Error as e:
        return e
    return oracle_connection