import oracledb
from secret_manager import username, password, hostname

#username = 'admin'
#password = 'password'
#hostname = 'oracledb.cljjq2zchhaf.eu-west-1.rds.amazonaws.com:1521/ORCL'

def oracledb_connection():
    oracle_connection = None
    try:
        oracle_connection = oracledb.connect(user = username, password = password, dsn = hostname)
    except oracledb.Error as e:
        return e
    return oracle_connection