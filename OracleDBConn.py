#import cx_Oracle
import oracledb
print("HelloWorld")

username = 'admin'
passwordd = 'password'
hostname = 'oracledb.cljjq2zchhaf.eu-west-1.rds.amazonaws.com:1521/ORCL'
portnumber = 1521
databasename = "ORCL"

#connection_string =  hostname + ":" + port + "/" + databasename

#oracle_connection = cx_Oracle.connect(connection_string)
oracle_connection = oracledb.connect(user = username, password = passwordd, dsn = hostname)


Query = "Select * from Employee"

#oracle_cursor = oracle_connection.cursor()
#oracle_cursor.execute(Query)

#for row in oracle_cursor:
#    print(row)

with oracle_connection.cursor() as cursor:
    for row in cursor.execute(Query):
        print(row)

print(oracle_connection.version)