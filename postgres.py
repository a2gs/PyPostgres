from sys import exit
import psycopg2
from psycopg2 import Error, Warning
from psycopg2 import OperationalError

def create_connection(dbName, dbUser, dbPasswd, dbHost, dbPort, autoCommit) -> [psycopg2.connect, bool]:
    connection = None

    try:
        connection = psycopg2.connect(database = dbName, user = dbUser, password = dbPasswd,
                                    host = dbHost, port = dbPort)
        print(f"Connection to PostgreSQL DB successful: [{user}@{host}:{port}/{database}]")

    except Warning as e:
        print(f"The error '{e}' occurred")
        return [None, False]
    except (Error, OperationalError) as e:
        print(f"==============================\nPOSTGRES ERROR: {e}\nMore error info:{e.pgerror}\nPostgres erro code: [{e.pgcode}]\n==============================")
        return [None, False]

    connection.autocommit = autoCommit;

    return [connection, True]

# ----------------------------------------------------------

def execSelect(conn : psycopg2.connect, query: str) -> [[], bool]:
    cursor = conn.cursor()
    result = []

    try:
        cursor.execute(query)
        result = cursor.fetchall() #cur.fetchone() or cur.fetchmany(n)
    except Warning as e:
        cursor.close()
        print(f"Warning '{e}' occurred")
        return [[], False]
    except (Error, OperationalError) as e:
        cursor.close()
        print(f"==============================\nPOSTGRES ERROR: {e}\nMore error info:{e.pgerror}\nPostgres erro code: [{e.pgcode}]\n==============================")
        return [[], False]

    cursor.close()
    return result, True

# ----------------------------------------------------------

def execInsertDeleteUpdate(conn : psycopg2.connect, query: str) -> bool:
    try:
        cursor = conn.cursor()
        cursor.execute(query)

    except Warning as e:
        print(f"Warning '{e}' occurred")
        cursor.close()
        return False
    except (Error, OperationalError) as e:
        cursor.close()
        print(f"==============================\nPOSTGRES ERROR: {e}\nMore error info:{e.pgerror}\nPostgres erro code: [{e.pgcode}]\n==============================")
        return False

    cursor.close()
    return True
# ----------------------------------------------------------

database = 'databaseschema'
user = 'postgres'
password = 'abcd1234'
host = 'localhost'
port = 5432

conn, ret = create_connection(database, user, password, host, port, False)
if ret == False:
    exit(0)

result, ret = execSelect(conn, "select * from example")
if ret == True:
    for row in result:
        print(row)

ret = execInsertDeleteUpdate(conn, "insert into example(c1, c2) values ('asd', 'qwe1')")
if ret == False:
    exit(0)

ret = execInsertDeleteUpdate(conn, "delete from example where c1 = 'abc'")

conn.commit()
conn.close()
