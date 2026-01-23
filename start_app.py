# https://github.com/daryshtaev/multibase_sql_executor

import jaydebeapi, jpype, os, json
from modules.arclogrotator import init_logger
from modules.properties_reader import Properties

# Версия программы
app_ver = '0.0.1'

def get_jdbc(url):
    splitted_url = url.split(":")
    dbms = splitted_url[1]
    jdbc_lib_jars = [
        {'oracle': {'driver_class': 'oracle.jdbc.driver.OracleDriver', 'jdbc_jar': jdbc_jar_oracle}},
        {'postgres': {'driver_class': 'org.postgresql.Driver', 'jdbc_jar': jdbc_jar_postres}},
        {'postgresql': {'driver_class': 'org.postgresql.Driver', 'jdbc_jar': jdbc_jar_postres}}
    ]
    jdbc_jar = []
    if dbms == "oracle":
        jdbc_jar = [x[dbms] for x in jdbc_lib_jars if dbms in x]
    elif dbms in ["postgres", "postgresql"]:
        jdbc_jar = [x[dbms] for x in jdbc_lib_jars if dbms in x]
    result = {"driver_class": jdbc_jar[0]["driver_class"], "jdbc_jar": jdbc_jar[0]["jdbc_jar"], "dbms": dbms}
    return result

# Программная папка, в которой размещен данный файл:
app_dir = os.path.dirname(os.path.realpath(__file__))

prop = Properties(directory=app_dir, file='app.properties')
log_file = prop.get_property('log.file')
log_level = prop.get_property('log.level', 'INFO')

logger = init_logger(directory=app_dir, file=log_file, level=log_level)
logger.info(f'App started, version {app_ver}.')

java_libjvm_file = prop.get_property("java.libjvm.file")

jdbc_connections_file = prop.get_property("jdbc.connection.list.file")
sql_param_name = "sql_query_select_only.text"
sql_query_select = prop.get_property(sql_param_name).strip()
if not sql_query_select.lower().startswith('select'):
    runtime_error_text = f'Выполнение прервано. В переменной "{sql_param_name}" допустимо указывать SQL-запрос только типа "SELECT".'
    logger.error(runtime_error_text)
    raise SystemExit(runtime_error_text)

with open(jdbc_connections_file, 'r') as file:
    jdbc_connections = json.load(file)

# Подготовка среды выполнения - загрузки библиотек JDBC
jdbc_jar_oracle = os.path.join(f'{os.path.dirname(os.path.realpath(__file__))}/lib', 'ojdbc6.jar')
jdbc_jar_postres = os.path.join(f'{os.path.dirname(os.path.realpath(__file__))}/lib', 'postgresql-42.2.1.jar')
# jpype.startJVM(jpype.getDefaultJVMPath(), classpath=[jdbc_jar_oracle, jdbc_jar_postres])
jpype.startJVM(java_libjvm_file, classpath=[jdbc_jar_oracle, jdbc_jar_postres])

try:
    for jdbc_connection in jdbc_connections:
        con_jdbc_url = jdbc_connection["jdbc_url"]
        con_jdbc_driver = get_jdbc(con_jdbc_url)["driver_class"]
        con_jdbc_jar = get_jdbc(con_jdbc_url)["jdbc_jar"]
        status_text = f'Connecting: {con_jdbc_url}'
        logger.info(status_text)
        con = jaydebeapi.connect(con_jdbc_driver, con_jdbc_url, {'user': jdbc_connection["jdbc_username"], 'password': jdbc_connection["jdbc_password"]}, con_jdbc_jar)
        status_text = f'Connected: {con_jdbc_url}'
        logger.info(status_text)
        print(status_text)
        cur = con.cursor()
        status_text = f'SQL from param "{sql_param_name}" executing ...'
        logger.info(status_text)
        print(status_text)
        cur.execute(sql_query_select)
        status_text = f'SQL from param "{sql_param_name}" executed.'
        logger.info(status_text)
        print(status_text)
        rows = cur.fetchall()
        for row in rows:
            print(row[0])
except Exception as e:
    error_text = repr(e)
    logger.error(error_text)
finally:
    # Закрываем курсоры, подключения, JVM
    cur.close()
    con.close()
    jpype.shutdownJVM()
    logger.info('App completed.')
