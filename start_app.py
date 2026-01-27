# https://github.com/daryshtaev/multibase_sql_executor

import jaydebeapi, jpype, os, json, re
from modules.arclogrotator import init_logger
from modules.properties_reader import Properties

# Версия программы
app_ver = '0.0.3'

def get_jdbc(url):
    splitted_url = url.split(":")
    dbms = splitted_url[1]
    jdbc_lib_jars = [
        {'firebirdsql': {'driver_class': 'org.firebirdsql.jdbc.FBDriver', 'jdbc_jar': jdbc_jar_firebird}},
        {'oracle': {'driver_class': 'oracle.jdbc.driver.OracleDriver', 'jdbc_jar': jdbc_jar_oracle}},
        {'postgres': {'driver_class': 'org.postgresql.Driver', 'jdbc_jar': jdbc_jar_postres}},
        {'postgresql': {'driver_class': 'org.postgresql.Driver', 'jdbc_jar': jdbc_jar_postres}}
    ]
    jdbc_jar = []
    if dbms == "oracle":
        jdbc_jar = [x[dbms] for x in jdbc_lib_jars if dbms in x]
    elif dbms in ["postgres", "postgresql"]:
        jdbc_jar = [x[dbms] for x in jdbc_lib_jars if dbms in x]
    elif dbms in ["firebirdsql"]:
        jdbc_jar = [x[dbms] for x in jdbc_lib_jars if dbms in x]
    result = {"driver_class": jdbc_jar[0]["driver_class"], "jdbc_jar": jdbc_jar[0]["jdbc_jar"], "dbms": dbms}
    return result

def get_jdbc_server_port(jdbc_url):
    # Функция обрезает адрес jdbc_url с такого значения "jdbc:postgresql://server1:5432/base1" до такого "jdbc:postgresql://server1:5432" - это нужно для получения списка баз данных с конкретных экземпляров СУБД PostgreSQL.
    # Регулярное выражение для вырезания нужной части jdbc_url
    pattern = r'^(.*)/[^/]*$'
    match = re.match(pattern, jdbc_url)
    if match:
        result = match.group(1)
    return result

app_dir = os.path.dirname(os.path.realpath(__file__))
prop = Properties(directory=app_dir, file='app.properties')
log_file = prop.get_property('log.file')
log_level = prop.get_property('log.level', 'INFO')
logger = init_logger(directory=app_dir, file=log_file, level=log_level)
status_text = f'App started, version {app_ver}.'
logger.info(status_text)
java_libjvm_file = prop.get_property("java.libjvm.file")
jdbc_connections_file = prop.get_property("jdbc.connection.list.file")
sql_param_name = "sql_query_select_only.file"
sql_query_file = prop.get_property(sql_param_name)
with open(sql_query_file, 'r', encoding='utf-8') as file:
    sql_query_select = file.read()
    if not sql_query_select.lower().startswith('select'):
        runtime_error_text = f'Выполнение прервано. В файле из переменной "{sql_param_name}" допустимо указывать SQL-запрос только типа "SELECT".'
        logger.error(runtime_error_text)
        raise SystemExit(runtime_error_text)
with open(jdbc_connections_file, 'r') as file:
    jdbc_connections = json.load(file)
# Подготовка среды выполнения - загрузки библиотек JDBC
jdbc_jar_oracle = os.path.join(f'{os.path.dirname(os.path.realpath(__file__))}/lib', 'ojdbc6.jar')
jdbc_jar_postres = os.path.join(f'{os.path.dirname(os.path.realpath(__file__))}/lib', 'postgresql-42.2.1.jar')
jdbc_jar_firebird = os.path.join(f'{os.path.dirname(os.path.realpath(__file__))}/lib', 'jaybird-5.0.11.java8.jar')
# jpype.startJVM(jpype.getDefaultJVMPath(), classpath=[jdbc_jar_oracle, jdbc_jar_postres])
jpype.startJVM(java_libjvm_file, classpath=[jdbc_jar_oracle, jdbc_jar_postres, jdbc_jar_firebird])

for jdbc_connection in jdbc_connections:
    con_jdbc_url = jdbc_connection["jdbc_url"]
    con_jdbc_username = jdbc_connection["jdbc_username"]
    con_jdbc_password = jdbc_connection["jdbc_password"]
    con_jdbc_driver = get_jdbc(con_jdbc_url)["driver_class"]
    con_jdbc_jar = get_jdbc(con_jdbc_url)["jdbc_jar"]
    status_text = f'{con_jdbc_url},{con_jdbc_username} connecting.'
    logger.info(status_text)
    con = jaydebeapi.connect(con_jdbc_driver, con_jdbc_url, {'user': con_jdbc_username, 'password': con_jdbc_password}, con_jdbc_jar)
    status_text = f'{con_jdbc_url},{con_jdbc_username} connected.'
    logger.info(status_text)
    cur = con.cursor()
    status_text = f'SQL executing.'
    logger.info(status_text)
    cur.execute(sql_query_select)
    status_text = f'SQL executed.'
    logger.info(status_text)
    sql_result_items = cur.fetchall()

    for item in sql_result_items:
        # (Начало) Блок для вывода данных о базах на серверах в формате для файла "jdbc_list.json"
        #con_jdbc_url_server_port = get_jdbc_server_port(con_jdbc_url)
        #print_curly_brace1 = '{'
        #print_curly_brace2 = '}'
        #db_jdbc_url = f'{con_jdbc_url_server_port}/{item[0]}'
        #result = f'{print_curly_brace1}"jdbc_url":"{db_jdbc_url}","jdbc_username":"{con_jdbc_username}","jdbc_password":"{con_jdbc_password}"{print_curly_brace2},'
        # (Конец) Блок для вывода данных о базах на серверах в формате для файла "jdbc_list.json".
        result = f'{con_jdbc_url},{con_jdbc_username}\t{item[0]}\t{item[1]}'
        print(result)
    cur.close()
    con.close()
jpype.shutdownJVM()
status_text = 'App completed.'
logger.info(status_text)