import mysql.connector
from database.dbGeneral import HOST,USER,PASSWORD,PROD_DATABASE,config_connection
from sqlalchemy import create_engine
import pandas as pd






def get_dispatch(portfolio, scenario, version):
    connection_instance = config_connection(HOST, USER, PASSWORD, PROD_DATABASE)
    sql_statement = "Select * from dispatch where company = %s and scenario = %s and version = %s; "
    dispatch_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, scenario, version])
    return dispatch_df






def put_dispatch(portfolio, scenario, version, ready_to_kean_dispatch_df):

    connection_instance = config_connection(HOST, USER, PASSWORD, PROD_DATABASE)
    delete_sql_statment = """
                    DELETE FROM dispatch
                    where
                    company = '""" + portfolio + """'
                    and
                    scenario = '""" + scenario + """'
                    and
                    version = '""" + version + """';
                   """
    cursor = connection_instance.cursor()
    cursor.execute(delete_sql_statment)
    connection_instance.commit()
    connection_instance.close()

    engine_str = 'mysql+mysqlconnector://' + USER + ':' + PASSWORD + '@' + HOST + '/' + PROD_DATABASE
    engine = create_engine(engine_str, encoding='latin1', echo=True)


    step = 3000
    current_index = 0
    while current_index + step < len(ready_to_kean_dispatch_df):
        ready_to_kean_dispatch_df.iloc[current_index:current_index+step].to_sql(name='dispatch', con=engine, if_exists='append', index=False)
        current_index += step


    ready_to_kean_dispatch_df.iloc[current_index:].to_sql(name='dispatch', con=engine, if_exists='append', index=False)
























# #
