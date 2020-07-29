import mysql.connector
from database.dbGeneral import HOST,USER,PASSWORD,PROD_DATABASE,config_connection
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime








def put_characteristics(ready_to_kean_pcuc_df, scenario, version):

    connection_instance = config_connection(HOST, USER, PASSWORD, PROD_DATABASE)
    delete_sql_statment = """
                    DELETE FROM plant_characteristics
                    where
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
    while current_index + step < len(ready_to_kean_pcuc_df):
        ready_to_kean_pcuc_df.iloc[current_index:current_index+step].to_sql(name='plant_characteristics', con=engine, if_exists='append', index=False)
        current_index += step


    ready_to_kean_pcuc_df.iloc[current_index:].to_sql(name='plant_characteristics', con=engine, if_exists='append', index=False)
    version_log_df = pd.DataFrame(columns=['timestamp','user','table_name','scenario','version','description', 'number_of_records_inserted'], data=[[datetime.now(),'chang.liu@kindle-energy.com','plant_characteristics',scenario,version,'loaded from script as of ' + str(datetime.now()), len(ready_to_kean_pcuc_df)]])
    version_log_df.to_sql(name='version_log', con=engine, if_exists='append', index=False)





































# #
