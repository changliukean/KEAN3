from database.dbGeneral import HOST, USER, PASSWORD, DATABASE, config_connection
import pandas as pd
from sqlalchemy import create_engine



def get_scenario_master(output_portfolio, output_scenario_name, output_version, output_module, output_table):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statment = """
                    SELECT * FROM scenario_master
                    where
                    portfolio = '""" + output_portfolio + """'
                    and
                    output_module = '""" + output_module + """'
                    and
                    output_scenario = '""" + output_scenario_name + """'
                    and
                    output_table = '""" + output_table + """'
                    and
                    output_version = '""" + output_version + """'
                    ;
                   """

    raw_scenario_master_df = pd.read_sql(sql_statment, connection_instance, params=[])
    connection_instance.close()

    return raw_scenario_master_df



def get_scenario_master_datetime(portfolio, scenario, version, module):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statment = """
                    SELECT * FROM scenario_datetime
                    where
                    portfolio = '""" + portfolio + """'
                    and
                    module = '""" + module + """'
                    and
                    scenario = '""" + scenario + """'
                    and
                    version = '""" + version + """'
                    ;
                   """

    # print (sql_statment)

    raw_scenario_master_datetime_df = pd.read_sql(sql_statment, connection_instance, params=[])
    connection_instance.close()

    return raw_scenario_master_datetime_df



def delete_scenario_master(output_portfolio, output_scenario_name, output_version, output_module, output_table):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    delete_sql_statment = """
                    DELETE FROM scenario_master
                    where
                    portfolio = '""" + output_portfolio + """'
                    and
                    output_module = '""" + output_module + """'
                    and
                    output_scenario = '""" + output_scenario_name + """'
                    and
                    output_table = '""" + output_table + """'
                    and
                    output_version = '""" + output_version + """'
                    ;
                   """
    cursor = connection_instance.cursor()
    cursor.execute(delete_sql_statment)
    connection_instance.commit()
    connection_instance.close()


def delete_scenario_datetime(portfolio, scenario, version, module):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    delete_sql_statment = """
                    DELETE FROM scenario_datetime
                    where
                    portfolio = '""" + portfolio + """'
                    and
                    module = '""" + module + """'
                    and
                    scenario = '""" + scenario + """'
                    and
                    version = '""" + version + """'
                    ;
                   """

    print (delete_sql_statment)

    cursor = connection_instance.cursor()
    cursor.execute(delete_sql_statment)
    connection_instance.commit()
    connection_instance.close()



def insert_scenario_datetime(module, portfolio, scenario, version, start_year, number_of_years, forecast_start_month, valuation_date):
    scenario_datetime_row_df = pd.DataFrame(data=[[module, portfolio, scenario, version, start_year, number_of_years, forecast_start_month, valuation_date]], columns=['module','portfolio','scenario','version','start_year','number_of_years','forecast_start_month','valuation_date'])
    engine_str = 'mysql+mysqlconnector://' + USER + ':' + PASSWORD + '@' + HOST + '/' + DATABASE
    engine = create_engine(engine_str, encoding='latin1', echo=True)
    scenario_datetime_row_df.to_sql(name='scenario_datetime', con=engine, if_exists='append', index=False)




def insert_scenario_master(ready_to_kean_sm_df):
    engine_str = 'mysql+mysqlconnector://' + USER + ':' + PASSWORD + '@' + HOST + '/' + DATABASE
    engine = create_engine(engine_str, encoding='latin1', echo=True)
    ready_to_kean_sm_df.to_sql(name='scenario_master', con=engine, if_exists='append', index=False)















# #
