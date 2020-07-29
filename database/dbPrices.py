from datetime import datetime, date, timedelta
from calendar import monthrange
from database.dbGeneral import HOST, USER, PASSWORD, DATABASE, PROD_DATABASE, config_connection
import pandas as pd
from sqlalchemy import create_engine




def get_historical_lmp(node_id, start_date, end_date, dart, database=PROD_DATABASE):
    connection_instance = config_connection(HOST, USER, PASSWORD, database)
    sql_statment = """
                    SELECT * FROM lmp_new
                    where
                    node_id = %s
                    and
                    delivery_date >= %s
                    and
                    delivery_date <= %s
                    and
                    dart = %s
                    ;
                   """

    raw_lmp_df = pd.read_sql(sql_statment, connection_instance, params=[node_id, start_date, end_date, dart])
    connection_instance.close()

    return raw_lmp_df
