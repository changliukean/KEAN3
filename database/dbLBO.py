import mysql.connector
from database.dbGeneral import HOST,USER,PASSWORD,DATABASE, PROD_DATABASE, config_connection
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, date


def put_financials_lbo(ready_to_kean_lbo_financials_df, portfolio, scenario, version, overwrite_option=False):

    if overwrite_option:
        connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
        delete_sql_statment = """
                                DELETE FROM financials_lbo
                                where
                                portfolio = '""" + portfolio + """'
                                and
                                scenario = '""" + scenario + """'
                                and
                                version = '""" + version + """';
                              """
        cursor = connection_instance.cursor()
        cursor.execute(delete_sql_statment)
        connection_instance.commit()
        connection_instance.close()

    engine_str = 'mysql+mysqlconnector://' + USER + ':' + PASSWORD + '@' + HOST + '/' + DATABASE
    engine = create_engine(engine_str, encoding='latin1', echo=True)
    # prices_df.to_sql(name='prices', con=engine, if_exists='append', index=False)


    step = 3000
    current_index = 0
    while current_index + step < len(ready_to_kean_lbo_financials_df):
        ready_to_kean_lbo_financials_df.iloc[current_index:current_index+step].to_sql(name='financials_lbo', con=engine, if_exists='append', index=False)
        current_index += step

    ready_to_kean_lbo_financials_df.iloc[current_index:].to_sql(name='financials_lbo', con=engine, if_exists='append', index=False)


def get_financials_lbo(portfolio, scenario, version):

    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM financials_lbo
                    where
                    portfolio = %s
                    and
                    scenario = %s
                    and
                    version = %s;
                  """

    lbo_financials_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, scenario, version])
    connection_instance.close()
    return lbo_financials_df


def put_powerplants(ready_to_kean_pp_df, portfolio=None, overwrite_option=False):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)


    if portfolio is not None and overwrite_option:
        print ("============================== herer?")

        sql_statement = " delete from powerplant where name in (select distinct entity_name from portfolio where name = %s and entity_type = 'plant');"

        print (sql_statement, portfolio)
        cursor = connection_instance.cursor()
        cursor.execute(sql_statement, params=[portfolio])
        connection_instance.commit()
        connection_instance.close()



    engine_str = 'mysql+mysqlconnector://' + USER + ':' + PASSWORD + '@' + HOST + '/' + DATABASE
    engine = create_engine(engine_str, encoding='latin1', echo=True)
    ready_to_kean_pp_df.to_sql(name='powerplant', con=engine, if_exists='append', index=False)



def put_powerplant(ready_to_kean_pp_df, id_powerplant=[]):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    engine_str = 'mysql+mysqlconnector://' + USER + ':' + PASSWORD + '@' + HOST + '/' + DATABASE
    engine = create_engine(engine_str, encoding='latin1', echo=True)
    if id_powerplant != [] and id_powerplant is not None:
        sql_statement = """
                        delete from powerplant where id_powerplant in (""" + ", ".join(id_powerplant) + """ );
                       """
        cursor = connection_instance.cursor()
        cursor.execute(sql_statement)
        connection_instance.commit()
        connection_instance.close()

    ready_to_kean_pp_df.to_sql(name='powerplant', con=engine, if_exists='append', index=False)



def put_technology(ready_to_kean_tech_df):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    delete_sql_statment = """
                            DELETE FROM technology;
                          """
    cursor = connection_instance.cursor()
    cursor.execute(delete_sql_statment)
    connection_instance.commit()
    connection_instance.close()

    engine_str = 'mysql+mysqlconnector://' + USER + ':' + PASSWORD + '@' + HOST + '/' + DATABASE
    engine = create_engine(engine_str, encoding='latin1', echo=True)
    ready_to_kean_tech_df.to_sql(name='technology', con=engine, if_exists='append', index=False)





def get_powerplants(effective_date=datetime.now().date()):

    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM powerplant WHERE effective_start <= %s and effective_end >= %s;
                  """
    powerplants_df = pd.read_sql(sql_statement, connection_instance, params=[effective_date, effective_date])
    connection_instance.close()
    return powerplants_df


def get_powerplants_by_portfolio(portfolio, effective_date=datetime.now().date()):

    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM powerplant WHERE effective_start <= %s and effective_end >= %s and name in (select distinct entity_name from portfolio where name = %s and entity_type='plant');
                  """
    powerplants_df = pd.read_sql(sql_statement, connection_instance, params=[effective_date, effective_date, portfolio])
    connection_instance.close()
    return powerplants_df



def get_powerplant(name, fuel_type, market, node, power_hub, effective_date=datetime.now().date()):

    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM powerplant
                    where name = %s
                    and
                    fuel_type = %s
                    and
                    market = %s
                    and
                    node = %s
                    and
                    power_hub = %s
                    and
                    effective_start <= %s
                    and
                    effective_end >= %s
                    ;
                  """

    powerplant_df = pd.read_sql(sql_statement, connection_instance, params=[name, fuel_type, market, node, power_hub, effective_date, effective_date])
    connection_instance.close()
    return powerplant_df






def get_technology(project):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM technology where project = %s;
                  """
    technology_df = pd.read_sql(sql_statement, connection_instance, params=[project])
    connection_instance.close()
    return technology_df



def get_portfolio_with_powerplant(portfolio_name):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """ select
                        a.name as portfolio_name,
                        a.entity_name as powerplant_name,
                        b.technology as technology_name,
                        b.fuel_type as fuel_type,
                        b.market as market,
                        b.power_hub as power_hub,
                        b.power_zone as power_zone,
                        b.power_hub_on_peak as power_hub_on_peak,
                        b.power_hub_off_peak as power_hub_off_peak,
                        b.node as node,
                        b.fuel_zone as fuel_zone,
                        b.fuel_hub as fuel_hub,
                        b.summer_fuel_basis as summer_fuel_basis,
                        b.winter_fuel_basis as winter_fuel_basis,
                        b.summer_duct_capacity as summer_duct_capacity,
                        b.summer_base_capacity as summer_base_capacity,
                        b.winter_duct_capacity as winter_duct_capacity,
                        b.winter_base_capacity as winter_base_capacity,
                        b.first_plan_outage_start as first_plan_outage_start,
                        b.first_plan_outage_end as first_plan_outage_end,
                        b.second_plan_outage_start as second_plan_outage_start,
                        b.second_plan_outage_end as second_plan_outage_end,
                        b.carbon_cost as carbon_cost,
                        b.source_notes as source_notes,
                        b.retirement_date as retirement_date,
                        b.ownership as ownership
                        from
                        (select * from portfolio where name = %s and entity_type='plant' ) as a
                        left join
                        (select * from powerplant ) as b
                        on a.entity_name = b.name
                        where b.effective_start <= CURDATE() and b.effective_end >= CURDATE(); """


    portfolio_with_powerplant_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio_name])
    connection_instance.close()
    return portfolio_with_powerplant_df




def put_lbo_assumptions(ready_to_kean_lbo_assumptions_df, portfolio, scenario, version, overwrite_option=False):

    if overwrite_option:
        connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
        delete_sql_statment = """
                                DELETE FROM lbo_assumptions
                                where
                                portfolio = '""" + portfolio + """'
                                and
                                scenario = '""" + scenario + """'
                                and
                                version = '""" + version + """';
                              """
        cursor = connection_instance.cursor()
        cursor.execute(delete_sql_statment)
        connection_instance.commit()
        connection_instance.close()

    engine_str = 'mysql+mysqlconnector://' + USER + ':' + PASSWORD + '@' + HOST + '/' + DATABASE
    engine = create_engine(engine_str, encoding='latin1', echo=True)

    index = 0
    step = 3000

    while index+step < len(ready_to_kean_lbo_assumptions_df):
        ready_to_kean_lbo_assumptions_df.iloc[index:index+step].to_sql(name='lbo_assumptions', con=engine, if_exists='append', index=False)
        index += step

    ready_to_kean_lbo_assumptions_df.iloc[index:].to_sql(name='lbo_assumptions', con=engine, if_exists='append', index=False)


def get_lbo_assumptions(portfolio, scenario, version):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM lbo_assumptions
                    where
                    portfolio = %s
                    and
                    scenario = %s
                    and
                    version = %s;
                  """

    lbo_assumptions_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, scenario, version])
    connection_instance.close()
    return lbo_assumptions_df





# #
