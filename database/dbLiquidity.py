import mysql.connector
from database.dbGeneral import HOST,USER,PASSWORD,DATABASE, PROD_DATABASE, config_connection
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, date



def get_financials(portfolio, scenario, version, financials_table):

    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM """ + financials_table + """
                    where
                    portfolio = %s
                    and
                    scenario = %s
                    and
                    version = %s;
                  """

    financials_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, scenario, version])
    connection_instance.close()
    return financials_df


def get_scenario_assumptions(portfolio, scenario, version):

    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM scenario_assumption
                    where
                    portfolio = %s
                    and
                    scenario = %s
                    and
                    version = %s;
                  """

    scenario_assumptions_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, scenario, version])
    connection_instance.close()
    return scenario_assumptions_df


def get_capital_structure(portfolio, scenario, version):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM capital_structure
                    where
                    portfolio = %s
                    and
                    scenario = %s
                    and
                    version = %s;
                  """

    capital_structure_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, scenario, version])
    connection_instance.close()
    return capital_structure_df


# def get_revolver_change(instrument_id):
#     connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
#     sql_statement = """
#                     SELECT * FROM debt_activity
#                     where
#                     instrument_id = %s;
#                   """
#
#     debt_activity_df = pd.read_sql(sql_statement, connection_instance, params=[instrument_id])
#     connection_instance.close()
#     return debt_activity_df


def get_debt_activity(instrument_id):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM debt_activity
                    where
                    instrument_id = %s;
                  """

    debt_activity_df = pd.read_sql(sql_statement, connection_instance, params=[instrument_id])
    connection_instance.close()
    return debt_activity_df



def get_waterfall(portfolio, scenario, version):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM waterfall
                    where
                    portfolio = %s
                    and
                    scenario = %s
                    and
                    version = %s
                    ;
                  """

    waterfall_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, scenario, version])
    connection_instance.close()
    return waterfall_df


def get_distributions(portfolio):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT date, amount FROM distribution
                    where
                    portfolio = %s;
                  """

    distributions_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio])
    connection_instance.close()
    distributions = distributions_df.set_index('date')['amount'].to_dict()
    return distributions


def get_paid_tax(portfolio, as_of_date):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT date, amount FROM distribution
                    where
                    portfolio = %s
                    and
                    type = 'permitted tax distribution'
                    and
                    date <= %s
                    ;
                  """

    paid_tax_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, as_of_date])
    connection_instance.close()
    paid_tax_df = paid_tax_df.set_index('date')['amount'].to_dict()
    return paid_tax_df





def get_cash_balance(portfolio, forecast_start_month):

    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """
                    SELECT * FROM cash_balance
                    where
                    portfolio = %s
                    and
                    as_of_date < %s
                    ;
                    """

    cash_balances_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, forecast_start_month])
    connection_instance.close()
    return cash_balances_df




def get_asset_depreciation(portfolio):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """ SELECT * FROM asset_depreciation where portfolio = %s; """
    asset_depreciation_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio])
    return asset_depreciation_df





def get_swap(portfolio, instrument_id):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """ SELECT * FROM swap
                        where portfolio = %s
                        and instrument_id = %s
                        ;
                    """
    swap_rates_df = pd.read_sql(sql_statement, connection_instance, params=[portfolio, instrument_id])
    return swap_rates_df





def get_curves(scenario, version):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """ SELECT * FROM curve
                        where scenario = %s
                        and version = %s
                        ;
                    """
    curves_df = pd.read_sql(sql_statement, connection_instance, params=[scenario, version])
    return curves_df




def get_rw_headers(name='Default'):
    connection_instance = config_connection(HOST, USER, PASSWORD, DATABASE)
    sql_statement = """ SELECT * FROM rw_headers
                        where name = %s;
                    """
    rw_headers_df = pd.read_sql(sql_statement, connection_instance, params=[name])
    return rw_headers_df



# #
