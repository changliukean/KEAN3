from utility.dispatchUtils import load_pp_tech_info, convert_uc_dataframe, load_solar_dispatch, load_nuclear_dispatch
from datetime import datetime, date
from database.dbPCUC import put_characteristics
from database.dbDispatch import put_dispatch, get_dispatch
from database.dbLBO import put_powerplant, put_technology, get_powerplants, get_technology, put_financials_lbo, get_financials_lbo, put_lbo_assumptions, get_lbo_assumptions,get_portfolio_with_powerplant,get_powerplants_by_portfolio
from database.dbScenarioMaster import insert_scenario_master, delete_scenario_master
from utility.lboUtils import read_excel_lbo_inputs
from lbo import lbo
from model.Entity import Powerplant
from model.Portfolio import Portfolio
from utility.dateUtils import get_month_list
import numpy as np
import sys
import pandas as pd





if __name__ == '__main__':

    # portfolio = 'Norway'
    # portfolio_obj = Portfolio('Norway')
    # powerplant_df = get_powerplants_by_portfolio(portfolio)


    """ diff report """
    portfolio = 'Norway'
    first_lbo_scenario = 'Norway'
    first_lbo_version = 'v7'
    second_lbo_scenario = 'Norway'
    second_lbo_version = 'v6'
    dest_file_path = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\reports\\" + portfolio

    first_lbo_financials_df = get_financials_lbo(portfolio, first_lbo_scenario, first_lbo_version)
    second_lbo_financials_df = get_financials_lbo(portfolio, second_lbo_scenario, second_lbo_version)
    lbo.write_lbo_financials_diff_report(dest_file_path, portfolio, first_lbo_financials_df, second_lbo_financials_df)

    sys.exit()

    portfolio = 'Vector'
    first_lbo_scenario = 'Vector'
    first_lbo_version = 'v12.2'
    second_lbo_scenario = 'Vector'
    second_lbo_version = 'v12'
    dest_file_path = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\reports\\" + portfolio

    first_lbo_financials_df = get_financials_lbo(portfolio, first_lbo_scenario, first_lbo_version)
    second_lbo_financials_df = get_financials_lbo(portfolio, second_lbo_scenario, second_lbo_version)
    lbo.write_lbo_financials_diff_report(dest_file_path, portfolio, first_lbo_financials_df, second_lbo_financials_df)


    # """ graphs output """
    # portfolio = 'Vector'
    # lbo_financials_scenario = 'Vector'
    # lbo_financials_version = 'v7.1'
    # lbo_graph_output_template = 'Dispatch Output_Graphs template.xlsx'
    # lbo_financials_df = get_financials_lbo(portfolio, lbo_financials_scenario, lbo_financials_version)
    # lbo.write_lbo_graph_report('Dispatch Output_Graphs template.xlsx', lbo_financials_df)





    # lbo_financials_df = get_financials_lbo(portfolio, lbo_financials_scenario, lbo_financials_version)
    # dest_file_path = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\reports\\" + portfolio
    #
    # lbo.write_lbo_financials_report_monthly(dest_file_path, lbo_financials_df, portfolio)


    # #




# #
