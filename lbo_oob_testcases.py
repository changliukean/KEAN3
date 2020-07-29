from utility.dispatchUtils import load_pp_tech_info, convert_uc_dataframe, load_solar_dispatch, load_nuclear_dispatch
from datetime import datetime, date
from database.dbPCUC import put_characteristics
from database.dbDispatch import put_dispatch, get_dispatch
from database.dbLBO import put_powerplant, put_technology, get_powerplant, get_technology, put_financials_lbo, get_financials_lbo, put_lbo_assumptions, get_lbo_assumptions
from database.dbScenarioMaster import insert_scenario_master, delete_scenario_master
from utility.lboUtils import read_excel_lbo_inputs
from lbo import lbo
from model.Entity import Powerplant
from model.Portfolio import Portfolio
from utility.dateUtils import get_month_list
import numpy as np
import sys
import pandas as pd
from reportwriter.ReportWriter import ReportWriter








if __name__ == '__main__':
    portfolio = Portfolio('Norway')

    """ Step 1, update powerplants information under portfolio """
    plant_tech_master_file = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Norway\pcuc\Norway plant char assumption_input v12.xlsx"
    # portfolio.update_powerplants_fromexcel(plant_tech_master_file, additional=False)
    portfolio.get_powerplant_fromdb()   # two statements to pick one, either the data is in kean or you need to load from an excel file

    # sys.exit()


    """ Step 2, load/update pcuc data """
    pc_date_start = date(2020, 1, 1)
    pc_date_end = date(2027,12,31)
    pc_scenario = 'Norway Converted'
    pc_version = 'v1'
    technology_df = get_technology('Norway')
    ready_to_kean_converted_pc_df = portfolio.bulk_convert_uc_dataframe(technology_df, pc_scenario, pc_version, pc_date_start, pc_date_end, push_to_kean=True)   # set put swtich to false by default


    """ Step 3, bulk calculate basis information for plants under a portfolio """
    portfolio.get_powerplant_fromdb()
    basis_start_date = date(2017,1,1)
    basis_end_date = date(2019,12,31)
    to_excel = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Norway\lmps\calculated_basis\Norway Basis_0221.xlsx"
    portfolio.bulk_prepare_basis(basis_start_date, basis_end_date, dart='Day Ahead', market='All', to_database_option=False, to_excel=to_excel)










    # #




# #
