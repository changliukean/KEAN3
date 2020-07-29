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



def run_convert_uc(project_name, date_start, date_end, pc_scenario, pc_version, plant_list=[], plant_tech_master_file=None, push_to_powerplant=False, push_to_technology=False, push_to_plant_characteristics=False):
    # "ERCOT", "HAYSEN3_4", "ERCOT", "HB_SOUTH", date(2017,1,1), date(2019,12,31), 'Day Ahead', 'Hays'
    # name, fuel_type, market, node, power_hub
    if plant_tech_master_file:
        ready_to_kean_pp_df, ready_to_kean_tech_df = load_pp_tech_info(plant_tech_master_file)

    ready_to_kean_tech_df['project'] = project_name
    if push_to_powerplant:
        put_powerplant(ready_to_kean_pp_df)
    if push_to_technology:
        put_technology(ready_to_kean_tech_df)




    powerplant_df = get_powerplants_by_portfolio(project_name)

    if plant_list != []:
        powerplant_df = powerplant_df.loc[powerplant_df.name.isin(plant_list)]

    technology_df = get_technology(project_name)

    print (len(powerplant_df))
    print (len(technology_df))

    ready_to_kean_converted_pc_df = convert_uc_dataframe(powerplant_df, technology_df, pc_scenario, pc_version, date_start, date_end)


    if push_to_plant_characteristics:
        put_characteristics(ready_to_kean_converted_pc_df, pc_scenario, pc_version)
    return ready_to_kean_converted_pc_df




def run_basis_calculation(powerplant_df,basis_start_date, basis_end_date, selected_powerplant_list=None):

    portfolio_basis_result_df = pd.DataFrame()
    portfolio_basis_detail_df = pd.DataFrame()
    for index, row in powerplant_df.iterrows():
        if selected_powerplant_list is None:
            if row['node'] != '' and row['power_hub'] != '':
                test_pp = Powerplant(row['name'], row['fuel_type'], row['market'], row['node'], row['power_hub'])
                merged_hub_nodal_lmp_df, monthly_onoffpeak_basis_df = test_pp.build_basis(basis_start_date, basis_end_date, 'Day Ahead')
                portfolio_basis_result_df = portfolio_basis_result_df.append(monthly_onoffpeak_basis_df)
                portfolio_basis_detail_df = portfolio_basis_detail_df.append(merged_hub_nodal_lmp_df)

        else:
            if row['node'] != '' and row['power_hub'] != '' and row['name'] in selected_powerplant_list:
                test_pp = Powerplant(row['name'], row['fuel_type'], row['market'], row['node'], row['power_hub'])
                merged_hub_nodal_lmp_df, monthly_onoffpeak_basis_df = test_pp.build_basis(basis_start_date, basis_end_date, 'Day Ahead')
                portfolio_basis_result_df = portfolio_basis_result_df.append(monthly_onoffpeak_basis_df)
                portfolio_basis_detail_df = portfolio_basis_detail_df.append(merged_hub_nodal_lmp_df)

    portfolio_basis_result_df = portfolio_basis_result_df.reset_index()
    portfolio_basis_result_df = pd.melt(portfolio_basis_result_df, id_vars=['month','peak_info','plant'],
                                        value_vars=['basis_$','basis_%'],
                                        var_name='instrument',
                                        value_name='value')
    portfolio_basis_result_df['instrument_id'] = portfolio_basis_result_df.apply(lambda row: row['plant'] + ' basis - ' + row['peak_info'] + "_" + row['instrument'].split("_")[1], axis=1)

    portfolio_basis_result_df = portfolio_basis_result_df.reset_index()

    portfolio_basis_result_df = pd.pivot_table(portfolio_basis_result_df, index=['month'], columns=['instrument_id'], values='value', aggfunc=np.sum)

    portfolio_basis_result_df = portfolio_basis_result_df.reset_index()

    return portfolio_basis_result_df, portfolio_basis_detail_df



def load_nondispatchable_plants(portfolio, scenario, version, type, plant_name, assumptions_file_path):

    if type == 'solar':
        solar_plant = plant_name
        solar_dispatch_df = load_solar_dispatch(portfolio, scenario, version, solar_plant, assumptions_file_path)
        put_dispatch(portfolio, scenario, version, solar_dispatch_df)


    if type == 'nuclear':
        nuc_plant = plant_name
        nuc_dispatch_df = load_nuclear_dispatch(portfolio, scenario, version, nuc_plant, assumptions_file_path)
        put_dispatch(portfolio, scenario, version, nuc_dispatch_df)




def load_lbo_assumptions(lbo_assumptions_file_path, portfolio, scenario, version, fsli_list, overwrite_option):
    total_lbo_assumptions_input_df = read_excel_lbo_inputs(lbo_assumptions_file_path, fsli_list)
    total_lbo_assumptions_input_df['scenario'] = scenario
    total_lbo_assumptions_input_df['version'] = version
    total_lbo_assumptions_input_df['portfolio'] = portfolio
    ready_to_kean_lbo_assumptions_df = total_lbo_assumptions_input_df
    put_lbo_assumptions(ready_to_kean_lbo_assumptions_df, portfolio, scenario, version, overwrite_option=overwrite_option)












if __name__ == '__main__':

    portfolio = 'Norway'
    portfolio_obj = Portfolio('Norway')
    powerplant_df = get_powerplants_by_portfolio(portfolio)

    # powerplant_df.to_csv("ppd.csv")

    """ 1 Convert PCUC file and save it to KEAN """
    # plant_tech_master_file = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Norway\pcuc\Norway plant char assumption_input v11.xlsx"
    # pc_date_start = date(2020, 1, 1)
    # pc_date_end = date(2027,12,31)
    # pc_scenario = 'Norway Converted'
    # pc_version = 'v1'
    # # run_convert_uc(plant_tech_master_file, pc_date_start, pc_date_end, pc_scenario, pc_version)
    # run_convert_uc('Norway', pc_date_start, pc_date_end, pc_scenario, pc_version, plant_tech_master_file=plant_tech_master_file, push_to_powerplant=False, push_to_technology=True, push_to_plant_characteristics=False)
    #


    """ get powerplant_df """
    # basis_start_date = date(2017,1,1)
    # basis_end_date = date(2019,12,31)
    # # selected_powerplant_list = ['Joppa_EEI','Fayette','Hanging Rock']
    #
    # """ 2 Calcualate basis data for powerplants """
    # portfolio_basis_result_df, portfolio_basis_detail_df = run_basis_calculation(powerplant_df,basis_start_date, basis_end_date)
    # portfolio_basis_result_df.to_excel("basis_result_prices_loader.xlsx")
    # portfolio_basis_detail_df.to_csv("portfolio_basis_detail_df.csv")


    """ 3 load non-dispatchable plants gross energy margin profile """
    # nondispatchable_assumptions_file_path = r'C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Norway\lbo_assumptions\norway_solar_nuclear_assumptions_v2.xlsx'
    #
    # # portfolio, scenario, version, type, plant_name, assumptions_file_path

    """ Norway nuclears and solar """
    # nondispatchable_assumptions_file_path = r'C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Norway\lbo_assumptions\norway_solar_nuclear_assumptions_v3_0226.xlsx'
    # # portfolio, scenario, version, type, plant_name, assumptions_file_path
    # load_nondispatchable_plants('Norway', 'Norway Nuclear', 'v2', 'nuclear', 'South Texas', nondispatchable_assumptions_file_path)
    # sys.exit()


    """ 4 put lbo assumptions """
    lbo_assumptions_file_path = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Norway\lbo_assumptions\Dispatch Model Inputs_Norway_v4_3.18.20.xlsx"
    fsli_list = ['Capacity Revenue','FOM','Taxes','Insurance','Fixed Costs','Hedges','Fixed Fuel Transport','Other Revenue','Ancillary Revenue','Capex']
    lbo_assumptions_scenario = 'Norway'
    lbo_assumptions_version = 'v4'
    # load_lbo_assumptions(lbo_assumptions_file_path, 'Norway', lbo_assumptions_scenario, lbo_assumptions_version, fsli_list, overwrite_option=True)
    # sys.exit()

    """ 5 run financials """
    """ 5.1 get lbo assumptions from KEAN3 """
    lbo_assumptions_df = get_lbo_assumptions('Norway', lbo_assumptions_scenario, lbo_assumptions_version)


    """ 5.2 get dispatch from KEAN3 """
    dispatch_scenario = 'Norway 20200226'
    dispatch_version = 'v1'
    dispatch_df = get_dispatch(portfolio, dispatch_scenario, dispatch_version)


    """ 5.3 build lbo financials """
    lbo_financials_scenario = 'Norway'
    lbo_financials_version = 'v7'
    entity_list = powerplant_df['name']

    print ("number of powerplants: ", len(powerplant_df))
    print ("number of dispatch records: ", len(dispatch_df))
    print ("number of lbo assumptions records: ", len(lbo_assumptions_df))
    lbo_financials_df = lbo.build_lbo_financials(powerplant_df, portfolio, lbo_financials_scenario, lbo_financials_version, dispatch_df, lbo_assumptions_df)
    # lbo_financials_df.to_csv("lbo_financials_df.csv")

    """ 5.4 put lbo financials to KEAN3 """
    put_financials_lbo(lbo_financials_df, portfolio, lbo_financials_scenario, lbo_financials_version, True)

    """ 5.5 put scenario master information to KEAN3 """
    ready_to_kean_sm_df = pd.DataFrame(columns=['portfolio',
                                                'output_module',
                                                'output_table',
                                                'output_scenario',
                                                'output_version',
                                                'input_module',
                                                'input_table',
                                                'input_scenario',
                                                'input_version',
                                                'scenario_level',
                                                'comment'],
                                       data=[[portfolio, 'financials', 'financials_lbo',
                                              lbo_financials_scenario, lbo_financials_version,
                                              'lbo_assumptions', 'EXCEL', 'LBO Assumptions', lbo_assumptions_file_path.split(".")[0][-2:], 'scenario', lbo_assumptions_file_path],
                                             [portfolio, 'financials', 'financials_lbo',
                                              lbo_financials_scenario, lbo_financials_version,
                                              'dispatch', 'dispatch', dispatch_scenario, dispatch_version, 'scenario_master', '']])

    delete_scenario_master(portfolio, lbo_financials_scenario, lbo_financials_version, 'financials', 'financials_lbo')

    insert_scenario_master(ready_to_kean_sm_df)
    #

    """ 5.6 get lbo financials from KEAN3 and write regular report """
    # display simple report for lbo_financials
    lbo_financials_df = get_financials_lbo(portfolio, lbo_financials_scenario, lbo_financials_version)
    dest_file_path = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\reports\\" + portfolio

    lbo.write_lbo_financials_report_monthly(dest_file_path, lbo_financials_df, portfolio)


    """ diff report """





if __name__ == '__main__Vector':

    portfolio = 'Vector'
    portfolio_obj = Portfolio('Vector')
    powerplant_df = get_powerplants_by_portfolio(portfolio)

    # powerplant_df.to_csv("ppd.csv")

    """ 1 Convert PCUC file and save it to KEAN """
    # plant_tech_master_file = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Vector\Vector plant char assumption_input 2.6.20_Gas convertion.xlsx"
    # pc_date_start = date(2020, 1, 1)
    # pc_date_end = date(2027,12,31)
    # pc_scenario = 'Vector Gas Conversion'
    # pc_version = 'v1'
    # # run_convert_uc(plant_tech_master_file, pc_date_start, pc_date_end, pc_scenario, pc_version)
    # run_convert_uc('Vector', pc_date_start, pc_date_end, pc_scenario, pc_version, plant_list=['Kincaid','Miami Fort 7 & 8','Zimmer'], plant_tech_master_file=plant_tech_master_file, push_to_powerplant=False, push_to_technology=False, push_to_plant_characteristics=True)
    # sys.exit()

    #


    """ get powerplant_df """
    # basis_start_date = date(2017,1,1)
    # basis_end_date = date(2019,12,31)
    # # selected_powerplant_list = ['Joppa_EEI','Fayette','Hanging Rock']
    #
    # """ 2 Calcualate basis data for powerplants """
    # portfolio_basis_result_df, portfolio_basis_detail_df = run_basis_calculation(powerplant_df,basis_start_date, basis_end_date)
    # portfolio_basis_result_df.to_excel("basis_result_prices_loader.xlsx")
    # portfolio_basis_detail_df.to_csv("portfolio_basis_detail_df.csv")


    """ 3 load non-dispatchable plants gross energy margin profile """
    # nondispatchable_assumptions_file_path = r'C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Norway\lbo_assumptions\norway_solar_nuclear_assumptions_v2.xlsx'
    #
    # # portfolio, scenario, version, type, plant_name, assumptions_file_path
    # load_nondispatchable_plants(portfolio, 'Vector Solar', 'v2', 'solar', 'Upton 2', nondispatchable_assumptions_file_path)
    # load_nondispatchable_plants('Norway', 'Norway Nuclear Modified Power', 'v1', 'nuclear', 'South Texas', nondispatchable_assumptions_file_path)

    """ Vector nuclears and solar """
    # nondispatchable_assumptions_file_path = r'C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Vector\solar_nuclear_assumptions_v7.2_0313.xlsx'
    # #
    # # # portfolio, scenario, version, type, plant_name, assumptions_file_path
    # load_nondispatchable_plants('Vector', 'Vector Nuclear', 'v7.2', 'nuclear', 'Comanche Peak', nondispatchable_assumptions_file_path)
    # sys.exit()




    """ 4 put lbo assumptions """
    lbo_assumptions_file_path = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\data\Vector\Dispatch Model Inputs_Margin_for V6 and V7_v10.xlsx"
    # fsli_list = ['ICAP', 'Capacity Revenue','FOM','Taxes','Insurance','Fixed Costs','Hedges','Fixed Fuel Transport','Other Revenue','Ancillary Revenue','Capex']
    lbo_assumptions_scenario = 'Vector'
    lbo_assumptions_version = 'v10'
    # load_lbo_assumptions(lbo_assumptions_file_path, 'Vector', lbo_assumptions_scenario, lbo_assumptions_version, fsli_list, overwrite_option=True)
    #
    # sys.exit()

    """ 5 run financials """
    """ 5.1 get lbo assumptions from KEAN3 """
    lbo_assumptions_df = get_lbo_assumptions('Vector', lbo_assumptions_scenario, lbo_assumptions_version)

    print (len(lbo_assumptions_df))

    """ 5.2 get dispatch from KEAN3 """
    dispatch_scenario = 'Vector 20200226 Adjusted'
    dispatch_version = 'v4.2'
    dispatch_df = get_dispatch(portfolio, dispatch_scenario, dispatch_version)


    """ 5.3 build lbo financials """
    lbo_financials_scenario = 'Vector'
    lbo_financials_version = 'v12.2'


    entity_list = powerplant_df['name']
    lbo_financials_df = lbo.build_lbo_financials(powerplant_df, portfolio, lbo_financials_scenario, lbo_financials_version, dispatch_df, lbo_assumptions_df)
    # lbo_financials_df.to_csv("lbo_financials_df.csv")

    """ 5.4 put lbo financials to KEAN3 """
    put_financials_lbo(lbo_financials_df, portfolio, lbo_financials_scenario, lbo_financials_version, True)

    """ 5.5 put scenario master information to KEAN3 """
    ready_to_kean_sm_df = pd.DataFrame(columns=['portfolio',
                                                'output_module',
                                                'output_table',
                                                'output_scenario',
                                                'output_version',
                                                'input_module',
                                                'input_table',
                                                'input_scenario',
                                                'input_version',
                                                'scenario_level',
                                                'comment'],
                                       data=[[portfolio, 'financials', 'financials_lbo',
                                              lbo_financials_scenario, lbo_financials_version,
                                              'lbo_assumptions', 'lbo_assumptions', 'LBO Assumptions', lbo_assumptions_file_path.split(".")[0][-2:], 'scenario', lbo_assumptions_file_path],
                                             [portfolio, 'financials', 'financials_lbo',
                                              lbo_financials_scenario, lbo_financials_version,
                                              'dispatch', 'dispatch', dispatch_scenario, dispatch_version, 'scenario_master', 're-adjust curves based on info from BX 0306']])

    delete_scenario_master(portfolio, lbo_financials_scenario, lbo_financials_version, 'financials', 'financials_lbo')

    insert_scenario_master(ready_to_kean_sm_df)
    #

    """ 5.6 get lbo financials from KEAN3 and write regular report """
    # display simple report for lbo_financials
    lbo_financials_df = get_financials_lbo(portfolio, lbo_financials_scenario, lbo_financials_version)
    dest_file_path = r"C:\Users\cliu\Kindle Energy Dropbox\Chang Liu\LBO\reports\\" + portfolio

    lbo.write_lbo_financials_report_monthly(dest_file_path, lbo_financials_df, portfolio)


    """ diff report """
    # portfolio = 'Norway'
    # first_lbo_scenario = 'Norway'
    # first_lbo_version = 'v3'
    # second_lbo_scenario = 'Norway'
    # second_lbo_version = 'v1'
    #
    # first_lbo_financials_df = get_financials_lbo(portfolio, first_lbo_scenario, first_lbo_version)
    # second_lbo_financials_df = get_financials_lbo(portfolio, second_lbo_scenario, second_lbo_version)
    # lbo.write_lbo_financials_diff_report(dest_file_path, portfolio, first_lbo_financials_df, second_lbo_financials_df)




    # """ graphs output """
    # portfolio = 'Vector'
    # lbo_financials_scenario = 'Vector'
    # lbo_financials_version = 'v7.1'
    # lbo_graph_output_template = 'Dispatch Output_Graphs template.xlsx'
    # lbo_financials_df = get_financials_lbo(portfolio, lbo_financials_scenario, lbo_financials_version)
    # lbo.write_lbo_graph_report('Dispatch Output_Graphs template.xlsx', lbo_financials_df)




# #
