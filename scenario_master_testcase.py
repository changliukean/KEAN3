from scenario_control.Scenario import Scenario, ScenarioMaster
from datetime import datetime, date
from financial.FSLI import FSLI


if __name__ == "__main__":
    print ("here we start our testing script")
    print ("---------------------------------------------")


    # Test case 1, load a financials scenario from database
    print ("Test case 1, load a financials scenario from database")

    module = 'financials'
    table = 'financials_dev'
    portfolio = 'Lightstone'
    scenario = '2019 Dec AMR'
    version = 'v2'


    myFinancialsScenario = Scenario(module, table, portfolio, scenario, version)
    myFinancialsScenario.print_scenario()

    print ("------------------------------------------------")
    myFinancialsScenarioMaster = ScenarioMaster(myFinancialsScenario)
    myFinancialsScenarioMaster.load_sm_fromdb()
    print (myFinancialsScenarioMaster)


    # Test case 2, initiate a financials scenario to database
    print ("================================================")
    print ("================================================")
    print ("Test case 2, initiate a financials scenario to database")



    new_module, new_table, new_portfolio, new_scenario, new_version = 'financials', 'financials_dev', 'Lightstone', '2019 Dec AMR OOB Test', 'v1'
    new_dispatch_module, new_dispatch_table, new_dispatch_portfolio, new_dispatch_scenario, new_dispatch_version = 'dispatch', 'dispatch', 'Lightstone', '2019 Dec AMR OOB Dispatch Test', 'v1'

    # we will get pure data matrix from KAT
    portfolio = 'Lightstone'

    # step 1. define the output scenario
    output_financials_scenario = Scenario('financials','financials_dev', portfolio, '2020 OOB Test Financials','v1','comments')

    # step 2. define the dispatch input scenario master
    output_dispatch_scenario = Scenario('dispatch','dispatch', portfolio, '2020 OOB Test Dispatch','v1','comments')

    output_dispatch_start_year = 2020
    output_dispatch_number_of_years = 6
    output_dispatch_forecast_start_month = date(2020, 2, 29)
    output_dispatch_valuation_date = date(2020, 1, 29)


    dispatch_input_list = [['curve','prices', portfolio, '2020 OOB Test Curve','v1','comments'],
                           ['curve_basis','prices', portfolio, '2020 OOB Test Basis','v1','comments'],
                           ['hourly_shaper','prices', portfolio, '2020 OOB Test Hourly Shaper','v1','comments'],
                           ['plant_characteristics', portfolio, 'plant_characteristics','2020 OOB Test PCUC','v1','comments']]

    dispatch_input_scenarios = []
    for dispatch_input_data in dispatch_input_list:
        dispatch_input_scenario = Scenario(dispatch_input_data[0], dispatch_input_data[1], dispatch_input_data[2], dispatch_input_data[3], dispatch_input_data[4], dispatch_input_data[5])
        dispatch_input_scenarios.append(dispatch_input_scenario)

    dispatch_scenario_master = ScenarioMaster(output_dispatch_scenario,
                                              output_dispatch_start_year,
                                              output_dispatch_number_of_years,
                                              output_dispatch_forecast_start_month,
                                              output_dispatch_valuation_date,
                                              inputScenarios=dispatch_input_scenarios)


    # step 3. define other input scenarios
    input_scenario_list = [['actuals_accrual', 'gl_activities', portfolio, '2020 OOB all transactions', 'v1', 'comment'],
                           ['actuals_cash', 'gl_activities', portfolio, '2020 OOB all invoices paidinfull', 'v1', 'comment'],
                           ['project_reforecast', 'projects', portfolio, '2020 OOB project reforecast', 'v1', 'comment'],
                           ['census', 'census', portfolio, '2020 OOB census', 'v1', 'comment'],
                           ['labor_assumptions', 'assumptions', portfolio, '2020 OOB labor assumptions', 'v1', 'comment'],
                           ['fsli_directload', 'assumptions', portfolio, '2020 OOB direct load fsli', 'v1', 'comment'],
                           ['prior_forecast', 'financials_dev', portfolio, '2020 OOB prior forecast', 'v1', 'comment'],
                           ['budget', 'financials_dev', portfolio, '2020 OOB budget', 'v1', 'comment']]


    financials_input_scenarios = []
    for input_scenario in input_scenario_list:
        input_scenario = Scenario(input_scenario[0], input_scenario[1], input_scenario[2], input_scenario[3], input_scenario[4], input_scenario[5])
        financials_input_scenarios.append(input_scenario)

    # step 4. define financials scenario master
    # by default, financials have the same datetime information with dispatch
    financials_start_year = 2020
    financials_number_of_years = 6
    financials_forecast_start_month = date(2020, 2, 29)
    financials_valuation_date = date(2020, 1, 29)


    dispatch_scenario_master = ScenarioMaster(output_financials_scenario,
                                              financials_start_year,
                                              financials_number_of_years,
                                              financials_forecast_start_month,
                                              financials_valuation_date,
                                              inputScenarios=financials_input_scenarios,
                                              inputScenarioMasters=[dispatch_scenario_master])

    # print (dispatch_scenario_master)
    dispatch_scenario_master.save()


    # Test case 3, load an inserted financials scenario from database

    module = 'financials'
    table = 'financials_dev'
    portfolio = 'Lightstone'
    scenario = '2020 OOB Test Financials'
    version = 'v1'

    myFinancialsScenario = Scenario(module, table, portfolio, scenario, version)
    myFinancialsScenario.print_scenario()

    print ("------------------------------------------------")
    myFinancialsScenarioMaster = ScenarioMaster(myFinancialsScenario)
    myFinancialsScenarioMaster.load_sm_fromdb()
    print (myFinancialsScenarioMaster)













# #
