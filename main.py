from scenario_control.Scenario import Scenario, ScenarioMaster
from datetime import datetime, date
from financial.FSLI import FSLI




if __name__ == '__main__':
    name = 'Gross Energy Margin'

    year_start = 2020
    year_end = 2025

    gem_value_list = [45636322, 41712668, 46086042, 47736731, 50610844, 54406182]
    otherrev_value_list = [10000000, 10000000, 10000000, 10000000, 10000000, 10000000]
    fixedcosts_value_list = [15000000, 15000000, 15000000, 15000000, 15000000, 15000000]
    capex_value_list = [1500000, 1500000, 1500000, 1500000, 1500000, 1500000]


    gem_fsli_list = []
    otherrev_fsli_list = []
    net_margin_fsli_list = []
    fixedcost_fsli_list = []
    ebitda_fsli_list = []
    total_capex_fsli_list = []




    for year in range(year_start, year_end):
        year_start_date = date(year_start, 1, 1)
        year_end_date = date(year_start, 12, 31)
        index = list(range(year_start, year_end)).index(year)
        gem_fsli = FSLI("Gross Energy Margin", year_start_date, year_end_date, gem_value_list[index], credit_sign=1)
        otherrev_fsli = FSLI("Total Other Revenue", year_start_date, year_end_date, otherrev_value_list[index], credit_sign=1, is_subtotal=True)
        net_margin_fsli = FSLI("Net Margin", year_start_date, year_end_date, credit_sign=1, is_subtotal=True)
        net_margin_fsli.calc_subtotal([gem_fsli, otherrev_fsli])
        gem_fsli_list.append(gem_fsli)
        otherrev_fsli_list.append(otherrev_fsli)
        net_margin_fsli_list.append(net_margin_fsli)
        fixedcost_fsli = FSLI("Total Fixed Costs", year_start_date, year_end_date, fixedcosts_value_list[index], credit_sign=-1, is_subtotal=True)
        fixedcosts_value_list.append(fixedcost_fsli)
        capex_fsli = FSLI("Total Capex", year_start_date, year_end_date, capex_value_list[index], credit_sign=-1, is_subtotal=True)
        ebitda_fsli = FSLI("EBITDA", year_start_date, year_end_date, credit_sign=1, is_subtotal=True)
        ebitda_fsli.calc_subtotal([net_margin_fsli, fixedcost_fsli])
        ebitda_fsli_list.append(ebitda_fsli)






    for obj in ebitda_fsli_list:
        print(obj)














































# #
