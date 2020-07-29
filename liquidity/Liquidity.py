from scenario_control.Scenario import Scenario, ScenarioMaster
from datetime import date, timedelta
from dateutil import relativedelta
from utility.dateUtils import get_date_obj_from_str
from calendar import monthrange
import numpy as np
from decimal import *
import pandas as pd
import sys

from scipy.optimize import fsolve

from utility import dateUtils
from database import dbLiquidity

a=1

class Liquidity:



    def __init__(self,
                 portfolio,
                 liquidity_scenario,
                 liquidity_version,
                 metadata={},
                 table='placeholder'):
        self.portfolio = portfolio

        self.scenarioMaster = ScenarioMaster(Scenario('liquidity', table, portfolio, liquidity_scenario, liquidity_version))
        # this is a ScenarioMaster object that stores all the related scenario master information for a liquidity run
        #  1. financials, basically all financial information for the liquidity scenario including Adj EBITDA, CAPEX, actual cash begin and ending balances etc
        #     1.1 libor curves along with the financials SM object
        #  2. capital structure scenario, all capital structure information including Operting Company, Term Loans, Equity, Revolver
        #  3. waterfall structure scenario, a defined waterfall for how cash should flow and the priorities of different tier debts
        #  4. liquidity assumptions scenario and version, the forced values for change in working capital, other cash use and revolver draw and pay back
        #  5. interest rate scenario and version, the libor rates information stored in prices table
        #  6. dates and time information for the liquidity e.g. forecast start date, actuals begin date
        self.scenarioMaster.load_sm_fromdb()
        self.scenarioMaster.load_scenario_datetime_fromdb()


        self.assumptions = self.__initialize_liquidity_assumptions()
        # this is the scenario assumptions that are related to the liquidity process
        # including items like: change in working capital, other cash use, projected revolver draw and repay

        self.interestRates = self.__initialize_interest_rate()
        # a dataframe of interest rates, being used within liquidity module at multiple places


        self.capitalStructure = self.__initialize_captital_structure()
        # captial structure is the list of instruments that this liquidity model has
        # will be reading information from its scenarioMaster object and initialize all objects

        self.waterfall = self.__initialize_waterfall()
        # waterfall information is a dataframe of cash inflow/outflow orders and priorities
        # for the direction of a waterfall item, positive inflow is an income and negative inflow means an outcome or expense

        self.fixedAssets = self.__initializ_fixed_asset_depreciation()
        # a list of fixed assets objects for the purpose of tax distribution calcualtion


        self.metadata = self.__initialize_metadata()
        # a dictionary of dataframes that store all support information






    def __initialize_liquidity_assumptions(self):

        liquidity_assumptions_obj = [input_scenario for input_scenario in self.scenarioMaster.inputScenarios if input_scenario.module == 'liquidity_assumptions'][0]
        liquidity_assumptions_scenario = liquidity_assumptions_obj.scenario
        liquidity_assumptions_version = liquidity_assumptions_obj.version
        # method to call database and get financials information for EBITDA and Capex
        scenario_assumptions_df = dbLiquidity.get_scenario_assumptions(self.portfolio, liquidity_assumptions_scenario, liquidity_assumptions_version)
        return scenario_assumptions_df


    def __initialize_interest_rate(self):
        libor_scenario = [ input_scenario for input_scenario in self.scenarioMaster.inputScenarios if input_scenario.module == 'interest_rate' ][0].scenario
        libor_version = [ input_scenario for input_scenario in self.scenarioMaster.inputScenarios if input_scenario.module == 'interest_rate' ][0].version
        libor_df = dbLiquidity.get_curves(libor_scenario, libor_version)
        return libor_df



    def __initialize_captital_structure(self):
        # 1. capital structure
        capital_structure = [input_scenario for input_scenario in self.scenarioMaster.inputScenarios if input_scenario.module == 'cap_structure'][0]
        capital_structure_scenario = capital_structure.scenario
        capital_structure_version = capital_structure.version

        capital_structure_df = dbLiquidity.get_capital_structure(self.portfolio, capital_structure_scenario, capital_structure_version)

        """ for each component the instrument id field should be called label so to be able to get the distinct components """

        unique_components_df = capital_structure_df.loc[capital_structure_df.field_name=='label']

        capital_structure = []
        for index, row in unique_components_df.iterrows():

            component = row['capital_component']
            component_cap_structure_df = capital_structure_df.loc[capital_structure_df.capital_component==component].copy()



            if component == 'Revolver':
                credit_line = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'credit_line'].iloc[0]['value'])
                min_cash_reserve_revolver = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'min_cash_reserve_revolver'].iloc[0]['value'])

                """ multiple margin records """
                margins = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'margin'][['value','effective_start_date', 'effective_end_date']].values.tolist()
                index = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'index'].iloc[0]['value']
                instrument_id = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'label'].iloc[0]['value']
                issue_date = get_date_obj_from_str(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'issue_date'].iloc[0]['value'])
                maturity_date = get_date_obj_from_str(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'maturity_date'].iloc[0]['value'])
                term = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'term'].iloc[0]['value'])
                initial_balance = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'initial_balance'].iloc[0]['value'])
                interest_start_date = get_date_obj_from_str(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'interest_start_date'].iloc[0]['value'])
                amort_start_date = date(1900,1,1)
                periodicity_months = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'periodicity_months'].iloc[0]['value'])
                annual_scheduled_amort = 0
                day_count = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'day_count'].iloc[0]['value']
                min_cash_reserve_prepay = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'min_cash_reserve_revolver'].iloc[0]['value']
                sweep_percent=1
                dsra_months=0
                oids=[]
                dfcs=[]
                oid_payments={}
                dfc_payments={}
                upsizes={}
                prepays={}
                effective_interest_rates={}
                interest_payments={}
                required_dsras={}
                dsra_cash_movement={}
                amortizations={}
                principal_balances ={}
                flag_prepayable=True
                flag_historicals = True if component_cap_structure_df.loc[component_cap_structure_df.field_name == 'flag_historicals'].iloc[0]['value'] == 'TRUE' else False

                revolver = Revolver(credit_line,
                                    min_cash_reserve_revolver,
                                    margins,
                                    index,
                                    instrument_id,
                                    issue_date,
                                    maturity_date,
                                    term,
                                    initial_balance,
                                    interest_start_date,
                                    amort_start_date,
                                    periodicity_months,
                                    annual_scheduled_amort,
                                    min_cash_reserve_prepay,
                                    day_count,
                                    sweep_percent,
                                    dsra_months,
                                    oids,
                                    dfcs,
                                    oid_payments,
                                    dfc_payments,
                                    upsizes,
                                    prepays,
                                    effective_interest_rates,
                                    interest_payments,
                                    required_dsras,
                                    dsra_cash_movement,
                                    amortizations,
                                    principal_balances,
                                    flag_prepayable,
                                    flag_historicals)
                revolver.set_historical_revolver_change(self.scenarioMaster.forecastStartMonth)
                revolver.set_projected_revolver_change(self.scenarioMaster.forecastStartMonth, self.assumptions)
                """ revolver has no floor on interest rates """
                revolver.set_effective_interest_rates(self.interestRates, self.scenarioMaster.forecastStartMonth)
                capital_structure.append(revolver)




            if component in ['TLB', 'TLC']:
                margins = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'margin'][['value','effective_start_date', 'effective_end_date']].values.tolist()
                index = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'index']['value'].iloc[0]
                instrument_id = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'label'].iloc[0]['value']
                issue_date = get_date_obj_from_str(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'issue_date'].iloc[0]['value'])
                maturity_date = get_date_obj_from_str(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'maturity_date'].iloc[0]['value'])
                term = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'term'].iloc[0]['value'])
                initial_balance = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'initial_balance'].iloc[0]['value'])
                interest_start_date = get_date_obj_from_str(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'interest_date_start'].iloc[0]['value'])
                amort_start_date = get_date_obj_from_str(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'amort_date_start'].iloc[0]['value'])
                periodicity_months = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'periodicity_months'].iloc[0]['value'])
                annual_scheduled_amort = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'annual_schedule_amort'].iloc[0]['value'])
                annual_scheduled_amort_type = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'annual_schedule_amort'].iloc[0]['value_type']
                if annual_scheduled_amort_type == 'percentage':
                    annual_scheduled_amort = annual_scheduled_amort / 100

                day_count = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'day_count'].iloc[0]['value']
                sweep_percent = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'sweep_percent'].iloc[0]['value'])
                dsra_months = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'dsra_months'].iloc[0]['value'])
                oids = []      # a list of OID objects OID(balance, begin_date, end_date, oid_discount)
                dfcs = []      # a list of DFC objects DFC(balance, begin_date, end_date, oid_discount)
                oid_payments = {}
                dfc_payments = {}
                upsizes = {}
                prepays = {}
                effective_interest_rates = {}
                interest_payments = {}
                required_dsras = {}
                dsra_cash_movement = {}
                amortizations = {}
                principal_balances = {}
                flag_prepayable = True if component_cap_structure_df.loc[component_cap_structure_df.field_name == 'flag_prepayable'].iloc[0]['value'] == 'TRUE' else False
                flag_historicals = True if component_cap_structure_df.loc[component_cap_structure_df.field_name == 'flag_historicals'].iloc[0]['value'] == 'TRUE' else False
                min_cash_reserve_prepay = 0
                flag_dsra_fund_by_lc = True if component_cap_structure_df.loc[component_cap_structure_df.field_name == 'flag_dsra_fund_by_lc'].iloc[0]['value'] == 'TRUE' else False
                if flag_prepayable:
                    min_cash_reserve_prepay = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'prepay_min_cash_reserve'].iloc[0]['value'])


                if component_cap_structure_df.loc[component_cap_structure_df.field_name == 'class'].iloc[0]['value'] == 'FloatingDebt':
                    floating_debt = FloatingDebt(margins,    # only floating debt has margin
                                                 index,     # only floating debt has index
                                                 instrument_id,
                                                 issue_date,
                                                 maturity_date,
                                                 term,
                                                 initial_balance,
                                                 interest_start_date,
                                                 amort_start_date,
                                                 periodicity_months,
                                                 annual_scheduled_amort,
                                                 min_cash_reserve_prepay,
                                                 day_count,
                                                 sweep_percent,
                                                 dsra_months,
                                                 oids,     # a list of OID objects
                                                 dfcs,     # a list of DFC objects
                                                 oid_payments,
                                                 dfc_payments,
                                                 upsizes,
                                                 prepays,
                                                 effective_interest_rates,
                                                 interest_payments,
                                                 required_dsras,
                                                 dsra_cash_movement,
                                                 amortizations,
                                                 principal_balances,
                                                 flag_prepayable,
                                                 flag_historicals)

                    debt_activity_df = floating_debt.set_historical_size_change(self.scenarioMaster.forecastStartMonth)[2]
                    floating_debt.set_historical_interest_payments(self.scenarioMaster.forecastStartMonth, debt_activity_df)
                    floating_debt.set_historical_amortization(self.scenarioMaster.forecastStartMonth, debt_activity_df)
                    floating_debt.set_effective_interest_rates(self.interestRates, self.scenarioMaster.forecastStartMonth, floor=0.01)
                    capital_structure.append(floating_debt)
                if component_cap_structure_df.loc[component_cap_structure_df.field_name == 'class'].iloc[0]['value'] == 'FixedDebt':
                    """ for fixeddebt, the interest is a fixed constant """
                    fixed_rate = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'fixed_rate']['value'].iloc[0]
                    fixed_rate = float(fixed_rate)
                    fixed_debt = FixedDebt(fixed_rate,
                                           instrument_id,
                                           issue_date,
                                           maturity_date,
                                           term,
                                           initial_balance,
                                           interest_start_date,
                                           amort_start_date,
                                           periodicity_months,
                                           annual_scheduled_amort,
                                           min_cash_reserve_prepay,
                                           day_count,
                                           sweep_percent,
                                           dsra_months,
                                           oids,     # a list of OID objects
                                           dfcs,     # a list of DFC objects
                                           oid_payments,
                                           dfc_payments,
                                           upsizes,
                                           prepays,
                                           effective_interest_rates,
                                           interest_payments,
                                           required_dsras,
                                           dsra_cash_movement,
                                           amortizations,
                                           principal_balances,
                                           flag_prepayable,
                                           flag_historicals,
                                           flag_dsra_fund_by_lc)

                    debt_activity_df = fixed_debt.set_historical_size_change(self.scenarioMaster.forecastStartMonth)[2]
                    fixed_debt.set_historical_interest_payments(self.scenarioMaster.forecastStartMonth, debt_activity_df)
                    fixed_debt.set_historical_amortization(self.scenarioMaster.forecastStartMonth, debt_activity_df)
                    fixed_debt.set_effective_interest_rates()
                    fixed_debt.build_principal_balances()
                    capital_structure.append(fixed_debt)

            if component in ['OpCo']:
                financials_scenario_obj = [input_scenario for input_scenario in self.scenarioMaster.inputScenarios if input_scenario.module == 'financials'][0]
                financials_scenario = financials_scenario_obj.scenario
                financials_version = financials_scenario_obj.version
                financials_table = financials_scenario_obj.table

                working_capital={}
                other_cash_use={}
                liquidity_assumptions_df = self.assumptions
                # potentially needs a data type conversion here
                working_capital = liquidity_assumptions_df.loc[liquidity_assumptions_df.account=='Change In Working Capital'][['date_end','value']].set_index('date_end')['value'].to_dict()
                other_cash_use = liquidity_assumptions_df.loc[liquidity_assumptions_df.account=='Other Cash Use'][['date_end','value']].set_index('date_end')['value'].to_dict()

                opco = OperatingCompany(self.portfolio,
                                        financials_scenario,
                                        financials_version,
                                        financials_table,
                                        ebitda={},
                                        capex={},
                                        working_capital=working_capital,
                                        other_cash_use=other_cash_use)
                capital_structure.append(opco)

            if component in ['TaxRegister']:

                effective_tax_rate = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'effective_tax_rate'].iloc[0]['value']
                tax_split_ratio_list = component_cap_structure_df.loc[component_cap_structure_df.field_name.str.contains('tax_split')][['field_name', 'value']].values.tolist()
                tax_split_ratio_list.sort(key = lambda x: x[0])
                tax_split_ratio_list = [float(item[1]) for item in tax_split_ratio_list]
                tax_register =  TaxRegister(self.portfolio, effective_tax_rate=float(effective_tax_rate), tax_split_ratio=tax_split_ratio_list, paid_tax={})
                capital_structure.append(tax_register)

            if 'swap' in component.lower():
                instrument_id = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'instrument_id'].iloc[0]['value']
                trade_date = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'trade_date'].iloc[0]['value']
                counterparty = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'counterparty'].iloc[0]['value']
                index = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'index'].iloc[0]['value']
                swap_rates = [[]]
                swap = Swap(self.portfolio, instrument_id, index, trade_date, counterparty, swap_rates)
                swap.get_swap_rates_from_db()
                capital_structure.append(swap)

            if component in ['Equity']:
                purchase_price = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'purchase_price'].iloc[0]['value']
                purchase_price = float(purchase_price)
                debt_percentage = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'debt_percentage'].iloc[0]['value']
                debt_percentage_value_type = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'debt_percentage'].iloc[0]['value_type']
                if debt_percentage_value_type == 'percentage':
                    debt_percentage = float(debt_percentage) / 100
                name = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'name'].iloc[0]['value']
                exit_multiple = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'exit_multiple'].iloc[0]['value']
                exit_multiple = float(exit_multiple)
                irr_frequency = component_cap_structure_df.loc[component_cap_structure_df.field_name == 'irr_frequency'].iloc[0]['value']
                exit_time = dateUtils.get_date_obj_from_str(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'exit_time'].iloc[0]['value'])
                periodicity_months = float(component_cap_structure_df.loc[component_cap_structure_df.field_name == 'periodicity_months'].iloc[0]['value'])
                equity_component = Equity(name, purchase_price, debt_percentage, exit_multiple, irr_frequency, exit_time, periodicity_months)


                capital_structure.append(equity_component)




        self.capitalStructure = capital_structure


        return capital_structure

    # # 2. waterfall
    # waterfall = [input_scenario for input_scenario in self.ScenarioMaster.inputScenarios.scenario if input_scenario.module == 'waterfall'][0]
    # waterfall_scenario = waterfall.scenario
    # waterfall_version = waterfall.version
    def __initialize_waterfall(self):
        waterfall_scenario = [input_scenario for input_scenario in self.scenarioMaster.inputScenarios if input_scenario.module == 'waterfall'][0].scenario
        waterfall_version = [input_scenario for input_scenario in self.scenarioMaster.inputScenarios if input_scenario.module == 'waterfall'][0].version
        waterfall_df = dbLiquidity.get_waterfall(self.portfolio, waterfall_scenario, waterfall_version)
        waterfall_df = waterfall_df.sort_values(['level','sub_level'], ascending=[True, True])
        return waterfall_df

    def __initialize_metadata(self):

        sorted_months_index = list(sorted((set(self.scenarioMaster.actualMonths + self.scenarioMaster.forecastMonths))))
        # waterfall_df = self.waterfall.copy()
        # waterfall_df = waterfall_df.sort_values(["level", "sub_level"], ascending=(True, True))
        # waterfall_df['instrument_name'] = waterfall_df['instrument'] + " - " + waterfall_df['item']
        # all_instrument_names = list(waterfall_df['instrument_name'])
        # all_instrument_names.insert(0, 'Beginning Cash Balance')
        # all_instrument_names.insert(len(all_instrument_names), 'Ending Cash Balance')

        cashflow_df = pd.DataFrame(index=sorted_months_index, columns=['Beginning Cash Balance'], data=0)

        # metadata_df.to_csv("metadata_df.csv")
        # metadata_df.T.to_csv("metadata_df_T.csv")
        return {'cashflow': cashflow_df}



    def set_cashflow_with_waterfall(self):
        level = 1
        max_level = self.waterfall.level.max()


        while level <= max_level:
            level_related_component = self.waterfall.loc[self.waterfall.level == level]
            sub_level = 1
            max_sub_level = level_related_component.sub_level.max()
            while sub_level <= max_sub_level:
                selected_sub_level_component_df = level_related_component.loc[level_related_component.sub_level == sub_level]
                for index, selected_sub_level_component in selected_sub_level_component_df.iterrows():
                    """ Operating Company related components """

                    if selected_sub_level_component.instrument == 'OpCo':
                        operating_company = [capital_component for capital_component in self.capitalStructure if isinstance(capital_component, OperatingCompany)][0]
                        if selected_sub_level_component['item'].upper() == 'EBITDA':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(operating_company.ebitda)
                        if selected_sub_level_component['item'].upper() == 'Capex'.upper():
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(operating_company.capex)
                        if selected_sub_level_component['item'].lower() == 'working capital':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(operating_company.workingCapital)
                        if selected_sub_level_component['item'].lower() == 'other cash use':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(operating_company.otherCashUse)
                        if selected_sub_level_component['item'].lower() == 'cfo':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(operating_company.build_cfo())

                    """ Revolver related components """
                    if selected_sub_level_component.instrument == 'Revolver':
                        revolver = [capital_component for capital_component in self.capitalStructure if isinstance(capital_component, Revolver)][0]
                        if selected_sub_level_component['item'].lower() == 'draw':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(revolver.upsizes)
                        if selected_sub_level_component['item'].lower() == 'repay':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = - pd.Series(revolver.prepays)
                        if selected_sub_level_component['item'].lower() == 'interest expense':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = - pd.Series(revolver.interestPayments)


                    """ Term Loan related components """
                    if selected_sub_level_component.instrument in ['TLB', 'TLC']:
                        tl_obj = [capital_component for capital_component in self.capitalStructure if isinstance(capital_component, Debt) and capital_component.instrumentID == self.portfolio + " " + selected_sub_level_component.instrument][0]

                        if selected_sub_level_component['item'].lower() == 'interest expense':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(tl_obj.interestPayments)

                        if selected_sub_level_component['item'].lower() == 'amortization':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(tl_obj.amortizations)

                        if selected_sub_level_component['item'].lower() == 'dsra release':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = 0.0

                        if selected_sub_level_component['item'].lower() == 'prepayment':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = - pd.Series(tl_obj.prepays)

                        if selected_sub_level_component['item'].lower() == 'upsize':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(tl_obj.upsizes)

                    """ Portfolio level """
                    if selected_sub_level_component.instrument in ['Portfolio']:
                        """ permitted tax distribution """
                        if selected_sub_level_component['item'].lower() == 'ptd':
                            distributions = dbLiquidity.get_distributions(self.portfolio)
                            distributions = [item for item in distributions if item < self.scenarioMaster.forecastStartMonth]
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = pd.Series(distributions)

                    if selected_sub_level_component.instrument in ['Equity']:
                        if selected_sub_level_component['item'].lower() == 'sweep':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = 0.0

                    if selected_sub_level_component.instrument in ['Swap']:
                        if selected_sub_level_component['item'].lower() == 'interest expense':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = 0.0

                    if selected_sub_level_component.instrument in ['TaxRegister']:
                        if selected_sub_level_component['item'].lower() == 'ptd':
                            self.metadata['cashflow'][selected_sub_level_component.instrument + " - " + selected_sub_level_component['item'].lower()] = 0.0

                sub_level += 1
            level += 1


    """ analyze liquidity method has more customizations for existing portfolio """
    def analyze_liquidity(self):
        """ step 1, build initial cash balances """
        cash_balances_df = dbLiquidity.get_cash_balance(self.portfolio, self.scenarioMaster.forecastStartMonth)
        self.__build_beginning_cash(cash_balances_df)

        """ step 2, build debt related components """
        debt_components = [item for item in self.capitalStructure if isinstance(item, FloatingDebt)]
        for debt_item in debt_components:
            """ step 2.1 build balance """
            debt_item.build_principal_balances()

        """ step 3, build swap related components """
        swap_components = [item for item in self.capitalStructure if isinstance(item, Swap)]

        total_swap_interest_payment_df = pd.DataFrame()
        total_swap_detail_df = pd.DataFrame()
        for swap_item in swap_components:
            """ step 3.1  """
            swap_item.build_swap_interest_payments(self.interestRates)
            start_month = self.metadata['cashflow'].index.min()
            end_month = self.metadata['cashflow'].index.max()
            swap_item_interest_payment_df = swap_item.build_swap_payments_by_month(start_month, end_month)

            # swap_item_interest_payment_df.to_csv(swap_item.instrumentID + "_swap_detail.csv")
            pd.DataFrame(swap_item.swapRates).to_csv(swap_item.instrumentID + "_swap_raw_detail.csv")


            total_swap_interest_payment_df = total_swap_interest_payment_df.append(swap_item_interest_payment_df)

            swap_item_raw_detail_df = pd.DataFrame(data=swap_item.swapRates, columns=['date_fix_rate', 'date_start', 'date_end', 'notional', 'fix_rate', 'floating_rate', 'number_of_days', 'swap_per_day'])
            swap_item_raw_detail_df['swap_instrument_id'] = swap_item.instrumentID
            total_swap_detail_df = total_swap_detail_df.append(swap_item_raw_detail_df)

        self.metadata['swap_payment'] = total_swap_interest_payment_df
        self.metadata['swap_detail'] = total_swap_detail_df

        pivot_swap_interest_payment_df = pd.pivot_table(total_swap_interest_payment_df, values='total_interest_payment', columns='instrument_id', index='month_end', aggfunc=np.sum)
        pivot_swap_interest_payment_df.fillna(value={'total_interest_payment':0.0}, inplace=True)
        pivot_swap_interest_payment_df['Swap - interest expense'] = - pivot_swap_interest_payment_df[list(pivot_swap_interest_payment_df.columns)].sum(axis=1)

        pivot_swap_interest_payment_df = pivot_swap_interest_payment_df.loc[pivot_swap_interest_payment_df.index >= self.scenarioMaster.forecastStartMonth]
        self.metadata['cashflow']['Swap - interest expense'] = pd.Series(pivot_swap_interest_payment_df['Swap - interest expense'])


        """ step 4, build ptd related components """
        opco = [item for item in self.capitalStructure if isinstance(item, OperatingCompany)][0]
        entity_capex_df = opco.get_entity_capex()
        entity_list = list(set(entity_capex_df.entity))
        additional_capex_dict = {}
        entity_capex_df['year'] = entity_capex_df.apply(lambda row: row['period'].year, axis=1)
        for entity in entity_list:
            additional_capex_dict[entity] = entity_capex_df.loc[entity_capex_df.entity == entity].groupby(['year'])['value'].agg(sum).to_dict()

        year_range = list(range(self.metadata['cashflow'].index.min().year, self.metadata['cashflow'].index.max().year+1))
        for year in year_range:
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)
            """ hardcoded for now """
            total_oid = 5201004
            total_dfc = 9535996

            total_ebitda = self.metadata['cashflow'][start_date:end_date]['OpCo - ebitda'].sum()


            # additional_capex_dict = {'Gavin':{2020:55145000, 2021:111111111}, .... }

            ptd_schedule = self.__build_ptd(year, total_oid, total_ebitda, total_dfc, additional_capex_dict, total_interest_expense=None)

            ptd_period_list = [date(year, 3, 31), date(year, 6, 30), date(year, 9, 30), date(year, 12, 31)]

            for ptd_period in ptd_period_list:
                self.metadata['cashflow'].at[ptd_period, 'TaxRegister - ptd'] = - ptd_schedule[ptd_period_list.index(ptd_period)]


        """ step 5, do cash waterfall month over month staring the forecast phase """
        cash_waterfall_forecast_months = self.scenarioMaster.forecastMonths

        for forecast_month in cash_waterfall_forecast_months:
            beginning_cash = self.metadata['cashflow'].loc[forecast_month]['Beginning Cash Balance']
            if str(beginning_cash) == 'nan':
                beginning_cash = 0.0

            cashflow_for_period = beginning_cash
            """ waterfall is ordered by level and sub level """
            for index, waterfall_item in self.waterfall.iterrows():
                """ 3 key configurable variables to determine how to react on the cash """
                instrument = waterfall_item['instrument']
                item = waterfall_item['item']
                method = waterfall_item['method']
                direction = waterfall_item['direction']
                direction_sign = 1 if direction == 'inflow' else -1

                """ OpCo items already set from initialization phase """
                if instrument == 'OpCo':
                    cashflow_item_value = self.metadata['cashflow'].loc[forecast_month][instrument + " - " + item.lower()] if str(self.metadata['cashflow'].loc[forecast_month][instrument + " - " + item.lower()]) != 'nan' else 0.0
                    cashflow_for_period += cashflow_item_value

                if instrument == 'Revolver':
                    revolver_obj = [item for item in self.capitalStructure if isinstance(item, Revolver)][0]

                    if item == 'interest expense':
                        revolver_interest_expense = revolver_obj.calculate_interest_expense(forecast_month)
                        cashflow_for_period += revolver_interest_expense * direction_sign
                        self.metadata['cashflow'].at[forecast_month, instrument + ' - ' + item.lower()] = revolver_interest_expense * direction_sign

                    if item in ['draw','repay']:
                        """ for now we use manual revolver adjustment """
                        """ revolver draw or repay balances will be set on the initialization phase """
                        cashflow_item_value = self.metadata['cashflow'].loc[forecast_month][instrument + " - " + item.lower()] if str(self.metadata['cashflow'].loc[forecast_month][instrument + " - " + item.lower()]) != 'nan' else 0.0
                        cashflow_for_period += cashflow_item_value
                        continue


                if instrument in ['Swap']:
                    if item == 'interest expense':
                        cashflow_item_value = self.metadata['cashflow'].loc[forecast_month][instrument + " - " + item.lower()] if str(self.metadata['cashflow'].loc[forecast_month][instrument + " - " + item.lower()]) != 'nan' else 0.0
                        cashflow_for_period += cashflow_item_value
                        continue

                if instrument == 'TaxRegister':

                    if item == 'ptd':
                        cashflow_item_value = self.metadata['cashflow'].loc[forecast_month][instrument + " - " + item.lower()] if str(self.metadata['cashflow'].loc[forecast_month][instrument + " - " + item.lower()]) != 'nan' else 0.0
                        cashflow_for_period += cashflow_item_value
                        continue



                if instrument in ['TLB', 'TLC']:
                    tl_obj = [item for item in self.capitalStructure if isinstance(item, FloatingDebt) and item.instrumentID == self.portfolio + " " + instrument][0]

                    if item == 'interest expense':
                        tl_interest_expense = tl_obj.calculate_interest_expense(forecast_month)
                        cashflow_for_period += tl_interest_expense * direction_sign
                        self.metadata['cashflow'].at[forecast_month, instrument + ' - ' + item.lower()] = tl_interest_expense * direction_sign

                    if item == 'prepayment':
                        flag_prepayable = tl_obj.flagPrepayable
                        periodicity_months =tl_obj.periodicityMonths
                        prepayment = 0
                        if flag_prepayable and forecast_month.month % periodicity_months == 0:
                            min_cash_reserve = tl_obj.minCashReservePrepay
                            prepayment = max([0, cashflow_for_period - min_cash_reserve])
                            self.metadata['cashflow'].at[forecast_month, instrument + ' - ' + item.lower()] = prepayment * direction_sign
                            tl_obj.prepay_debt(forecast_month, self.scenarioMaster.forecastStartMonth, prepayment)
                        else:
                            """ for debt which is not prepayable or not in periodicity, just present 0s for prepayment even with excess cash """
                            self.metadata['cashflow'].at[forecast_month, instrument + ' - ' + item.lower()] = 0.0
                            continue
                        cashflow_for_period += prepayment * direction_sign





            next_month_end = dateUtils.get_one_month_later(forecast_month)
            if next_month_end <= self.metadata['cashflow'].index.max():
                self.metadata['cashflow'].at[next_month_end, 'Beginning Cash Balance'] = cashflow_for_period



        revolver = [ item for item in self.capitalStructure if isinstance(item, Revolver)][0]

        # pd.DataFrame.from_dict(revolver.interestPayments, orient='index').to_csv("revolver_balances.csv")

        tlb = [ item for item in self.capitalStructure if isinstance(item, FloatingDebt) and item.instrumentID == 'Lightstone TLB'][0]
        tlb_df = pd.DataFrame.from_dict(tlb.principalBalances, columns=['balance'], orient='index')
        tlb_df['upsize'] = pd.Series(tlb.upsizes)
        tlb_df['prepay'] = pd.Series(tlb.prepays)
        tlb_df['margin'] = 0.0375
        tlb_df['floating_rate'] = pd.Series(tlb.effectiveInterestRates)
        tlb_df['floating_rate'] = tlb_df['floating_rate'] - tlb_df['margin']
        tlb_df['interest_payments'] = pd.Series(tlb.interestPayments)
        tlb_df = tlb_df.T
        tlb_df.to_csv("tlb.csv")

        tlc = [ item for item in self.capitalStructure if isinstance(item, FloatingDebt) and item.instrumentID == 'Lightstone TLC'][0]
        tlc_df = pd.DataFrame.from_dict(tlc.principalBalances, columns=['balance'], orient='index')
        tlc_df['upsize'] = pd.Series(tlc.upsizes)
        tlc_df['prepay'] = pd.Series(tlc.prepays)
        tlc_df['margin'] = 0.0375
        tlc_df['floating_rate'] = pd.Series(tlc.effectiveInterestRates)
        tlc_df['floating_rate'] = tlc_df['floating_rate'] - tlc_df['margin']
        tlc_df['interest_payments'] = pd.Series(tlc.interestPayments)
        tlc_df = tlc_df.T
        # tlc_df.to_csv("tlc.csv")


    """ when we do lbo analysis, we usually do not need too much customizations, we only need assumptions """
    def analyze_leverage_buyout(self):

        """ step 1, get beginning cash if available """
        cash_balances_df = dbLiquidity.get_cash_balance(self.portfolio, self.scenarioMaster.forecastStartMonth)
        if len(cash_balances_df) > 0:
            self.__build_beginning_cash(cash_balances_df)
        else:
            self.metadata['cashflow']['Beginning Cash Balance'] = 0.0

        """ step 2, follow the order of cash waterfall """

        monthly_cashflow = 0.0
        for index, row in self.metadata['cashflow'].iterrows():
            for cash_item_title in self.metadata['cashflow'].columns:
                if cash_item_title == 'Beginning Cash Balance':
                    if index.year == self.scenarioMaster.startYear and index.month == 1:
                        monthly_cashflow = row[cash_item_title]
                    else:
                        self.metadata['cashflow'].at[index, cash_item_title] = monthly_cashflow
                    continue

                object = cash_item_title.split(" - ")[0]
                object_item = cash_item_title.split(" - ")[1]
                if object == 'OpCo':
                    monthly_cashflow += row[cash_item_title]

                if object == 'TLB':
                    tlb_obj = [item for item in self.capitalStructure if isinstance(item, Debt) and item.instrumentID == self.portfolio + " TLB"][0]

                    if object_item.lower() == 'interest expense':
                        required_interest_payment = tlb_obj.calculate_interest_expense(index)
                        self.metadata['cashflow'].at[index, cash_item_title] = -required_interest_payment
                        monthly_cashflow -= required_interest_payment


                    if object_item.lower() == 'amortization':
                        required_amort = tlb_obj.calculate_amortization(index)
                        self.metadata['cashflow'].at[index, cash_item_title] = -required_amort
                        monthly_cashflow -= required_amort



                    if object_item.lower() == 'prepayment':
                        available_cash = monthly_cashflow
                        prepayment = tlb_obj.calculate_prepayment(index, available_cash)
                        self.metadata['cashflow'].at[index, cash_item_title] = -prepayment
                        monthly_cashflow -= prepayment


                if object == 'Equity':
                    equity_obj = [item for item in self.capitalStructure if isinstance(item, Equity)][0]

                    if object_item.lower() == 'sweep':
                        equity_sweep = equity_obj.calculate_equity_sweep(index, monthly_cashflow)
                        self.metadata['cashflow'].at[index, cash_item_title] = -equity_sweep
                        monthly_cashflow -= equity_sweep

        """ exit phase analysis """
        equity_obj = [item for item in self.capitalStructure if isinstance(item, Equity)][0]
        exit_time = equity_obj.exitTime

        tl_objects = [item for item in self.capitalStructure if isinstance(item, Debt)]
        ending_debt_balances = sum([item.principalBalances[exit_time] for item in tl_objects])

        tlb_object = tl_objects[0]


        last_tweleve_months_start_date = dateUtils.get_months_shift_date(exit_time, -11)
        last_tweleve_months_ebitda = self.metadata['cashflow'].loc[last_tweleve_months_start_date:exit_time]['OpCo - ebitda'].sum()
        equity_exit_value = equity_obj.calculate_exit_value(last_tweleve_months_ebitda)

        equity_annual_cashflow_list, irr, moic = equity_obj.calculate_irr_and_moic(self.metadata['cashflow'][['Equity - sweep']], equity_exit_value - ending_debt_balances)

        print (irr)
        print ("first")
        print ([item/1000000.0 for item in equity_annual_cashflow_list], irr, moic)
        print ("------------------------------------------------")
        return equity_annual_cashflow_list, irr, moic







    """ A key function here to solve for a purchase price with targetted IRR """
    def solve_purchase_price_by_irr(self, targeted_irr):
        data = (targeted_irr, self)
        equity_obj = [item for item in self.capitalStructure if isinstance(item, Equity)][0]
        purchase_price = 1
        result_purchase_price = fsolve(self.solver_purchase_price, x0=purchase_price, args=data, factor=100, xtol=0.000001)
        print ("result: ", str(result_purchase_price[0]))



    """ solver for purchase price """

    @staticmethod
    def solver_purchase_price(purchase_price, *args):
        targeted_irr, liquidity_obj = args

        """ exit phase analysis """
        equity_obj = [item for item in liquidity_obj.capitalStructure if isinstance(item, Equity)][0]
        """ reset equity purchase price! """
        equity_obj.purchasePrice = purchase_price[0]

        tl_obj = [item for item in liquidity_obj.capitalStructure if isinstance(item, Debt)][0]
        tl_obj.initialBalance = purchase_price[0] * equity_obj.debtPercentage
        tl_obj.prepays = {}
        tl_obj.amortizations = {}
        tl_obj.upsizes = {}
        tl_obj.build_principal_balances()



        """ step 1, get beginning cash if available """
        cash_balances_df = dbLiquidity.get_cash_balance(liquidity_obj.portfolio, liquidity_obj.scenarioMaster.forecastStartMonth)
        if len(cash_balances_df) > 0:
            liquidity_obj.__build_beginning_cash(cash_balances_df)
        else:
            liquidity_obj.metadata['cashflow']['Beginning Cash Balance'] = 0.0

        """ step 2, follow the order of cash waterfall """

        monthly_cashflow = 0.0
        for index, row in liquidity_obj.metadata['cashflow'].iterrows():
            for cash_item_title in liquidity_obj.metadata['cashflow'].columns:
                if cash_item_title == 'Beginning Cash Balance':
                    if index.year == liquidity_obj.scenarioMaster.startYear and index.month == 1:
                        monthly_cashflow = row[cash_item_title]
                    else:
                        liquidity_obj.metadata['cashflow'].at[index, cash_item_title] = monthly_cashflow
                    continue

                object = cash_item_title.split(" - ")[0]
                object_item = cash_item_title.split(" - ")[1]
                if object == 'OpCo':
                    monthly_cashflow += row[cash_item_title]

                if object == 'TLB':
                    tlb_obj = [item for item in liquidity_obj.capitalStructure if isinstance(item, Debt) and item.instrumentID == liquidity_obj.portfolio + " TLB"][0]


                    if object_item.lower() == 'interest expense':
                        required_interest_payment = tlb_obj.calculate_interest_expense(index)
                        liquidity_obj.metadata['cashflow'].at[index, cash_item_title] = -required_interest_payment

                        monthly_cashflow -= required_interest_payment


                    if object_item.lower() == 'amortization':
                        required_amort = tlb_obj.calculate_amortization(index)
                        liquidity_obj.metadata['cashflow'].at[index, cash_item_title] = -required_amort
                        monthly_cashflow -= required_amort



                    if object_item.lower() == 'prepayment':
                        available_cash = monthly_cashflow
                        prepayment = tlb_obj.calculate_prepayment(index, available_cash)
                        liquidity_obj.metadata['cashflow'].at[index, cash_item_title] = -prepayment
                        monthly_cashflow -= prepayment


                if object == 'Equity':
                    equity_obj = [item for item in liquidity_obj.capitalStructure if isinstance(item, Equity)][0]

                    if object_item.lower() == 'sweep':
                        equity_sweep = equity_obj.calculate_equity_sweep(index, monthly_cashflow)
                        liquidity_obj.metadata['cashflow'].at[index, cash_item_title] = -equity_sweep
                        monthly_cashflow -= equity_sweep




        exit_time = equity_obj.exitTime

        tl_objects = [item for item in liquidity_obj.capitalStructure if isinstance(item, Debt)]
        ending_debt_balances = sum([item.principalBalances[exit_time] for item in tl_objects])

        tlb_object = tl_objects[0]


        last_tweleve_months_start_date = dateUtils.get_months_shift_date(exit_time, -11)
        last_tweleve_months_ebitda = liquidity_obj.metadata['cashflow'].loc[last_tweleve_months_start_date:exit_time]['OpCo - ebitda'].sum()
        equity_exit_value = equity_obj.calculate_exit_value(last_tweleve_months_ebitda)

        equity_annual_cashflow_list, irr, moic = equity_obj.calculate_irr_and_moic(liquidity_obj.metadata['cashflow'][['Equity - sweep']], equity_exit_value - ending_debt_balances)


        print (purchase_price[0], purchase_price[0] * equity_obj.debtPercentage, [item/1000000 for item in equity_annual_cashflow_list], irr, targeted_irr - irr)

        # df = liquidity_obj.metadata['cashflow'].copy()
        # global a
        # df['tlb_balance'] = pd.Series(tlb_object.principalBalances)
        # df.to_csv(str(a) + ".csv")
        # a+=1
        return targeted_irr - irr




    def __build_beginning_cash(self, cash_balances_df):
        if len(cash_balances_df) > 0:
            cash_balances_df['begin_date'] = cash_balances_df.apply(lambda row: dateUtils.get_cash_balance_begin_date(row['as_of_date']), axis=1)

            for index, row in self.metadata['cashflow'].iterrows():
                if len(cash_balances_df.loc[cash_balances_df.begin_date == index]) > 0:
                    self.metadata['cashflow'].at[index, 'Beginning Cash Balance'] = cash_balances_df.loc[cash_balances_df.begin_date == index].iloc[0]['balance']


    def __initializ_fixed_asset_depreciation(self):
        asset_df = dbLiquidity.get_asset_depreciation(self.portfolio)
        entity_list = list(set(list(asset_df.entity)))
        fixed_assets_obj_list = []
        for entity in entity_list:
            entity_asset_df = asset_df.loc[asset_df.entity==entity]
            entity_name = entity
            depreciation_method = entity_asset_df.iloc[0]['depreciation_method']
            depreciation_term = entity_asset_df.iloc[0]['depreciation_term']
            in_service_year = entity_asset_df.iloc[0]['in_service_year']
            initial_purchase_price = entity_asset_df.loc[entity_asset_df.type == 'Purchase Price'].iloc[0]['value']

            capex_df = entity_asset_df[entity_asset_df.type=='Capex']
            grouped_capex_df = capex_df.groupby('in_service_year')['value'].sum()
            capex_dict = grouped_capex_df.to_dict()

            depreciation_adjustment_df = entity_asset_df[entity_asset_df.type.isin(['Disposal'])]
            grouped_depreciation_adjustment_df = depreciation_adjustment_df.groupby('in_service_year')['value'].sum()
            depreciation_adjustment_dict = grouped_depreciation_adjustment_df.to_dict()

            fixed_asset_obj = FixedAsset(self.portfolio, entity_name, depreciation_method, depreciation_term, in_service_year, initial_purchase_price, capex=capex_dict, depreciation_adjustment=depreciation_adjustment_dict)
            fixed_assets_obj_list.append(fixed_asset_obj)

        return fixed_assets_obj_list


    """ build up of PTD is differantiated between different portfolios, so this function has to provide different implementation """
    def __build_ptd(self, year, total_oid, total_ebitda, total_dfc, additional_capex_dict, total_interest_expense=None):
        """ step 1, get self tax register for the information like rate and split """
        tax_register_list = [item for item in self.capitalStructure if isinstance(item, TaxRegister)]
        fixed_assets_obj_list = self.fixedAssets
        if self.portfolio == 'Lightstone':
            tax_register = tax_register_list[0]
            tax_register.get_paid_tax_from_db(self.scenarioMaster.forecastStartMonth)
            # year = 2020
            # total_interest_expense = 112547000
            # total_oid = 5201000
            # total_ebitda = 214107000
            # total_dfc = 9536000
            # additional_capex_dict = {'Gavin':{2020:55145000, 2021:111111111}, .... }


            total_tax_depreciation = 0



            for fixed_asset in fixed_assets_obj_list:
                additional_capex = additional_capex_dict[fixed_asset.entityName] if fixed_asset.entityName in additional_capex_dict else {}
                total_tax_depreciation += fixed_asset.calcualte_tax_depreciation(additional_capex, year)




            ptd_schedule = tax_register.calculate_tax_payment(year, total_oid, total_ebitda, total_dfc, total_tax_depreciation,  total_interest_expense)





            return ptd_schedule

    def get_financials(self):
        operating_company = [item for item in self.capitalStructure if isinstance(item, OperatingCompany)][0]
        return operating_company.get_financials()

    def output_liquidity_results(self):
        monthly_list = self.metadata['cashflow'].index.tolist()
        financials_df = self.get_financials()

        rw_headers_df = self.get_output_row_headers_fromdb()

        """ step 1, get the ebitda related fslis """

        default_row_header = rw_headers_df.sort_values(by='order')['header'].tolist()

        financials_df = financials_df.loc[(financials_df.period.isin(monthly_list)) & (financials_df.fsli.isin(default_row_header))]


        merged_financials_df = pd.merge(financials_df, rw_headers_df, how='left', left_on='fsli', right_on = 'header')

        merged_financials_df['display_value'] = merged_financials_df['value'] * merged_financials_df['display_sign']

        merged_financials_df = merged_financials_df[['fsli','period','display_value']]


        annual_financials_df = merged_financials_df.copy()
        annual_financials_df['year'] = annual_financials_df.apply(lambda row: row['period'].year, axis=1)

        pivot_financials_df = pd.pivot_table(merged_financials_df, index='fsli', values='display_value', columns='period', aggfunc='sum')

        pivot_financials_df.fillna(0.0, inplace=True)

        pivot_financials_df = pivot_financials_df.reindex(default_row_header)

        pivot_annual_financials_df = pd.pivot_table(annual_financials_df, index='fsli', values='display_value', columns='year', aggfunc='sum')

        pivot_annual_financials_df.fillna(0.0, inplace=True)

        pivot_annual_financials_df = pivot_annual_financials_df.reindex(default_row_header)

        # pivot_annual_financials_df.to_csv("pivot_annual_financials_df.csv")

        monthly_output_result_datarows = [['Financials $(mm)'] + [dateUtils.get_year_month_header(period_month) for period_month in monthly_list]]

        monthly_output_result_datarows = monthly_output_result_datarows + pivot_financials_df.reset_index().values.tolist()

        """ step 2, get the liquidity related fslis """
        cashflow_df = self.metadata['cashflow']
        cashflow_df.fillna(0.0, inplace=True)
        for column in cashflow_df.columns:
            cashflow_df[column] = cashflow_df[column] * 0.000001

        beginning_cash_datarow = cashflow_df['Beginning Cash Balance'].tolist()

        beginning_cash_datarow = ['Beginning Cash Balance'] + beginning_cash_datarow

        monthly_output_result_datarows.append([])
        monthly_output_result_datarows.append(beginning_cash_datarow)

        order_of_capital_component = ['OpCo', 'Revolver', 'TLB', 'TLC', 'Swap', 'TaxRegister', 'Equity']


        for capital_component in order_of_capital_component:
            monthly_output_result_datarows.append([capital_component])

            for column in cashflow_df.columns:
                if capital_component in column:
                    capital_component_sub_item = column.split(" - ")[1]
                    capital_component_sub_item_datarow = cashflow_df[column].values.tolist()
                    monthly_output_result_datarows.append([capital_component_sub_item] + capital_component_sub_item_datarow)

            monthly_output_result_datarows.append([])


        # monthly_output_result_datarows_df = pd.DataFrame(monthly_output_result_datarows)
        #
        # monthly_output_result_datarows_df.to_csv("monthly_output_result_datarows_df.csv")

        start_year = min(monthly_list).year
        end_year = max(monthly_list).year

        year_range = list(range(start_year, end_year+1))

        annual_output_result_datarows = [['Financials $(mm)'] + year_range]
        annual_output_result_datarows = annual_output_result_datarows + pivot_annual_financials_df.reset_index().values.tolist()
        annual_output_result_datarows.append([])
        annual_begining_cash_balance_datarow = ['Beginning Cash Balance']
        for year in year_range:
            beginning_cash = cashflow_df.loc[date(year,1,31)]['Beginning Cash Balance']
            annual_begining_cash_balance_datarow.append(beginning_cash)

        annual_output_result_datarows.append(annual_begining_cash_balance_datarow)

        for capital_component in order_of_capital_component:
            annual_output_result_datarows.append([capital_component])

            for column in cashflow_df.columns:
                if capital_component in column:
                    capital_component_sub_item = column.split(" - ")[1]
                    capital_component_sub_item_datarow = []
                    for year in year_range:
                        sub_item_value = cashflow_df.loc[date(year,1,31):date(year,12,31)][column].sum()
                        sub_item_value = 0.0 if str(sub_item_value) == 'nan' else sub_item_value
                        capital_component_sub_item_datarow.append(sub_item_value)

                    annual_output_result_datarows.append([capital_component_sub_item] + capital_component_sub_item_datarow)

            annual_output_result_datarows.append([])


        return annual_output_result_datarows, monthly_output_result_datarows



    def get_output_row_headers_fromdb(self):
        rw_headers_df = dbLiquidity.get_rw_headers()
        return rw_headers_df



class OperatingCompany:
    def __init__(self,
                 portfolio,
                 financials_scenario='',
                 financials_version='',
                 financials_table='',
                 ebitda={},
                 capex={},
                 working_capital={},
                 other_cash_use={}):

        self.portfolio = portfolio
        self.financialsScenario = financials_scenario
        self.financialsVersion = financials_version
        self.financialsTable = financials_table
        self.ebitda = ebitda
        self.capex = capex
        if financials_scenario != '' and financials_version != '':
            financials_df = self.get_financials()
            self.ebitda = financials_df.loc[financials_df.fsli=='EBITDA'].groupby(['fsli','period']).sum().reset_index()[['period','value']].set_index('period')['value'].to_dict()
            self.capex = financials_df.loc[financials_df.fsli=='Total Capex'].groupby(['fsli','period']).sum().reset_index()[['period','value']].set_index('period')['value'].to_dict()

            if 'Total Capex' not in financials_df.fsli.tolist():
                self.capex = financials_df.loc[financials_df.fsli=='Capex'].groupby(['fsli','period']).sum().reset_index()[['period','value']].set_index('period')['value'].to_dict()

            # """ flip the sign of the capex items """
            # for key, value in self.capex.items():
            #     self.capex[key] = -value

        self.workingCapital = working_capital
        self.otherCashUse = other_cash_use



    def get_financials(self):
        # method to call database and get financials information for EBITDA and Capex
        financials_df = dbLiquidity.get_financials(self.portfolio, self.financialsScenario, self.financialsVersion, self.financialsTable)
        return financials_df

    def get_entity_capex(self):
        financials_df = self.get_financials()
        entity_capex_df = financials_df.loc[financials_df.fsli=='Total Capex']
        if 'Total Capex' not in financials_df.fsli.tolist():
            entity_capex_df = financials_df.loc[financials_df.fsli=='Capex']

        return entity_capex_df


    def build_cfo(self):
        cfo = {}
        for key in self.ebitda:
            ebitda = self.ebitda[key]
            capex = 0
            if key in self.capex:
                capex = self.capex[key]
            cfo_dollar_amount = ebitda - capex
            cfo[key] = {'EBITDA': ebitda, 'CAPEX': capex, 'CFO': cfo_dollar_amount}
        return cfo






class Debt:
    def __init__(self,
                 instrument_id,
                 issue_date,
                 maturity_date,
                 term,
                 initial_balance,
                 interest_start_date,
                 amort_start_date,
                 periodicity_months,
                 annual_scheduled_amort,
                 min_cash_reserve_prepay,
                 day_count='30/360',
                 sweep_percent=1,
                 dsra_months=6,
                 oids=[],     #  a list of OID objects
                 dfcs=[],     #  a list of DFC objects
                 oid_payments={},
                 dfc_payments={},
                 upsizes={},
                 prepays={},
                 effective_interest_rates={},
                 interest_payments={},
                 required_dsras={},
                 dsra_cash_movement={},
                 amortizations={},
                 principal_balances={},
                 flag_prepayable=True,
                 flag_historicals=True,
                 flag_dsra_fund_by_lc=True):

        self.instrumentID = instrument_id   # name of this debt
        self.issueDate = issue_date
        self.maturityDate = maturity_date
        self.term = term
        self.initialBalance = initial_balance
        self.interestStartDate = interest_start_date
        self.amortStartDate = amort_start_date
        self.periodicityMonths = periodicity_months
        self.dsraMonths = dsra_months
        self.annualScheduledAmort = annual_scheduled_amort
        self.minCashReservePrepay = min_cash_reserve_prepay
        self.dayCount = day_count
        self.sweepPercent = sweep_percent
        self.effectiveInterestRates = effective_interest_rates
        self.upsizes = upsizes
        self.prepays = prepays
        self.oids = oids
        self.dfcs = dfcs
        self.oidPayments = oid_payments
        self.dfcPayments = dfc_payments
        self.interestPayments = interest_payments
        self.requiredDSRAs = required_dsras
        self.dsraCashMovement = dsra_cash_movement
        self.amortizations = amortizations
        self.principalBalances = principal_balances
        self.flagPrepayable = flag_prepayable
        self.flagDsraFundByLc = flag_dsra_fund_by_lc
        self.flagHistoricals = flag_historicals



    def build_period_list(self):
        period_list = []
        if self.issueDate == date(self.issueDate.year, self.issueDate.month, monthrange(self.issueDate.year, self.issueDate.month)[-1]):
            self.issueDate = self.issueDate + timedelta(days=1)

        month_end = date(self.issueDate.year, self.issueDate.month, monthrange(self.issueDate.year, self.issueDate.month)[-1])

        while month_end < self.maturityDate:
            number_of_days_for_period = month_end.day
            if month_end.year == self.issueDate.year and month_end.month == self.issueDate.month:
                number_of_days_for_period = (month_end.day - self.issueDate.day) + 1
                period_list.append([month_end, number_of_days_for_period])
            else:
                period_list.append([month_end, number_of_days_for_period])

            month_end = month_end + timedelta(days=1)
            month_end = date(month_end.year, month_end.month, monthrange(month_end.year, month_end.month)[-1])

        if month_end >= self.maturityDate and month_end.year == self.maturityDate.year and month_end.month == self.maturityDate.month:
            number_of_days_for_period = self.maturityDate.day
            period_list.append([month_end, number_of_days_for_period])


        return period_list


    def build_principal_balances(self):
        period_list = self.build_period_list()


        for period_item in period_list:
            month_end = period_item[0]
            self.principalBalances[month_end] = 0.0
            balance = self.initialBalance
            upsize_balance = sum([self.upsizes[month] for month in self.upsizes if month <= month_end])
            prepayment_balance = sum([self.prepays[month] for month in self.prepays if month <= month_end])
            amortization_balance = sum([self.amortizations[month] for month in self.amortizations if month <= month_end])
            balance += upsize_balance
            balance -= prepayment_balance
            balance -= amortization_balance
            self.principalBalances[month_end] = balance


        return self.principalBalances




    def build_interest_payments(self, forecast_start):
        period_list = self.build_period_list()

        period_list = [item for item in period_list if item[0] >= forecast_start]

        for period_item in period_list:
            month_end = period_item[0]
            effective_interest_rate = self.effectiveInterestRates[month_end]
            balance = self.initialBalance
            upsize_balance = sum([self.upsizes[month] for month in self.upsizes if month <= month_end])
            prepayment_balance = sum([self.prepays[month] for month in self.prepays if month <= month_end])
            balance += upsize_balance
            balance -= prepayment_balance
            self.interestPayments[month_end] = balance * effective_interest_rate * 30 / 360
            if self.dayCount == 'day/365':
                self.interestPayments[month_end] = balance * effective_interest_rate * month_end.day / 365

        return self.interestPayments



    def prepay_debt(self, forecast_month, forecast_start, prepayment):
        if forecast_month in self.prepays:
            self.prepays[forecast_month] = self.prepays[forecast_month] + prepayment
        else:
            self.prepays[forecast_month] = prepayment

        self.build_principal_balances()
        self.build_dsras(forecast_start)


    def calculate_interest_expense(self, forecast_month):
        balance = self.principalBalances[forecast_month]
        effective_interest_rate = self.effectiveInterestRates[forecast_month]
        number_of_days = forecast_month.day
        interest_expense = balance * effective_interest_rate * number_of_days / 365
        if self.dayCount == 'day/365':
            interest_expense = balance * effective_interest_rate * number_of_days / 365
        if self.dayCount == '30/360':
            interest_expense = balance * effective_interest_rate * 30 / 360
            number_of_days = 30


        self.interestPayments[forecast_month] = interest_expense
        return interest_expense

    def calculate_amortization(self, forecast_month):
        balance = self.initialBalance
        annual_amort_amount = balance * self.annualScheduledAmort
        periodicity = self.periodicityMonths
        if forecast_month.month % periodicity != 0:
            return 0
        else:
            amortization = annual_amort_amount / 12.0 * periodicity
            if amortization > self.principalBalances[forecast_month]:
                amortization = self.principalBalances[forecast_month]

            self.amortizations[forecast_month] = amortization
            self.build_principal_balances()
            return amortization




    """ method to calculate term loan prepayment based on available_cash """
    def calculate_prepayment(self, forecast_month, available_cash):
        if self.flagPrepayable == False:
            """ if a debt is not prepayable, then return 0 """
            self.prepays[forecast_month] = 0.0
            return 0.0

        if forecast_month.month % self.periodicityMonths != 0:
            """ if period is not on periodicity, then return 0 """
            self.prepays[forecast_month] = 0.0
            return 0.0

        prepay = available_cash if available_cash > 0 else 0.0
        current_balance = self.principalBalances[forecast_month]


        if prepay > current_balance:
            prepay = current_balance


        self.prepays[forecast_month] = prepay
        self.build_principal_balances()

        return prepay





    # """ method to build term loan prepayment for lbo analysis """
    # """ not designed for the liquidity purpose """
    # def build_prepayments(self, forecast_start_month, available_cash):
    #     period_list = self.build_period_list()
    #     for period_month in period_list:
    #         if period_month[0] >= forecast_start_month:
    #             if period_month[0].month % self.period_month == 0:
    #                 period_start = date(period_month[0].year, period_month[0] - self.periodicityMonths + 1, monthrange(period_month[0].year, period_month[0] - self.periodicityMonths + 1)[1])
    #                 period_end = period_month[0]
    #                 beginning_debt_balance = self.principalBalances[period_start]
    #                 effective_interest_rates = {key:self.effectiveInterestRates[key] for key in self.effectiveInterestRates if key >= period_start and key <= period_end}
    #                 available_cash = {key:available_cash[key] for key in available_cash if key >= period_start and key <= period_end}





    def build_dsras(self, start_date):
        period_list = self.build_period_list()
        for period_item in period_list:
            month_end = period_item[0]
            if month_end >= start_date:
                # quarterly : mod(3)
                # semiannually : mod(6)
                if month_end.month % self.periodicityMonths == 0:
                    start_debt_balance = self.initialBalance \
                                         - sum([self.prepays[month] for month in self.prepays if month <= month_end]) \
                                         + sum([self.upsizes[month] for month in self.upsizes if month <= month_end])
                    next_six_months_list = []
                    next_month = month_end
                    max_number_of_months = 1
                    required_interest = 0.0
                    while max_number_of_months <= self.dsraMonths:
                        next_month = next_month + timedelta(days=1)
                        next_month = date(next_month.year, next_month.month, monthrange(next_month.year, next_month.month)[1])
                        next_six_months_list.append(next_month)
                        interest_month = next_month

                        """ if month exceeds the maximum available forecast period then use the last month """
                        if next_month > self.maturityDate:
                            interest_month = self.maturityDate

                        required_interest += start_debt_balance * self.effectiveInterestRates[interest_month] * 30 / 360
                        if self.dayCount == 'day/365':
                            required_interest += start_debt_balance * self.effectiveInterestRates[interest_month] * next_month.day / 365
                        max_number_of_months += 1

                    self.requiredDSRAs[month_end] = required_interest

        """ add logic to check if fundbylc then leave cash movement as 0 """
        if self.flagDsraFundByLc:
            self.dsraCashMovement[month_end] = 0.0



    def set_historical_amortization(self, forecast_start_month, debt_activity_df=None):
        if self.flagHistoricals:
            if debt_activity_df is None:
                debt_activity_df = dbLiquidity.get_debt_activity(self.instrumentID)
            amortizations_df = debt_activity_df.loc[(debt_activity_df.instrument_id == self.instrumentID) & (debt_activity_df.activity_type == 'amortization') & (debt_activity_df.date < forecast_start_month)].copy()
            amortizations_df['value'] = amortizations_df['value']
            self.amortizations = amortizations_df.set_index('date')['value'].to_dict()


    def set_historical_size_change(self, forecast_start_month, debt_activity_df=None):
        if self.flagHistoricals:
            if debt_activity_df is None:
                debt_activity_df = dbLiquidity.get_debt_activity(self.instrumentID)

            """ load debt upsize information """
            upsizes_df = debt_activity_df.loc[(debt_activity_df.activity_type=='additional borrowing') & (debt_activity_df.date < forecast_start_month)][['date','value']].copy()
            if len(upsizes_df) > 0:
                upsizes_df['date'] = upsizes_df.apply(lambda row: date(row['date'].year, row['date'].month, monthrange(row['date'].year, row['date'].month)[1]), axis=1)
                self.upsizes = upsizes_df.set_index('date')['value'].to_dict()
            else:
                self.upsizes = {}

            """ load debt prepayment information """
            prepay_df = debt_activity_df.loc[debt_activity_df.activity_type=='prepayment'][['date','value']].copy()
            if len(prepay_df) > 0:
                prepay_df['date'] = prepay_df.apply(lambda row: date(row['date'].year, row['date'].month, monthrange(row['date'].year, row['date'].month)[1]), axis=1)
                prepay_df['value'] = prepay_df['value']
                self.prepays = prepay_df.set_index('date')['value'].to_dict()
            else:
                self.prepays = {}


        return self.upsizes, self.prepays, debt_activity_df

    def set_historical_interest_payments(self, forecast_start_month, debt_activity_df=None):
        if self.flagHistoricals:
            if debt_activity_df is None:
                debt_activity_df = dbLiquidity.get_debt_activity(self.instrumentID)
            interest_expense_df = debt_activity_df.loc[(debt_activity_df.instrument_id == self.instrumentID) & (debt_activity_df.activity_type == 'interest expense') & (debt_activity_df.date < forecast_start_month)].copy()
            interest_expense_df['value'] = - interest_expense_df['value']
            self.interestPayments = interest_expense_df.set_index('date')['value'].to_dict()
        return self.interestPayments


    def build_amortizations(self, forecast_start_month):
        period_list = self.build_period_list()
        for period_month in period_list:
            if period_month[0] >= forecast_start_month:
                if period_month[0].month % self.periodicityMonths == 0:
                    self.amortizations[period_month[0]] = self.initialBalance * self.annualScheduledAmort / 12.0 * self.periodicityMonths
        return self.amortizations





class FixedDebt(Debt):


    def __init__(self,
                 fixed_rate,
                 instrument_id,
                 issue_date,
                 maturity_date,
                 term,
                 initial_balance,
                 interest_start_date,
                 amort_start_date,
                 periodicity_months,
                 annual_scheduled_amort,
                 min_cash_reserve_prepay,
                 day_count='30/360',
                 sweep_percent=1,
                 dsra_months=6,
                 oids=[],     # a list of OID objects
                 dfcs=[],     # a list of DFC objects
                 oid_payments={},
                 dfc_payments={},
                 upsizes={},
                 prepays={},
                 effective_interest_rates={},
                 interest_payments={},
                 required_dsras={},
                 dsra_cash_movement={},
                 amortizations={},
                 principal_balances={},
                 flag_prepayable=True,
                 flag_historicals=True,
                 flag_dsra_fund_by_lc=True):


        Debt.__init__(self,
                      instrument_id,
                      issue_date,
                      maturity_date,
                      term,
                      initial_balance,
                      interest_start_date,
                      amort_start_date,
                      periodicity_months,
                      annual_scheduled_amort,
                      min_cash_reserve_prepay,
                      day_count,
                      sweep_percent,
                      dsra_months,
                      oids,
                      dfcs,
                      oid_payments,
                      dfc_payments,
                      upsizes,
                      prepays,
                      effective_interest_rates,
                      interest_payments,
                      required_dsras,
                      dsra_cash_movement,
                      amortizations,
                      principal_balances,
                      flag_prepayable,
                      flag_historicals,
                      flag_dsra_fund_by_lc)
        self.fixedRate = fixed_rate

    def set_effective_interest_rates(self):
        period_list = Debt.build_period_list(self)
        for month in period_list:
            month_end = month[0]
            self.effectiveInterestRates[month_end] = self.fixedRate

        return self.effectiveInterestRates



# TLB TLC
class FloatingDebt(Debt):

    def __init__(self,
                 margins,    # only floating debt has margin
                 index,     # only floating debt has index
                 instrument_id,
                 issue_date,
                 maturity_date,
                 term,
                 initial_balance,
                 interest_start_date,
                 amort_start_date,
                 periodicity_months,
                 annual_scheduled_amort,
                 min_cash_reserve_prepay,
                 day_count='30/360',
                 sweep_percent=1,
                 dsra_months=6,
                 oids=[],     # a list of OID objects
                 dfcs=[],     # a list of DFC objects
                 oid_payments={},
                 dfc_payments={},
                 upsizes={},
                 prepays={},
                 effective_interest_rates={},
                 interest_payments={},
                 required_dsras={},
                 dsra_cash_movement={},
                 amortizations={},
                 principal_balances={},
                 flag_prepayable=True,
                 flag_historicals=True):

        Debt.__init__(self,
                      instrument_id,
                      issue_date,
                      maturity_date,
                      term,
                      initial_balance,
                      interest_start_date,
                      amort_start_date,
                      periodicity_months,
                      annual_scheduled_amort,
                      min_cash_reserve_prepay,
                      day_count,
                      sweep_percent,
                      dsra_months,
                      oids,
                      dfcs,
                      oid_payments,
                      dfc_payments,
                      upsizes,
                      prepays,
                      effective_interest_rates,
                      interest_payments,
                      required_dsras,
                      dsra_cash_movement,
                      amortizations,
                      principal_balances,
                      flag_prepayable,
                      flag_historicals)


        self.index = index
        self.margins = margins



    def set_effective_interest_rates(self, index_df, forecast_start, floor=None):
        # to be implemented
        index_df = index_df.loc[index_df.instrument_id==self.index]
        index_df['adjusted_period'] = index_df.apply(lambda row: dateUtils.get_one_month_later(row['period']), axis=1)

        period_list = Debt.build_period_list(self)
        period_list = [period for period in period_list if period[0] >= forecast_start]


        for period_month in period_list:
            month_end = period_month[0]
            floating_interest_rate = index_df.loc[index_df.adjusted_period==month_end].iloc[0]['value']

            margin = sum([float(margin_item[0]) for margin_item in self.margins if margin_item[1] <= month_end and month_end <= margin_item[2]])

            if floor is not None:
                floating_interest_rate = 0.01 if floating_interest_rate < 0.01 else floating_interest_rate
            self.effectiveInterestRates[month_end] = floating_interest_rate + margin




# Revolver
class Revolver(FloatingDebt):
    def __init__(self,
                 credit_line,   # only revolver has credit line, the maximum capacity
                 min_cash_reserve_revolver,   # still thinking if we need this:  condition for the revolver draw if cash is below a certain amount
                 # new logic is going to assume fully draw revolver
                 # then get the total liquidity = revolver + ending cash
                 # repay revolver as much as possible to its credit line
                 # repay revolver as much as possible to minimum_cash_reserve_revolver if cannot repay back to
                 # for lightstone, it is if balance below 0 for a month, repay amount only to get the
                 # revolver draw should happen during month while repay should only happen during quarter ends
                 margins,    # only floating debt has margin
                 index,     # only floating debt has index
                 instrument_id,
                 issue_date,
                 maturity_date,
                 term,
                 initial_balance,
                 interest_start_date,
                 amort_start_date,      # will be none since revolver doesnt do amortization
                 periodicity_months,     # will be 1 month
                 annual_scheduled_amort,   # will be 0 since revolver doesnt do amortization
                 min_cash_reserve_prepay,
                 day_count='30/360',
                 sweep_percent=1,    # will not be used by revolver
                 dsra_months=6,     # will not be used by revolver
                 oids=[],     # empty for revolver
                 dfcs=[],     # empty for revolver
                 oid_payments={},   # empty for revolver
                 dfc_payments={},   # empty for revolver
                 upsizes={},     # this will be used as the revolver draw
                 prepays={},     # this will be used as the revolver payback
                 effective_interest_rates={},   # the effective interest rates
                 interest_payments={},   # the interest payments
                 required_dsras={},   # empty for revolver
                 dsra_cash_movement={},   # empty for revolver
                 amortizations={},
                 principal_balances={},
                 flag_prepayable=True,   # prepay for revolver is acting as repay any remaining balance of the revolver
                 flag_historicals=True):   # flag for reading historicials for the revolver, always true for revolver to get the life to date balance




        FloatingDebt.__init__(self,
                              margins,    # only floating debt has margin
                              index,      # only floating debt has index
                              instrument_id,
                              issue_date,
                              maturity_date,
                              term,
                              initial_balance,
                              interest_start_date,
                              amort_start_date,
                              periodicity_months,
                              annual_scheduled_amort,
                              min_cash_reserve_prepay,
                              day_count,
                              sweep_percent,
                              dsra_months,
                              oids,
                              dfcs,
                              oid_payments,
                              dfc_payments,
                              upsizes,
                              prepays,
                              effective_interest_rates,
                              interest_payments,
                              required_dsras,
                              dsra_cash_movement,
                              amortizations,
                              principal_balances,
                              flag_prepayable,
                              flag_historicals)
        self.creditLine = credit_line
        self.minCashReserveRevolver = min_cash_reserve_revolver




    def build_revolver_draw(self, ending_cash_balances):
        for period in ending_cash_balances:
            ending_cash_balance = ending_cash_balances[period]
            # only if a period
            # 1. has negative cash flow
            # 2. is not a quarter end
            # 3. does not have predefined draw amount
            if ending_cash_balance < 0 and period.month % self.periodicityMonths != 0 and self.upsizes[period] != 0:
                self.upsizes[period] = self.min_cash_reserve_revolver - ending_cash_balance

        return self.upsizes

    def set_historical_revolver_change(self, forecast_start_month):
        if self.flagHistoricals is True:
            revolver_activity_df = dbLiquidity.get_debt_activity(self.instrumentID)
            upsizes_df = revolver_activity_df.loc[(revolver_activity_df.activity_type=='draw') & (revolver_activity_df.date < forecast_start_month)][['date','value']]
            prepays_df = revolver_activity_df.loc[(revolver_activity_df.activity_type=='repay') & (revolver_activity_df.date < forecast_start_month)][['date','value']]
            upsizes_df['date'] = upsizes_df.apply(lambda row: date(row['date'].year, row['date'].month, monthrange(row['date'].year, row['date'].month)[1]), axis=1)
            prepays_df['date'] = prepays_df.apply(lambda row: date(row['date'].year, row['date'].month, monthrange(row['date'].year, row['date'].month)[1]), axis=1)
            self.upsizes = upsizes_df.set_index('date')['value'].to_dict()
            self.prepays = prepays_df.set_index('date')['value'].to_dict()

        return self.upsizes, self.prepays

    def set_projected_revolver_change(self, forecast_start_month, scenario_assumptions_df):
        scenario_assumptions_df['value'] = pd.to_numeric(scenario_assumptions_df['value'], downcast='float')
        upsizes_df = scenario_assumptions_df.loc[(scenario_assumptions_df.account=='Revolver Change') & (scenario_assumptions_df.value > 0) & (scenario_assumptions_df.date_end >= forecast_start_month)][['date_end','value']]
        prepays_df = scenario_assumptions_df.loc[(scenario_assumptions_df.account=='Revolver Change') & (scenario_assumptions_df.value <= 0) & (scenario_assumptions_df.date_end >= forecast_start_month)][['date_end','value']]
        prepays_df['value'] = - prepays_df['value']

        upsizes_df['date'] = upsizes_df.apply(lambda row: date(row['date_end'].year, row['date_end'].month, monthrange(row['date_end'].year, row['date_end'].month)[1]), axis=1)
        prepays_df['date'] = prepays_df.apply(lambda row: date(row['date_end'].year, row['date_end'].month, monthrange(row['date_end'].year, row['date_end'].month)[1]), axis=1)



        projected_upsizes_dict = upsizes_df.set_index('date')['value'].to_dict()
        for month in projected_upsizes_dict:
            if month in self.upsizes:
                self.upsizes[month] = self.upsizes[month] + projected_upsizes_dict[month]
            else:
                self.upsizes[month] = projected_upsizes_dict[month]

        projected_prepays_dict = prepays_df.set_index('date')['value'].to_dict()

        for month in projected_prepays_dict:
            if month in self.prepays:
                self.prepays[month] = self.prepays[month] - projected_prepays_dict[month]
            else:
                self.prepays[month] = projected_prepays_dict[month]


        return self.upsizes, self.prepays






class OID:
    def __init__(self, balance, begin_date, end_date, oid_discount):
        self.balance = balance
        self.beginDate = begin_date
        self.endDate = end_date
        self.oidDiscount = oid_discount


    # private function for calculating monthly accretions
    def __balance_accretion(balance, oid_discount, oid_ytm, begin_date, end_date):
        start_discounted_balance = balance * oid_discount / 100.0

        # if begin_date is already a month end, then start from the next month
        month_end = date(begin_date.year, begin_date.month, monthrange(begin_date.year, begin_date.month)[-1])
        month_begin_balance = start_discounted_balance
        monthly_oid_payments = {}

        while month_end < end_date:
            month_begin_balance += (1/12.0) * oid_ytm * month_begin_balance
            monthly_oid_payments[month_end] = (1/12.0) * oid_ytm * month_begin_balance
            month_end = month_end + timedelta(days=1)
            month_end = date(month_end.year, month_end.month, monthrange(month_end.year, month_end.month)[-1])


        if month_end >= end_date and month_end.year == end_date.year and month_end.month == end_date.month:
            month_begin_balance += (1/12.0) * oid_ytm * month_begin_balance
            monthly_oid_payments[month_end] = (1/12.0) * oid_ytm * month_begin_balance


        return month_begin_balance, monthly_oid_payments




    # private function for calculating oid accretions
    def __oid_ytm_calc_wrapper(oid_ytm, *args):
        balance, begin_date, end_date, oid_discount = args
        accretioned_balance, monthly_oid_payments = OID.__balance_accretion(balance, oid_discount, oid_ytm[0], begin_date, end_date)

        return balance - accretioned_balance



    def build_monthly_oid_payments(self):
        oid_ytm = 0.001
        oid_ytm = fsolve(OID.__oid_ytm_calc_wrapper, oid_ytm, args=(self.balance, self.beginDate, self.endDate, self.oidDiscount))
        reached_balance, monthly_oid_payments = OID.__balance_accretion(self.balance, self.oidDiscount, oid_ytm[0], self.beginDate, self.endDate)
        return monthly_oid_payments


    @staticmethod
    def calc_monthly_oid_payments(balance, begin_date, end_date, oid_discount):
        oid_ytm = 0.001
        oid_ytm = fsolve(OID.__oid_ytm_calc_wrapper, oid_ytm, args=(balance, begin_date, end_date, oid_discount))
        reached_balance, monthly_oid_payments = OID.__balance_accretion(balance, oid_discount, oid_ytm[0], begin_date, end_date)
        return monthly_oid_payments





class DFC:
    def __init__(self, debt_balance, begin_date, end_date, dfc_rate):
        self.debtBalance = debt_balance
        self.beginDate = begin_date
        self.endDate = end_date
        self.dfcRate = dfc_rate

    def build_monthly_dfc_payments(self):
        month_end = date(self.beginDate.year, self.beginDate.month, monthrange(self.beginDate.year, self.beginDate.month)[-1])
        monthly_dfc_payments = {}

        month_end = date(self.beginDate.year, self.beginDate.month, monthrange(self.beginDate.year, self.beginDate.month)[-1])

        while month_end < self.endDate:
            number_of_days_for_period = month_end.day
            if month_end.year == self.beginDate.year and month_end.month == self.beginDate.month:
                number_of_days_for_period = (month_end.day - self.beginDate.day) + 1
            number_of_days_for_year = (date(month_end.year, 12, 31) - date(month_end.year, 1, 1)).days + 1
            monthly_dfc_payments[month_end] = (number_of_days_for_period / number_of_days_for_year) * self.dfcRate * self.debtBalance
            month_end = month_end + timedelta(days=1)
            month_end = date(month_end.year, month_end.month, monthrange(month_end.year, month_end.month)[-1])

        if month_end >= self.endDate and month_end.year == self.endDate.year and month_end.month == self.endDate.month:
            number_of_days_for_period = self.endDate.day
            number_of_days_for_year = (date(self.endDate.year, 12, 31) - date(self.endDate.year, 1, 1)).days
            monthly_dfc_payments[month_end] = (number_of_days_for_period / number_of_days_for_year) * self.dfcRate * self.debtBalance

        return monthly_dfc_payments





class Swap:
    def __init__(self, portfolio, instrument_id, index, trade_date, counterparty, swap_rates):
        self.portfolio = portfolio
        self.instrumentID = instrument_id
        self.index = index
        self.tradeDate = trade_date
        self.counterparty = counterparty
        self.swapRates = swap_rates

    """ date_fix_rate, date_start, date_end, notional, fix_rate, floating_rate, number_of_days, swap_per_day """

    def build_swap_interest_payments(self, index_df):
        for swap_info in self.swapRates:
            date_fix_rate = swap_info[0]
            date_start = swap_info[1]
            date_end = swap_info[2]
            notional = swap_info[3]
            fix_rate = swap_info[4]

            floating_rate = 0.0

            index_df['adjusted_period'] = index_df.apply(lambda row: date(row['period'].year, row['period'].month, monthrange(row['period'].year, row['period'].month)[1]), axis=1)
            """ since fix_rate_date is always the prior month rate, there is no need to shift the libor again """
            # index_df['rate_use_date'] = index_df.apply(lambda row: dateUtils.get_one_month_later(row['adjusted_period']), axis=1)

            date_fix_rate = date(date_fix_rate.year, date_fix_rate.month, monthrange(date_fix_rate.year, date_fix_rate.month)[1])
            floating_rate = index_df.loc[(index_df.adjusted_period==date_fix_rate) & (index_df.instrument_id==self.index)]['value'].mean()

            """ for swap, floating side libor has a 1 percent floor """
            floating_rate = 0.01 if floating_rate < 0.01 else floating_rate

            number_of_days = (date_end - date_start).days
            swap_payment_perday = 1 / 365 * (fix_rate - floating_rate) * notional
            swap_info.append(floating_rate)
            swap_info.append(number_of_days)
            swap_info.append(swap_payment_perday)

        return self.swapRates

    def get_swap_rates_from_db(self):
        swap_rates_df = dbLiquidity.get_swap(self.portfolio, self.instrumentID)
        self.swapRates = swap_rates_df[['date_fix_rate', 'date_start', 'date_end', 'notional', 'fix_rate']].values.tolist()

    def build_swap_payments_by_month(self, start_month, end_month):
        index_month = start_month
        swap_payments_monthly_result_list = []
        while index_month <= end_month:
            index_month_start_date = date(index_month.year, index_month.month, 1)
            index_month_end_date = date(index_month.year, index_month.month, monthrange(index_month.year, index_month.month)[1])

            index_day = index_month_start_date
            total_days = 0.0
            total_balance = 0.0
            total_interest_payment = 0.0
            average_daily_notional = 0.0
            effective_interest_rate = 0.0

            while index_day <= index_month_end_date:
                for swap_rate_info in self.swapRates:
                    if swap_rate_info[1] <= index_day and swap_rate_info[2] >= index_day:
                        total_balance += swap_rate_info[3]
                        total_interest_payment += swap_rate_info[7]
                        total_days += 1
                        break
                index_day = index_day + timedelta(1)

            if total_days != 0:
                average_daily_notional = total_balance / total_days
                effective_interest_rate = total_interest_payment / total_days * 365 / average_daily_notional

            swap_payments_monthly_result_list.append([index_month_start_date, index_month_end_date, total_days, average_daily_notional, effective_interest_rate, total_interest_payment])



            index_month = index_month + timedelta(1)
            index_month = date(index_month.year, index_month.month, monthrange(index_month.year, index_month.month)[1])

        swap_payments_monthly_result_df = pd.DataFrame(data=swap_payments_monthly_result_list, columns=['month_start','month_end','number_of_days','average_daily_notional','effective_interest_rate','total_interest_payment'])
        swap_payments_monthly_result_df['instrument_id'] = self.instrumentID

        return swap_payments_monthly_result_df





class LettersOfCredit():
    pass







class TaxRegister():
    def __init__(self, portfolio, effective_tax_rate=0.0, tax_split_ratio=[], paid_tax={}):
        self.portfolio = portfolio
        self.effectiveTaxRate = effective_tax_rate
        self.taxSplitRatio = tax_split_ratio
        self.paidTax = paid_tax


    def get_paid_tax_from_db(self, as_of_date):
        paid_tax_dict = dbLiquidity.get_paid_tax(self.portfolio, as_of_date)
        self.paidTax = paid_tax_dict


    def calculate_tax_payment(self, year, total_oid, total_ebitda, total_dfc, total_tax_depreciation, total_interest_expense = None):
        """ differs by portfolio """
        if self.portfolio == 'Lightstone':
            adj_interest_deduction_cap = 0.0
            if year <= 2021:
                adj_interest_deduction_cap = total_ebitda * 0.3
                if total_interest_expense is not None:
                    adj_interest_deduction_cap = min([total_ebitda * 0.3, total_interest_expense + total_oid])
            else:
                adj_interest_deduction_cap = (total_ebitda - total_tax_depreciation) * 0.3
                if total_interest_expense is not None:
                    adj_interest_deduction_cap = min([(total_ebitda - total_tax_depreciation) * 0.3, total_interest_expense + total_oid])

            ebt = total_ebitda - adj_interest_deduction_cap - total_dfc - total_tax_depreciation


            total_tax = ebt * self.effectiveTaxRate

            paid_ptd_list = []
            for key in sorted(self.paidTax.keys()):
                if key.year == year:
                    paid_ptd_list.append(self.paidTax[key])

            if len(paid_ptd_list) == 0:
                return [total_tax * item for item in self.taxSplitRatio]
            else:
                return paid_ptd_list + [(total_tax - sum(paid_ptd_list)) / (4 - len(paid_ptd_list)) for i in range(4 - len(paid_ptd_list))]


class FixedAsset():
    def __init__(self, portfolio, entity_name, depreciation_method, depreciation_term, in_service_year, initial_purchase_price, capex={}, depreciation_adjustment={}):
        self.portfolio = portfolio
        self.entityName = entity_name
        self.depreciationMethod = depreciation_method
        self.depreciationTerm = depreciation_term
        self.inServiceYear = in_service_year
        self.initialPurchasePrice = initial_purchase_price
        self.capex = capex
        self.depreciationAdjustment = depreciation_adjustment


    def calcualte_tax_depreciation(self, additional_capex, year):
        if self.depreciationTerm == 0:
            """ e.g. land """
            return 0

        total_tax_depreciation = 0

        if self.inServiceYear == year and self.depreciationMethod == 'Straight Line':
            total_tax_depreciation += self.initialPurchasePrice * 1/self.depreciationTerm / 2

        if self.inServiceYear < year and self.depreciationMethod == 'Straight Line':
            total_tax_depreciation += self.initialPurchasePrice * 1/self.depreciationTerm

        total_previous_year_capex = sum([self.capex[capex_year] for capex_year in self.capex if capex_year < year])
        total_previous_year_dep_adjustment = sum([self.depreciationAdjustment[adj_year] for adj_year in self.depreciationAdjustment if adj_year + 1 == year])
        total_tax_depreciation += total_previous_year_capex * 1 / self.depreciationTerm
        total_tax_depreciation += total_previous_year_dep_adjustment * 1 / self.depreciationTerm


        for capex_year in additional_capex:
            if capex_year < year:
                total_tax_depreciation += additional_capex[capex_year] * 1/self.depreciationTerm
        if year in additional_capex:
            total_tax_depreciation += additional_capex[year] * 1/self.depreciationTerm/2

        return total_tax_depreciation



class Equity():
    def __init__(self, name, purchase_price, debt_percentage, exit_multiple, irr_frequency, exit_time, periodicity_months, exit_value=0.0):
        self.name = name
        self.purchasePrice = purchase_price
        self.debtPercentage = debt_percentage
        self.exitMultiple = exit_multiple
        self.irrFrequency = irr_frequency
        self.exitTime = exit_time
        self.periodicityMonths = periodicity_months
        self.exitValue = exit_value

    def calculate_initial_equity(self):
        return self.purchasePrice - self.purchasePrice * self.debtPercentage

    def calculate_dollar_per_capacity(self, total_capacity, unit='$/Kw'):
        return self.purchasePrice / total_capacity


    def calculate_exit_value(self, last_tweleve_months_ebitda):
        return self.exitMultiple * last_tweleve_months_ebitda

    def calculate_equity_sweep(self, forecast_month, available_cash):
        if forecast_month.month % self.periodicityMonths != 0:
            return 0

        if available_cash > 0:
            return available_cash

        return 0

    def calculate_irr_and_moic(self, equity_cashflow, exit_value_less_debt):
        if self.irrFrequency.lower() == 'annual':
            initial_equity = self.purchasePrice * (1 - self.debtPercentage)
            start_year = equity_cashflow.index.min().year
            end_year = equity_cashflow.index.max().year
            year_list = list(range(start_year, end_year+1))
            equity_annual_cashflow_list = []
            for year in year_list:
                if year == year_list[-1]:
                    cashflow_for_the_year = exit_value_less_debt - equity_cashflow.loc[date(year,1,31):date(year,12,31)]['Equity - sweep'].sum()
                else:
                    cashflow_for_the_year = -equity_cashflow.loc[date(year,1,31):date(year,12,31)]['Equity - sweep'].sum()
                equity_annual_cashflow_list.append(cashflow_for_the_year)
            equity_annual_cashflow_list = [-initial_equity] + equity_annual_cashflow_list

            return equity_annual_cashflow_list, np.irr(equity_annual_cashflow_list), sum(equity_annual_cashflow_list) / initial_equity
        else:
            return 0.0,0.0


    def calculate_equity_exit(index, last_tweleve_months_ebitda):
        if index != self.exitTime:
            return 0
        return last_tweleve_months_ebitda * self.exitMultiple








# #
