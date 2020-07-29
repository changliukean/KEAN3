from database import dbScenarioMaster
from utility import dateUtils
import pandas as pd


from datetime import datetime, date
import sys


class Scenario:

    def __init__(self, module, table, portfolio, scenario, version, comment=''):

        self.module = module
        self.table = table
        self.portfolio = portfolio
        self.scenario = scenario
        self.version = version
        self.comment = ''

    def print_scenario(self):
        print ("---------------------------")
        print ("Scenario object:")
        print ("module:", self.module)
        print ("table:", self.table)
        print ("portfolio: ", self.portfolio)
        print ("scenario:", self.scenario)
        print ("version:", self.version)
        print ("comment:", self.comment)


    def __str__(self):
        console_text = ''
        console_text += ("---------------------------")
        console_text += ("Scenario object:\n")
        console_text += ("module:" + self.module + "\n")
        console_text += ("table:" + self.table + "\n")
        console_text += ("portfolio: " + self.portfolio + "\n")
        console_text += ("scenario: " + self.scenario + "\n")
        console_text += ("version: " + self.version + "\n")
        console_text += ("comment: " + self.comment + "\n")
        # sys.stdout.write(console_text)  # print to the shell
        return console_text




class ScenarioMaster:
    def __init__(self,
                 outputScenario,
                 startYear=1900,
                 numberOfYears=-1,
                 forecastStartMonth=date(1900,1,1),
                 valuationDate=date(1900,1,1),
                 inputScenarios=[],
                 actualMonths=[],
                 forecastMonths=[],
                 inputScenarioMasters=[]):

        # a Scenario OBJECT for output, this is a MUST HAVE parameter for initiating a ScenarioMater instance
        self.outputScenario = outputScenario
        # call a db getter to fill the date time information
        db_start_year, db_number_of_years, db_forecast_start_month, db_valuation_date = self.load_scenario_datetime_fromdb()
        # the first year of the scenario
        self.startYear = startYear if startYear != 1900 else db_start_year
        # the month that the forecast starts
        self.forecastStartMonth = forecastStartMonth if forecastStartMonth != date(1900,1,1) else db_forecast_start_month
        # total number of years
        self.numberOfYears = numberOfYears if numberOfYears != -1 else db_number_of_years
        # valuation date (if needed)
        self.valuationDate = valuationDate if valuationDate != date(1900,1,1) else db_valuation_date
        # list of months for actual period
        self.actualMonths = actualMonths if actualMonths != [] else self.build_actuals_period()
        # list of months for forecast period
        self.forecastMonths = forecastMonths if forecastMonths != [] else self.build_forecast_period()
        # a list of Scenarios OBJECTS for input
        self.inputScenarios = inputScenarios
        # a list of ScenarioMaster OBJECTS for input
        self.inputScenarioMasters = inputScenarioMasters


    def load_sm_fromdb(self):
        raw_scenario_master_df = dbScenarioMaster.get_scenario_master(self.outputScenario.portfolio, self.outputScenario.scenario, self.outputScenario.version, self.outputScenario.module, self.outputScenario.table)

        for index, row in raw_scenario_master_df.iterrows():
            scenario_level = row['scenario_level']
            if scenario_level == 'scenario':
                scenario = Scenario(row['input_module'], row['input_table'], row['portfolio'], row['input_scenario'], row['input_version'], row['comment'])
                self.inputScenarios.append(scenario)

            if scenario_level == 'scenario_master':
                scenario = Scenario(row['input_module'], row['input_table'], row['portfolio'], row['input_scenario'], row['input_version'], row['comment'])
                scenario_master = ScenarioMaster(scenario)
                scenario_master.load_sm_fromdb()
                self.inputScenarioMasters.append(scenario_master)





    def load_scenario_datetime_fromdb(self):
        raw_scenario_master_datetime_df = dbScenarioMaster.get_scenario_master_datetime(self.outputScenario.portfolio, self.outputScenario.scenario, self.outputScenario.version, self.outputScenario.module)
        # raw_scenario_master_datetime_df.to_csv("raw_scenario_master_datetime_df.csv")
        if raw_scenario_master_datetime_df is not None and len(raw_scenario_master_datetime_df) > 0:
            start_year = raw_scenario_master_datetime_df.iloc[0]['start_year']
            number_of_years = raw_scenario_master_datetime_df.iloc[0]['number_of_years']
            forecast_start_month = datetime.strptime(str(raw_scenario_master_datetime_df.iloc[0]['forecast_start_month']), "%Y-%m-%d").date()
            valuation_date = datetime.strptime(str(raw_scenario_master_datetime_df.iloc[0]['valuation_date']), "%Y-%m-%d").date()
            return start_year, number_of_years, forecast_start_month, valuation_date
        else:
            return 1900, -1, date(1900,1,1), date(1900,1,1)

    def build_actuals_period(self):
        actuals_end_month = dateUtils.get_one_month_ago(self.forecastStartMonth)
        actuals_begin_month = date(self.startYear, 1, 31)
        actual_months = dateUtils.get_month_list(actuals_begin_month, actuals_end_month)
        return actual_months


    def build_forecast_period(self):
        forecast_end_month = date(self.startYear + self.numberOfYears - 1, 12,31)
        forecast_months = dateUtils.get_month_list(self.forecastStartMonth, forecast_end_month)
        return forecast_months



    def print_scenario_master(self):
        print ("====================================")
        print ("Scenario Master object: ")
        print ("start year:", self.startYear)
        print ("forecast start month:", self.forecastStartMonth)
        print ("number of years:", self.numberOfYears)
        print ("valuation date:", self.valuationDate)
        print ("actual month list:", self.actualMonths)
        print ("forecast month list:", self.forecastMonths)
        print ("----------------- output scenario: ")
        self.outputScenario.print_scenario()
        print ("input scenarios: ")
        for scenario in self.inputScenarios:
            print (scenario)
        print ("----------------- input scenario masters:")
        for scenario_master in self.inputScenarioMasters:
            scenario_master.print_scenario_master()



    def __str__(self):
        console_text = ''
        console_text += ("====================================\n")
        console_text += ("Scenario Master object: \n")
        console_text += ("start year:" + str(self.startYear) + "\n")
        console_text += ("forecast start month:" + str(self.forecastStartMonth) + "\n")
        console_text += ("number of years:" + str(self.numberOfYears) + "\n")
        console_text += ("valuation date:" + str(self.valuationDate) + "\n")
        console_text += ("actual month list:" + ",".join([str(item) for item in self.actualMonths]) + "\n")
        console_text += ("forecast month list:" + ",".join([str(item) for item in self.forecastMonths]) + "\n")

        console_text += ("----------------- output scenario: \n")
        console_text += str(self.outputScenario)
        console_text += ("input scenarios: \n")
        for scenario in self.inputScenarios:
            # scenario.print_scenario()
            console_text += str(scenario)
        console_text += ("----------------- input scenario masters:\n")
        for scenario_master in self.inputScenarioMasters:
            console_text += str(scenario_master.outputScenario)

        return console_text



    def save(self, force_overwrite=True):

        # step 1 check if scenario_master has it
        existing_scenario_master_df = dbScenarioMaster.get_scenario_master(self.outputScenario.portfolio, self.outputScenario.scenario, self.outputScenario.version, self.outputScenario.module, self.outputScenario.table)

        """ portfolio, scenario, version, module """
        existing_scenario_datetime_df = dbScenarioMaster.get_scenario_master_datetime(self.outputScenario.portfolio, self.outputScenario.scenario, self.outputScenario.version, self.outputScenario.module)

        log_code = "000000"
        # step 2, remove existing record if force_overwrite
        if (len(existing_scenario_master_df) > 0 or len(existing_scenario_datetime_df) > 0) and force_overwrite:
            dbScenarioMaster.delete_scenario_master(self.outputScenario.portfolio, self.outputScenario.scenario, self.outputScenario.version, self.outputScenario.module, self.outputScenario.table)
            dbScenarioMaster.delete_scenario_datetime(self.outputScenario.portfolio, self.outputScenario.scenario, self.outputScenario.version, self.outputScenario.module)
            log_code = "010003"

        # step 3, if not force_overwrite, return and signal it
        if (len(existing_scenario_master_df)) > 0 and (not force_overwrite):
            log_code = "010002"
            return log_code

        # step 4, insert new records
        # step 4.1, reorganize and insert scenario master time info
        dbScenarioMaster.insert_scenario_datetime(self.outputScenario.module,
                                                  self.outputScenario.portfolio,
                                                  self.outputScenario.scenario,
                                                  self.outputScenario.version,
                                                  self.startYear,
                                                  self.numberOfYears,
                                                  self.forecastStartMonth,
                                                  self.valuationDate)

        # step 4.2, reorganize and insert scenario master input info
        ready_to_kean_sm_list = []
        for input_scenario in self.inputScenarios:
            ready_to_kean_sm_row = []
            ready_to_kean_sm_row.append(self.outputScenario.portfolio)
            ready_to_kean_sm_row.append(self.outputScenario.module)
            ready_to_kean_sm_row.append(self.outputScenario.table)
            ready_to_kean_sm_row.append(self.outputScenario.scenario)
            ready_to_kean_sm_row.append(self.outputScenario.version)
            ready_to_kean_sm_row.append(input_scenario.module)
            ready_to_kean_sm_row.append(input_scenario.table)
            ready_to_kean_sm_row.append(input_scenario.scenario)
            ready_to_kean_sm_row.append(input_scenario.version)
            ready_to_kean_sm_row.append("scenario")
            ready_to_kean_sm_row.append(input_scenario.comment)
            ready_to_kean_sm_list.append(ready_to_kean_sm_row)


        for input_scenario_master in self.inputScenarioMasters:
            ready_to_kean_sm_row = []
            ready_to_kean_sm_row.append(self.outputScenario.portfolio)
            ready_to_kean_sm_row.append(self.outputScenario.module)
            ready_to_kean_sm_row.append(self.outputScenario.table)
            ready_to_kean_sm_row.append(self.outputScenario.scenario)
            ready_to_kean_sm_row.append(self.outputScenario.version)
            ready_to_kean_sm_row.append(input_scenario_master.outputScenario.module)
            ready_to_kean_sm_row.append(input_scenario_master.outputScenario.table)
            ready_to_kean_sm_row.append(input_scenario_master.outputScenario.scenario)
            ready_to_kean_sm_row.append(input_scenario_master.outputScenario.version)
            ready_to_kean_sm_row.append("scenario_master")
            ready_to_kean_sm_row.append(input_scenario_master.outputScenario.comment)
            ready_to_kean_sm_list.append(ready_to_kean_sm_row)

        ready_to_kean_sm_df = pd.DataFrame(data=ready_to_kean_sm_list, columns=['portfolio','output_module','output_table','output_scenario','output_version','input_module','input_table','input_scenario','input_version','scenario_level','comment'])
        dbScenarioMaster.insert_scenario_master(ready_to_kean_sm_df)
        return log_code






    def remove(self):
        # to be implemented

        return
















# #
