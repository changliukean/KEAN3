import pandas as pd
from database import dbLBO, dbDispatch, dbPCUC
from utility import dispatchUtils, dateUtils
from model.Entity import Powerplant
from datetime import date
import sys
from pyexcelerate import Workbook
import numpy as np


class Portfolio:

    def __init__(self, name, entities=[]):
        self.name = name
        self.entities = entities


    """ Powerplants related operations """

    def bulk_prepare_basis(self, start_date, end_date, dart='Day Ahead', market='All', to_database_option=False, to_excel=None):

        powerplant_list = [entity for entity in self.entities if entity.type == 'plant']
        if market != 'All':
            powerplant_list = [ powerplant for powerplant in powerplant_list if powerplant.market == market]

        basis_df = pd.DataFrame()
        basis_hourly_detail_df = pd.DataFrame()

        for powerplant in powerplant_list:
            powerplant_basis_df, powerplant_basis_details_df = powerplant.build_basis(start_date, end_date, dart)
            basis_df = basis_df.append(powerplant_basis_df)
            basis_hourly_detail_df = basis_hourly_detail_df.append(powerplant_basis_details_df)
        #
        # basis_df.to_csv("basis_df.csv")
        basis_df = basis_df.reset_index()

        # print (basis_df.columns)

        # basis_df = pd.read_csv("basis_df.csv")

        portfolio_basis_result_df = pd.melt(basis_df, id_vars=['month','peak_info','plant'],
                                            value_vars=['basis_$','basis_%'],
                                            var_name='instrument',
                                            value_name='value')

        portfolio_basis_result_df['instrument_id'] = portfolio_basis_result_df.apply(lambda row: row['plant'] + ' basis - ' + row['peak_info'] + "_" + row['instrument'].split("_")[1], axis=1)

        portfolio_basis_result_df = portfolio_basis_result_df.reset_index()

        portfolio_basis_result_df = pd.pivot_table(portfolio_basis_result_df, index=['month'], columns=['instrument_id'], values='value', aggfunc=np.sum)

        portfolio_basis_result_df = portfolio_basis_result_df.reset_index()

        # portfolio_basis_result_df.to_csv("portfolio_basis_result_df.csv")

        if to_excel is not None:
            # basis_df.to_excel(to_excel, sheet_name='basis')
            # basis_df.to_excel(to_excel, sheet_name='detail')
            basis_values = [portfolio_basis_result_df.columns] + list(portfolio_basis_result_df.values)
            wb = Workbook()
            wb.new_sheet('basis', data=basis_values)
            wb.save(to_excel)

            wb = Workbook()
            basis_detail_values = [basis_hourly_detail_df.columns] + list(basis_hourly_detail_df.values)
            wb.new_sheet('basis_details', data=basis_detail_values)
            wb.save(to_excel.split('.')[0] + "_hourly_detail.xlsx")

        return basis_df, basis_hourly_detail_df

    def get_powerplant_fromdb(self, initiate_technology=False):
        portfolio_with_powerplant_df = dbLBO.get_portfolio_with_powerplant(self.name)
        for index, row in portfolio_with_powerplant_df.iterrows():
            powerplant = Powerplant(row.powerplant_name,
                                    row.fuel_type,
                                    row.market,
                                    row.node,
                                    row.power_hub,
                                    row.technology_name,
                                    row.power_zone,
                                    row.power_hub_on_peak,
                                    row.power_hub_off_peak,
                                    row.fuel_zone,
                                    row.fuel_hub,
                                    row.summer_fuel_basis,
                                    row.winter_fuel_basis,
                                    row.summer_duct_capacity,
                                    row.summer_base_capacity,
                                    row.winter_duct_capacity,
                                    row.winter_base_capacity,
                                    row.first_plan_outage_start,
                                    row.first_plan_outage_end,
                                    row.second_plan_outage_start,
                                    row.second_plan_outage_end,
                                    row.carbon_cost,
                                    row.source_notes,
                                    row.retirement_date,
                                    row.ownership)
            self.entities.append(powerplant)

        return self.entities




    def update_portfolio_fromexcel(self, plant_tech_master_file):
        # to be implemented
        pass

    def update_powerplants_fromexcel(self, plant_tech_master_file, additional=True):
        ready_to_kean_pp_df, ready_to_kean_tech_df = dispatchUtils.load_pp_tech_info(plant_tech_master_file)

        if not additional:
            dbLBO.put_powerplants(ready_to_kean_pp_df, self.name, overwrite_option=True)
        else:
            dbLBO.put_powerplants(ready_to_kean_pp_df)

    def bulk_convert_uc_dataframe(self, technology_df, scenario, version, start_date, end_date, escalation=0.02, push_to_kean=False):
        powerplant_info_list = []
        for entity in self.entities:
            if isinstance(entity, Powerplant):
                powerplant_info_list.append([entity.name,
                                             entity.technology,
                                             entity.fuelType,
                                             entity.market,
                                             entity.powerHub,
                                             entity.powerZone,
                                             entity.powerHubOnPeak,
                                             entity.powerHubOffPeak,
                                             entity.node,
                                             entity.fuelZone,
                                             entity.fuelHub,
                                             entity.summerFuelBasis,
                                             entity.winterFuelBasis,
                                             entity.summerDuctCapacity,
                                             entity.summerBaseCapacity,
                                             entity.winterDuctCapacity,
                                             entity.winterBaseCapacity,
                                             entity.firstPlanOutageStart,
                                             entity.firstPlanOutageEnd,
                                             entity.secondPlanOutageStart,
                                             entity.secondPlanOutageEnd,
                                             entity.carbonCost,
                                             entity.sourceNotes,
                                             entity.retirementDate,
                                             entity.ownership])

        powerplant_df = pd.DataFrame(data=powerplant_info_list, columns=['name',
                                                                         'technology',
                                                                         'fuel_type',
                                                                         'market',
                                                                         'power_hub',
                                                                         'power_zone',
                                                                         'power_hub_on_peak',
                                                                         'power_hub_off_peak',
                                                                         'node',
                                                                         'fuel_zone',
                                                                         'fuel_hub',
                                                                         'summer_fuel_basis',
                                                                         'winter_fuel_basis',
                                                                         'summer_duct_capacity',
                                                                         'summer_base_capacity',
                                                                         'winter_duct_capacity',
                                                                         'winter_base_capacity',
                                                                         'first_plan_outage_start',
                                                                         'first_plan_outage_end',
                                                                         'second_plan_outage_start',
                                                                         'second_plan_outage_end',
                                                                         'carbon_cost',
                                                                         'source_notes',
                                                                         'retirement_date',
                                                                         'ownership'])


        month_list = dateUtils.get_month_list(start_date, end_date)

        merged_simple_uc_df = pd.merge(powerplant_df, technology_df, left_on='technology', right_on='name', how="left")

        ready_to_kean_pcuc_df = pd.DataFrame()

        for index, row in merged_simple_uc_df.iterrows():
            plant_name = row['name_x']
            total_plant_temp_df = pd.DataFrame()
            temp_ready_to_kean_df = pd.DataFrame(data=month_list, columns=['period'])

            """ emissions """
            emissions = row['carbon_cost'] * row['emissions_rate'] / 2000.0

            if row['market'] == 'CAISO':
                emissions = row['carbon_cost'] * row['emissions_rate'] / 2205.0

            emissions_temp_ready_to_kean_df = temp_ready_to_kean_df
            emissions_temp_ready_to_kean_df['characteristic'] = 'emissions'
            emissions_temp_ready_to_kean_df['value'] = emissions_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_escalated_value(emissions, escalation, row['period']), axis=1)
            emissions_temp_ready_to_kean_df['value_str'] = ''

            total_plant_temp_df = total_plant_temp_df.append(emissions_temp_ready_to_kean_df)



            """ forced_outage_value """
            forced_outage_value = row['uof']
            fov_temp_ready_to_kean_df = temp_ready_to_kean_df
            fov_temp_ready_to_kean_df['characteristic'] = 'forced_outage_value'
            fov_temp_ready_to_kean_df['value'] = forced_outage_value
            fov_temp_ready_to_kean_df['value_str'] = ''

            total_plant_temp_df = total_plant_temp_df.append(fov_temp_ready_to_kean_df)

            """ fuel_transport """
            fuel_transport_summer = row['summer_fuel_basis']
            fuel_transport_winter = row['winter_fuel_basis']

            ftp_temp_ready_to_kean_df = temp_ready_to_kean_df
            ftp_temp_ready_to_kean_df['characteristic'] = 'fuel_transport'
            ftp_temp_ready_to_kean_df['value'] = ftp_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_load(row, fuel_transport_summer, fuel_transport_winter), axis=1)
            ftp_temp_ready_to_kean_df['value_str'] = ''

            total_plant_temp_df = total_plant_temp_df.append(ftp_temp_ready_to_kean_df)

            """ fuel_type """
            fuel_type = row['fuel_type']
            ft_temp_ready_to_kean_df = temp_ready_to_kean_df
            ft_temp_ready_to_kean_df['characteristic'] = 'fuel_type'
            ft_temp_ready_to_kean_df['value'] = 0.0
            ft_temp_ready_to_kean_df['value_str'] = fuel_type

            total_plant_temp_df = total_plant_temp_df.append(ft_temp_ready_to_kean_df)

            """ gas_instrument_id """
            gas_instrument_id = row['fuel_hub']
            gii_temp_ready_to_kean_df = temp_ready_to_kean_df
            gii_temp_ready_to_kean_df['characteristic'] = 'gas_instrument_id'
            gii_temp_ready_to_kean_df['value'] = 0.0
            gii_temp_ready_to_kean_df['value_str'] = gas_instrument_id

            total_plant_temp_df = total_plant_temp_df.append(gii_temp_ready_to_kean_df)


            """ heatrate_high_load """
            heatrate_high_load_summer = row['summer_base_heatrate']
            heatrate_high_load_winter = row['winter_base_heatrate']

            hhl_temp_ready_to_kean_df = temp_ready_to_kean_df
            hhl_temp_ready_to_kean_df['value'] = hhl_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_hr(row, heatrate_high_load_summer, heatrate_high_load_winter), axis=1)
            hhl_temp_ready_to_kean_df['characteristic'] = 'heatrate_high_load'
            hhl_temp_ready_to_kean_df['value_str'] = ''

            total_plant_temp_df = total_plant_temp_df.append(hhl_temp_ready_to_kean_df)


            """ heatrate_max_load """
            heatrate_max_load_summer = row['summer_duct_heatrate']
            heatrate_max_load_winter = row['winter_duct_heatrate']

            hml_temp_ready_to_kean_df = temp_ready_to_kean_df
            hml_temp_ready_to_kean_df['value'] = hml_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_hr(row, heatrate_max_load_summer, heatrate_max_load_winter), axis=1)
            hml_temp_ready_to_kean_df['characteristic'] = 'heatrate_max_load'
            hml_temp_ready_to_kean_df['value_str'] = ''

            total_plant_temp_df = total_plant_temp_df.append(hml_temp_ready_to_kean_df)


            """ heatrate_min_load """
            heatrate_min_load_summer = row['summer_base_heatrate'] * row['lol_summer_heatrate']
            heatrate_min_load_winter = row['winter_base_heatrate'] * row['lol_winter_heatrate']

            hminl_temp_ready_to_kean_df = temp_ready_to_kean_df
            hminl_temp_ready_to_kean_df['value'] = hminl_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_hr(row, heatrate_min_load_summer, heatrate_min_load_winter), axis=1)
            hminl_temp_ready_to_kean_df['characteristic'] = 'heatrate_min_load'
            hminl_temp_ready_to_kean_df['value_str'] = ''

            total_plant_temp_df = total_plant_temp_df.append(hminl_temp_ready_to_kean_df)


            """ high_load """
            high_load_summer = row['summer_base_capacity']
            high_load_winter = row['winter_base_capacity']

            hl_temp_ready_to_kean_df = temp_ready_to_kean_df
            hl_temp_ready_to_kean_df['value'] = hl_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_load(row, high_load_summer, high_load_winter), axis=1)
            hl_temp_ready_to_kean_df['characteristic'] = 'high_load'
            hl_temp_ready_to_kean_df['value_str'] = ''

            total_plant_temp_df = total_plant_temp_df.append(hl_temp_ready_to_kean_df)


            """ max_load """
            max_load_summer = row['summer_duct_capacity']
            max_load_winter = row['winter_duct_capacity']

            ml_temp_ready_to_kean_df = temp_ready_to_kean_df
            ml_temp_ready_to_kean_df['value'] = ml_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_load(row, max_load_summer, max_load_winter), axis=1)
            ml_temp_ready_to_kean_df['characteristic'] = 'max_load'
            ml_temp_ready_to_kean_df['value_str'] = ''

            total_plant_temp_df = total_plant_temp_df.append(ml_temp_ready_to_kean_df)



            """ min_load """
            min_load_summer = row['summer_base_capacity'] * row['lol_capacity']
            min_load_winter = row['winter_base_capacity'] * row['lol_capacity']

            ml_temp_ready_to_kean_df = temp_ready_to_kean_df
            ml_temp_ready_to_kean_df['value'] = ml_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_load(row, min_load_summer, min_load_winter), axis=1)
            ml_temp_ready_to_kean_df['characteristic'] = 'min_load'
            ml_temp_ready_to_kean_df['value_str'] = ''

            total_plant_temp_df = total_plant_temp_df.append(ml_temp_ready_to_kean_df)



            """ offpeak_power_hub_instrument_id """
            offpeak_power_hub_instrument_id = row['power_hub_off_peak']
            oph_temp_ready_to_kean_df = temp_ready_to_kean_df
            oph_temp_ready_to_kean_df['value_str'] = offpeak_power_hub_instrument_id
            oph_temp_ready_to_kean_df['value'] = 0.0
            oph_temp_ready_to_kean_df['characteristic'] = 'offpeak_power_hub_instrument_id'

            total_plant_temp_df = total_plant_temp_df.append(oph_temp_ready_to_kean_df)


            """ onpeak_power_hub_instrument_id """
            onpeak_power_hub_instrument_id = row['power_hub_on_peak']
            onph_temp_ready_to_kean_df = temp_ready_to_kean_df
            onph_temp_ready_to_kean_df['value_str'] = onpeak_power_hub_instrument_id
            onph_temp_ready_to_kean_df['value'] = 0.0
            onph_temp_ready_to_kean_df['characteristic'] = 'onpeak_power_hub_instrument_id'

            total_plant_temp_df = total_plant_temp_df.append(onph_temp_ready_to_kean_df)


            """ outage_days """
            outage_start_date = row['first_plan_outage_start']
            outage_end_date = row['first_plan_outage_end']
            od_temp_ready_to_kean_df = temp_ready_to_kean_df
            od_temp_ready_to_kean_df['value'] = od_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_outage_days(row, outage_start_date, outage_end_date), axis=1)
            od_temp_ready_to_kean_df['value_str'] = ''
            od_temp_ready_to_kean_df['characteristic'] = 'outage_days'

            total_plant_temp_df = total_plant_temp_df.append(od_temp_ready_to_kean_df)


            """ dafault to 0s """
            for char in ['ramp_dowm_cold_hours', 'ramp_down_warm_hours', 'ramp_energy_cold', 'ramp_energy_warm', 'ramp_fuel_warm', 'ramp_up_warm_hours']:
                temp_char_df = temp_ready_to_kean_df
                temp_char_df['value'] = 0.0
                temp_char_df['value_str'] = ''
                temp_char_df['characteristic'] = char
                total_plant_temp_df = total_plant_temp_df.append(temp_char_df)


            """ ramp_fuel_cold """
            ramp_fuel_cold_summer = row['start_fuel'] * row['summer_duct_capacity']
            ramp_fuel_cold_winter = row['start_fuel'] * row['winter_duct_capacity']
            rfc_temp_ready_to_kean_df = temp_ready_to_kean_df
            rfc_temp_ready_to_kean_df['value'] = rfc_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_load(row, ramp_fuel_cold_summer, ramp_fuel_cold_winter), axis=1)
            rfc_temp_ready_to_kean_df['value_str'] = ''
            rfc_temp_ready_to_kean_df['characteristic'] = 'ramp_fuel_cold'

            total_plant_temp_df = total_plant_temp_df.append(rfc_temp_ready_to_kean_df)


            """ ramp_up_cold_hours """
            ramp_up_cold_hours = row['start_hours']
            ruch_temp_ready_to_kean_df = temp_ready_to_kean_df
            ruch_temp_ready_to_kean_df['value'] = ramp_up_cold_hours
            ruch_temp_ready_to_kean_df['value_str'] = ''
            ruch_temp_ready_to_kean_df['characteristic'] = 'ramp_up_cold_hours'

            total_plant_temp_df = total_plant_temp_df.append(rfc_temp_ready_to_kean_df)


            """ start_cost """
            start_cost_summer = row['start_expense'] * row['summer_duct_capacity']
            start_cost_winter = row['start_expense'] * row['winter_duct_capacity']
            sc_temp_ready_to_kean_df = temp_ready_to_kean_df
            sc_temp_ready_to_kean_df['value'] = sc_temp_ready_to_kean_df.apply(lambda row: dispatchUtils.get_load(row, start_cost_summer, start_cost_winter), axis=1)
            sc_temp_ready_to_kean_df['value_str'] = ''
            sc_temp_ready_to_kean_df['characteristic'] = 'start_cost'

            total_plant_temp_df = total_plant_temp_df.append(sc_temp_ready_to_kean_df)


            """ units """
            u_temp_char_df = temp_ready_to_kean_df
            u_temp_char_df['value'] = 1
            u_temp_char_df['value_str'] = ''
            u_temp_char_df['characteristic'] = 'units'
            total_plant_temp_df = total_plant_temp_df.append(u_temp_char_df)



            """ vom_high_load vom_max_load vom_min_load """
            vom = row['vom']
            for char in ['vom_high_load', 'vom_max_load', 'vom_min_load']:
                temp_char_df = temp_ready_to_kean_df
                temp_char_df['value'] = temp_char_df.apply(lambda row: dispatchUtils.get_escalated_value(vom, escalation, row['period']), axis=1)
                temp_char_df['value_str'] = ''
                temp_char_df['characteristic'] = char
                total_plant_temp_df = total_plant_temp_df.append(temp_char_df)

            total_plant_temp_df['entity'] = plant_name
            total_plant_temp_df['unit'] = 'all'
            total_plant_temp_df['scenario'] = scenario
            total_plant_temp_df['version'] = version


            ready_to_kean_pcuc_df = ready_to_kean_pcuc_df.append(total_plant_temp_df)

        if push_to_kean:
            dbPCUC.put_characteristics(ready_to_kean_pcuc_df, scenario, version)

        return ready_to_kean_pcuc_df


    """ Liquidity related operations """


















# #
