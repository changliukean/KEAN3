import pandas as pd
import numpy as np
from utility.dateUtils import get_month_list
from database.dbPrices import get_historical_lmp

from dateutil.relativedelta import relativedelta
from datetime import date, datetime
import sys



def get_hr(row, heatrate_summer, heatrate_winter):
    period = row['period']
    if period.month < 5 or period.month > 9:
        return heatrate_winter / 1000.0
    else:
        return heatrate_summer / 1000.0



def get_load(row, load_summer, load_winter):
    period = row['period']
    if period.month < 5 or period.month > 9:
        return load_winter
    else:
        return load_summer



def get_outage_days(row, outage_start_date, outage_end_date):
    period = row['period']
    period_start = date(period.year, period.month, 1)

    if pd.isnull(outage_start_date) or pd.isnull(outage_end_date):
        return 0


    outage_start_date = date(period.year, outage_start_date.month, outage_start_date.day)
    outage_end_date = date(period.year, outage_end_date.month, outage_end_date.day)

    if outage_start_date > period or outage_end_date < period_start:
        return 0

    if outage_start_date <= period_start and outage_end_date >= period:
        return period.day

    if outage_start_date <= period_start and outage_end_date < period:
        return relativedelta(outage_end_date, outage_start_date).days + 1

    if outage_start_date > period_start and outage_end_date >= period:
        return relativedelta(period, outage_start_date).days + 1





def get_escalated_value(value, escalation, period):

    return value * (1 + escalation) ** (period.year - 2020)






def convert_uc(plant_tech_master_file, scenario, version, start_date, end_date, escalation=0.02):
    simple_uc_df = pd.read_excel(plant_tech_master_file, sheet_name='Simple UC')
    tech_df = pd.read_excel(plant_tech_master_file, sheet_name='Tech')
    month_list = get_month_list(start_date, end_date)

    # for month in month_list:
    #     print (month)

    merged_simple_uc_df = pd.merge(simple_uc_df, tech_df, on='Tech', how="left")
    # merged_simple_uc_df.to_csv("merged_simple_uc_df.csv")

    ready_to_kean_pcuc_df = pd.DataFrame()

    for index, row in merged_simple_uc_df.iterrows():
        plant_name = row['Plant']
        total_plant_temp_df = pd.DataFrame()
        temp_ready_to_kean_df = pd.DataFrame(data=month_list, columns=['period'])

        """ emissions """
        emissions = row['Carbon Cost ($/Ton)'] * row['Emissions Rate (lb/MMBtu)'] / 2000.0
        emissions_temp_ready_to_kean_df = temp_ready_to_kean_df
        emissions_temp_ready_to_kean_df['characteristic'] = 'emissions'
        emissions_temp_ready_to_kean_df['value'] = emissions_temp_ready_to_kean_df.apply(lambda row: get_escalated_value(emissions, escalation, row['period']), axis=1)
        emissions_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(emissions_temp_ready_to_kean_df)



        """ forced_outage_value """
        forced_outage_value = row['UOF']
        fov_temp_ready_to_kean_df = temp_ready_to_kean_df
        fov_temp_ready_to_kean_df['characteristic'] = 'forced_outage_value'
        fov_temp_ready_to_kean_df['value'] = forced_outage_value
        fov_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(fov_temp_ready_to_kean_df)

        """ fuel_transport """
        fuel_transport = row['Fuel Basis ($/MMBtu)']
        ftp_temp_ready_to_kean_df = temp_ready_to_kean_df
        ftp_temp_ready_to_kean_df['characteristic'] = 'fuel_transport'
        ftp_temp_ready_to_kean_df['value'] = fuel_transport
        ftp_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(ftp_temp_ready_to_kean_df)

        """ fuel_type """
        fuel_type = row['Fuel Type']
        ft_temp_ready_to_kean_df = temp_ready_to_kean_df
        ft_temp_ready_to_kean_df['characteristic'] = 'fuel_type'
        ft_temp_ready_to_kean_df['value'] = 0.0
        ft_temp_ready_to_kean_df['value_str'] = fuel_type

        total_plant_temp_df = total_plant_temp_df.append(ft_temp_ready_to_kean_df)

        """ gas_instrument_id """
        gas_instrument_id = row['Fuel Hub']
        gii_temp_ready_to_kean_df = temp_ready_to_kean_df
        gii_temp_ready_to_kean_df['characteristic'] = 'gas_instrument_id'
        gii_temp_ready_to_kean_df['value'] = 0.0
        gii_temp_ready_to_kean_df['value_str'] = gas_instrument_id

        total_plant_temp_df = total_plant_temp_df.append(gii_temp_ready_to_kean_df)


        """ heatrate_high_load """
        heatrate_high_load_summer = row['Summer Base Heat Rate']
        heatrate_high_load_winter = row['Winter Base Heat Rate']

        hhl_temp_ready_to_kean_df = temp_ready_to_kean_df
        hhl_temp_ready_to_kean_df['value'] = hhl_temp_ready_to_kean_df.apply(lambda row: get_hr(row, heatrate_high_load_summer, heatrate_high_load_winter), axis=1)
        hhl_temp_ready_to_kean_df['characteristic'] = 'heatrate_high_load'
        hhl_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(hhl_temp_ready_to_kean_df)


        """ heatrate_max_load """
        heatrate_max_load_summer = row['Summer Duct Heat Rate']
        heatrate_max_load_winter = row['Winter Duct Heat Rate']

        hml_temp_ready_to_kean_df = temp_ready_to_kean_df
        hml_temp_ready_to_kean_df['value'] = hml_temp_ready_to_kean_df.apply(lambda row: get_hr(row, heatrate_max_load_summer, heatrate_max_load_winter), axis=1)
        hml_temp_ready_to_kean_df['characteristic'] = 'heatrate_max_load'
        hml_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(hml_temp_ready_to_kean_df)


        """ heatrate_min_load """
        heatrate_min_load_summer = row['Summer Base Heat Rate'] * row['Lower Operating Limit - Summer Heat Rate']
        heatrate_min_load_winter = row['Winter Base Heat Rate'] * row['Lower Operating Limit - Winter Heat Rate']

        hminl_temp_ready_to_kean_df = temp_ready_to_kean_df
        hminl_temp_ready_to_kean_df['value'] = hminl_temp_ready_to_kean_df.apply(lambda row: get_hr(row, heatrate_min_load_summer, heatrate_min_load_winter), axis=1)
        hminl_temp_ready_to_kean_df['characteristic'] = 'heatrate_min_load'
        hminl_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(hminl_temp_ready_to_kean_df)


        """ high_load """
        high_load_summer = row['Summer Base Capacity']
        high_load_winter = row['Winter Base Capacity']

        hl_temp_ready_to_kean_df = temp_ready_to_kean_df
        hl_temp_ready_to_kean_df['value'] = hl_temp_ready_to_kean_df.apply(lambda row: get_load(row, high_load_summer, high_load_winter), axis=1)
        hl_temp_ready_to_kean_df['characteristic'] = 'high_load'
        hl_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(hl_temp_ready_to_kean_df)


        """ max_load """
        max_load_summer = row['Summer Duct Capacity']
        max_load_winter = row['Winter Duct Capacity']

        ml_temp_ready_to_kean_df = temp_ready_to_kean_df
        ml_temp_ready_to_kean_df['value'] = ml_temp_ready_to_kean_df.apply(lambda row: get_load(row, max_load_summer, max_load_winter), axis=1)
        ml_temp_ready_to_kean_df['characteristic'] = 'max_load'
        ml_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(ml_temp_ready_to_kean_df)



        """ min_load """
        min_load_summer = row['Summer Base Capacity'] * row['Lower Operating Limit - Capacity']
        min_load_winter = row['Winter Base Capacity'] * row['Lower Operating Limit - Capacity']

        ml_temp_ready_to_kean_df = temp_ready_to_kean_df
        ml_temp_ready_to_kean_df['value'] = ml_temp_ready_to_kean_df.apply(lambda row: get_load(row, min_load_summer, min_load_winter), axis=1)
        ml_temp_ready_to_kean_df['characteristic'] = 'min_load'
        ml_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(ml_temp_ready_to_kean_df)



        """ offpeak_power_hub_instrument_id """
        offpeak_power_hub_instrument_id = row['Power Hub - Off Peak']
        oph_temp_ready_to_kean_df = temp_ready_to_kean_df
        oph_temp_ready_to_kean_df['value_str'] = offpeak_power_hub_instrument_id
        oph_temp_ready_to_kean_df['value'] = 0.0
        oph_temp_ready_to_kean_df['characteristic'] = 'offpeak_power_hub_instrument_id'

        total_plant_temp_df = total_plant_temp_df.append(oph_temp_ready_to_kean_df)


        """ onpeak_power_hub_instrument_id """
        onpeak_power_hub_instrument_id = row['Power Hub - On Peak']
        onph_temp_ready_to_kean_df = temp_ready_to_kean_df
        onph_temp_ready_to_kean_df['value_str'] = onpeak_power_hub_instrument_id
        onph_temp_ready_to_kean_df['value'] = 0.0
        onph_temp_ready_to_kean_df['characteristic'] = 'onpeak_power_hub_instrument_id'

        total_plant_temp_df = total_plant_temp_df.append(onph_temp_ready_to_kean_df)


        """ outage_days """
        outage_start_date = row['Planned Outage Start Date']
        outage_end_date = row['Planned Outage End Date']
        od_temp_ready_to_kean_df = temp_ready_to_kean_df
        od_temp_ready_to_kean_df['value'] = od_temp_ready_to_kean_df.apply(lambda row: get_outage_days(row, outage_start_date, outage_end_date), axis=1)
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
        ramp_fuel_cold_summer = row['Start Fuel (MMBtu/MW)'] * row['Summer Duct Capacity']
        ramp_fuel_cold_winter = row['Start Fuel (MMBtu/MW)'] * row['Winter Duct Capacity']
        rfc_temp_ready_to_kean_df = temp_ready_to_kean_df
        rfc_temp_ready_to_kean_df['value'] = rfc_temp_ready_to_kean_df.apply(lambda row: get_load(row, ramp_fuel_cold_summer, ramp_fuel_cold_winter), axis=1)
        rfc_temp_ready_to_kean_df['value_str'] = ''
        rfc_temp_ready_to_kean_df['characteristic'] = 'ramp_fuel_cold'

        total_plant_temp_df = total_plant_temp_df.append(rfc_temp_ready_to_kean_df)


        """ ramp_up_cold_hours """
        ramp_up_cold_hours = row['Start Hours']
        ruch_temp_ready_to_kean_df = temp_ready_to_kean_df
        ruch_temp_ready_to_kean_df['value'] = ramp_up_cold_hours
        ruch_temp_ready_to_kean_df['value_str'] = ''
        ruch_temp_ready_to_kean_df['characteristic'] = 'ramp_up_cold_hours'

        total_plant_temp_df = total_plant_temp_df.append(rfc_temp_ready_to_kean_df)


        """ start_cost """
        start_cost_summer = row['Start Expense ($/MW)'] * row['Summer Duct Capacity']
        start_cost_winter = row['Start Expense ($/MW)'] * row['Winter Duct Capacity']
        sc_temp_ready_to_kean_df = temp_ready_to_kean_df
        sc_temp_ready_to_kean_df['value'] = sc_temp_ready_to_kean_df.apply(lambda row: get_load(row, start_cost_summer, start_cost_winter), axis=1)
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
        vom = row['VOM']
        for char in ['vom_high_load', 'vom_max_load', 'vom_min_load']:
            temp_char_df = temp_ready_to_kean_df
            temp_char_df['value'] = temp_char_df.apply(lambda row: get_escalated_value(vom, escalation, row['period']), axis=1)
            temp_char_df['value_str'] = ''
            temp_char_df['characteristic'] = char
            total_plant_temp_df = total_plant_temp_df.append(temp_char_df)

        total_plant_temp_df['entity'] = plant_name
        total_plant_temp_df['unit'] = 'all'
        total_plant_temp_df['scenario'] = scenario
        total_plant_temp_df['version'] = version


        ready_to_kean_pcuc_df = ready_to_kean_pcuc_df.append(total_plant_temp_df)



    return ready_to_kean_pcuc_df







def convert_uc_dataframe(powerplant_df, technology_df, scenario, version, start_date, end_date, escalation=0.02):

    month_list = get_month_list(start_date, end_date)

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
        emissions_temp_ready_to_kean_df['value'] = emissions_temp_ready_to_kean_df.apply(lambda row: get_escalated_value(emissions, escalation, row['period']), axis=1)
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
        ftp_temp_ready_to_kean_df['value'] = ftp_temp_ready_to_kean_df.apply(lambda row: get_load(row, fuel_transport_summer, fuel_transport_winter), axis=1)
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
        hhl_temp_ready_to_kean_df['value'] = hhl_temp_ready_to_kean_df.apply(lambda row: get_hr(row, heatrate_high_load_summer, heatrate_high_load_winter), axis=1)
        hhl_temp_ready_to_kean_df['characteristic'] = 'heatrate_high_load'
        hhl_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(hhl_temp_ready_to_kean_df)


        """ heatrate_max_load """
        heatrate_max_load_summer = row['summer_duct_heatrate']
        heatrate_max_load_winter = row['winter_duct_heatrate']

        hml_temp_ready_to_kean_df = temp_ready_to_kean_df
        hml_temp_ready_to_kean_df['value'] = hml_temp_ready_to_kean_df.apply(lambda row: get_hr(row, heatrate_max_load_summer, heatrate_max_load_winter), axis=1)
        hml_temp_ready_to_kean_df['characteristic'] = 'heatrate_max_load'
        hml_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(hml_temp_ready_to_kean_df)


        """ heatrate_min_load """
        heatrate_min_load_summer = row['summer_base_heatrate'] * row['lol_summer_heatrate']
        heatrate_min_load_winter = row['winter_base_heatrate'] * row['lol_winter_heatrate']

        hminl_temp_ready_to_kean_df = temp_ready_to_kean_df
        hminl_temp_ready_to_kean_df['value'] = hminl_temp_ready_to_kean_df.apply(lambda row: get_hr(row, heatrate_min_load_summer, heatrate_min_load_winter), axis=1)
        hminl_temp_ready_to_kean_df['characteristic'] = 'heatrate_min_load'
        hminl_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(hminl_temp_ready_to_kean_df)


        """ high_load """
        high_load_summer = row['summer_base_capacity']
        high_load_winter = row['winter_base_capacity']

        hl_temp_ready_to_kean_df = temp_ready_to_kean_df
        hl_temp_ready_to_kean_df['value'] = hl_temp_ready_to_kean_df.apply(lambda row: get_load(row, high_load_summer, high_load_winter), axis=1)
        hl_temp_ready_to_kean_df['characteristic'] = 'high_load'
        hl_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(hl_temp_ready_to_kean_df)


        """ max_load """
        max_load_summer = row['summer_duct_capacity']
        max_load_winter = row['winter_duct_capacity']

        ml_temp_ready_to_kean_df = temp_ready_to_kean_df
        ml_temp_ready_to_kean_df['value'] = ml_temp_ready_to_kean_df.apply(lambda row: get_load(row, max_load_summer, max_load_winter), axis=1)
        ml_temp_ready_to_kean_df['characteristic'] = 'max_load'
        ml_temp_ready_to_kean_df['value_str'] = ''

        total_plant_temp_df = total_plant_temp_df.append(ml_temp_ready_to_kean_df)



        """ min_load """
        min_load_summer = row['summer_base_capacity'] * row['lol_capacity']
        min_load_winter = row['winter_base_capacity'] * row['lol_capacity']

        ml_temp_ready_to_kean_df = temp_ready_to_kean_df
        ml_temp_ready_to_kean_df['value'] = ml_temp_ready_to_kean_df.apply(lambda row: get_load(row, min_load_summer, min_load_winter), axis=1)
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
        od_temp_ready_to_kean_df['value'] = od_temp_ready_to_kean_df.apply(lambda row: get_outage_days(row, outage_start_date, outage_end_date), axis=1)
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
        rfc_temp_ready_to_kean_df['value'] = rfc_temp_ready_to_kean_df.apply(lambda row: get_load(row, ramp_fuel_cold_summer, ramp_fuel_cold_winter), axis=1)
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
        sc_temp_ready_to_kean_df['value'] = sc_temp_ready_to_kean_df.apply(lambda row: get_load(row, start_cost_summer, start_cost_winter), axis=1)
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
            temp_char_df['value'] = temp_char_df.apply(lambda row: get_escalated_value(vom, escalation, row['period']), axis=1)
            temp_char_df['value_str'] = ''
            temp_char_df['characteristic'] = char
            total_plant_temp_df = total_plant_temp_df.append(temp_char_df)

        total_plant_temp_df['entity'] = plant_name
        total_plant_temp_df['unit'] = 'all'
        total_plant_temp_df['scenario'] = scenario
        total_plant_temp_df['version'] = version


        ready_to_kean_pcuc_df = ready_to_kean_pcuc_df.append(total_plant_temp_df)



    return ready_to_kean_pcuc_df







def load_pp_tech_info(plant_tech_master_file):
    simple_uc_df = pd.read_excel(plant_tech_master_file, sheet_name='Simple UC')
    tech_df = pd.read_excel(plant_tech_master_file, sheet_name='Tech')

    """ powerplant table """

    simple_uc_df.rename(columns={'Plant':'name',
                                 'Tech':'technology',
                                 'Fuel Type':'fuel_type',
                                 'Market':'market',
                                 'Power Hub/Zone':'power_zone',
                                 'Power Hub - On Peak':'power_hub_on_peak',
                                 'Power Hub - Off Peak':'power_hub_off_peak',
                                 'Power Hub - SNL':'power_hub',
                                 'Node':'node',
                                 'Fuel Zone':'fuel_zone',
                                 'Fuel Hub':'fuel_hub',
                                 'Summer Fuel Basis ($/MMBtu)':'summer_fuel_basis',
                                 'Winter Fuel Basis ($/MMBtu)':'winter_fuel_basis',
                                 'Summer Duct Capacity':'summer_duct_capacity',
                                 'Summer Base Capacity':'summer_base_capacity',
                                 'Winter Duct Capacity':'winter_duct_capacity',
                                 'Winter Base Capacity':'winter_base_capacity',
                                 'Planned Outage Start Date':'first_plan_outage_start',
                                 'Planned Outage End Date':'first_plan_outage_end',
                                 'Carbon Cost ($/Ton)':'carbon_cost',
                                 'Retirement Date':'retirement_date',
                                 'Ownership':'ownership',
                                 'Source Notes':'source_notes'}, inplace=True)

    # simple_uc_df.to_csv("simple_uc_df.csv")

    simple_uc_df['retirement_date'] = simple_uc_df.apply(lambda row: date(2099,12,31) if pd.isnull(row['retirement_date']) else row['retirement_date'], axis=1)

    simple_uc_df['second_plan_outage_start'] = ''
    simple_uc_df['second_plan_outage_end'] = ''

    simple_uc_df['effective_start'] = date(2000,1,1)
    simple_uc_df['effective_end'] = date(2099,12,31)

    ready_to_kean_pp_df = simple_uc_df
    """ technology table """
    tech_df.rename(columns={'Tech': 'name',
                            'Summer Duct Heat Rate': 'summer_duct_heatrate',
                            'Summer Base Heat Rate': 'summer_base_heatrate',
                            'Winter Duct Heat Rate': 'winter_duct_heatrate',
                            'Winter Base Heat Rate': 'winter_base_heatrate',
                            'Lower Operating Limit - Capacity': 'lol_capacity',
                            'Lower Operating Limit - Summer Heat Rate': 'lol_summer_heatrate',
                            'Lower Operating Limit - Winter Heat Rate': 'lol_winter_heatrate',
                            'Start Expense ($/MW)': 'start_expense',
                            'Start Fuel (MMBtu/MW)': 'start_fuel',
                            'Start Hours': 'start_hours',
                            'Emissions Rate (lb/MMBtu)': 'emissions_rate',
                            'VOM': 'vom',
                            'UOF': 'uof'}, inplace=True)


    tech_df = tech_df.set_index('name')
    tech_df.fillna(0.0, inplace=True)
    tech_df = tech_df.reset_index()
    ready_to_kean_tech_df = tech_df


    return ready_to_kean_pp_df, ready_to_kean_tech_df










def get_match_signal(row):
    if np.isnan(row['total_lmp_x']) or np.isnan(row['total_lmp_y']):
        return 'Not matched'

    return 'Matched'




def get_month(row):
    return row['delivery_date'].month





def calculate_basis(nodal_market, nodal_id, hub_market, hub_id, start_date, end_date, dart, plant_name):
    nodal_lmp_df = get_historical_lmp(nodal_market, nodal_id, start_date, end_date, dart)
    hub_lmp_df = get_historical_lmp(hub_market, hub_id, start_date, end_date, dart)
    # nodal_lmp_df.to_csv("nodal_lmp_df.csv")
    # hub_lmp_df.to_csv("hub_lmp_df.csv")

    print ("------------------------------------------------")
    print (nodal_market, nodal_id, len(nodal_lmp_df))
    print (hub_market, hub_id, len(hub_lmp_df))

    merged_hub_nodal_lmp_df = pd.merge(nodal_lmp_df, hub_lmp_df, on=['delivery_date','hour_ending'], how='inner')

    # merged_hub_nodal_lmp_df.to_csv("merged_hub_nodal_lmp_df.csv")

    merged_hub_nodal_lmp_df['signal'] = merged_hub_nodal_lmp_df.apply(lambda row: get_match_signal(row), axis=1)

    merged_hub_nodal_lmp_df.rename(columns={'total_lmp_x':'nodal_lmp','total_lmp_y':'hub_lmp', 'peak_info_x': 'peak_info'}, inplace=True)

    merged_hub_nodal_lmp_df['month'] = merged_hub_nodal_lmp_df.apply(lambda row: get_month(row), axis=1)


    merged_hub_nodal_lmp_df = merged_hub_nodal_lmp_df[['delivery_date','hour_ending', 'month', 'nodal_lmp','hub_lmp','signal', 'peak_info']]
    merged_hub_nodal_lmp_df['basis_$'] = (merged_hub_nodal_lmp_df['nodal_lmp'] - merged_hub_nodal_lmp_df['hub_lmp'])
    merged_hub_nodal_lmp_df['basis_%'] = (merged_hub_nodal_lmp_df['nodal_lmp'] - merged_hub_nodal_lmp_df['hub_lmp']) / merged_hub_nodal_lmp_df['hub_lmp']

    merged_hub_nodal_lmp_df['basis_$'] = merged_hub_nodal_lmp_df.apply(lambda row: np.nan if abs(row['basis_%']) > 0.5 else row['basis_$'], axis=1)
    merged_hub_nodal_lmp_df['basis_%'] = merged_hub_nodal_lmp_df.apply(lambda row: np.nan if abs(row['basis_%']) > 0.5 else row['basis_%'], axis=1)



    merged_hub_nodal_lmp_df = merged_hub_nodal_lmp_df.replace([np.inf, -np.inf], 0.0)


    # merged_hub_nodal_lmp_df.to_csv("result.csv")
    merged_hub_nodal_lmp_df['plant'] = plant_name

    monthly_onoffpeak_basis_df = merged_hub_nodal_lmp_df.groupby(['month','peak_info'])[['basis_$','basis_%']].mean()
    monthly_onoffpeak_basis_df['plant'] = plant_name

    return monthly_onoffpeak_basis_df, merged_hub_nodal_lmp_df








def load_solar_dispatch(portfolio, scenario, version, plant_name, assumptions_file):
    solar_assumptions_df = pd.read_excel(assumptions_file, sheet_name='kean_load_solar')
    plant_assumptions_df = solar_assumptions_df.loc[solar_assumptions_df.plant == plant_name]

    melt_plant_assumptions_df = pd.melt(plant_assumptions_df, id_vars=['plant','fsli'],
                                        value_vars=[item for item in list(plant_assumptions_df.columns) if item not in ['plant','fsli']],
                                        var_name='period',
                                        value_name='value')

    melt_plant_assumptions_df = melt_plant_assumptions_df.reset_index()

    melt_plant_assumptions_df = melt_plant_assumptions_df[['plant','fsli','period','value']]

    melt_plant_assumptions_df = pd.pivot_table(melt_plant_assumptions_df, index=['plant','period'], columns=['fsli'], values='value', aggfunc=np.sum)

    melt_plant_assumptions_df = melt_plant_assumptions_df.reset_index()

    melt_plant_assumptions_df['Generation'] = melt_plant_assumptions_df['ICAP'] * (melt_plant_assumptions_df['Hours - On Peak'] + melt_plant_assumptions_df['Hours - Off Peak']) * melt_plant_assumptions_df['Capacity Factor']
    melt_plant_assumptions_df['Generation - On Peak'] = melt_plant_assumptions_df['ICAP'] * melt_plant_assumptions_df['Hours - On Peak'] * melt_plant_assumptions_df['Capacity Factor']
    melt_plant_assumptions_df['Generation - Off Peak'] = melt_plant_assumptions_df['ICAP'] * melt_plant_assumptions_df['Hours - Off Peak'] * melt_plant_assumptions_df['Capacity Factor']


    melt_plant_assumptions_df['Energy Revenue'] = melt_plant_assumptions_df['Generation'] * melt_plant_assumptions_df['PPA']

    melt_plant_assumptions_df['Realized Power Price - Off Peak'] = melt_plant_assumptions_df['PPA']
    melt_plant_assumptions_df['Realized Power Price - On Peak'] = melt_plant_assumptions_df['PPA']

    melt_plant_assumptions_df['Capacity Factor - On Peak'] = melt_plant_assumptions_df['Capacity Factor']
    melt_plant_assumptions_df['Capacity Factor - Off Peak'] = melt_plant_assumptions_df['Capacity Factor']

    melt_plant_assumptions_df['Delivered Fuel Expense'] = 0.0
    melt_plant_assumptions_df['Variable O&M Expense'] = 0.0
    melt_plant_assumptions_df['Net Emissions Expense'] = 0.0

    # solar_dispatch_df = melt_plant_assumptions_df
    melt_plant_assumptions_df.to_csv("tttt.csv")
    # solar_dispatch_df.to_csv("solar_dispatch_df.csv")

    solar_dispatch_df = pd.melt(melt_plant_assumptions_df,id_vars=['plant','period'],
                                value_vars=[item for item in list(melt_plant_assumptions_df.columns) if item not in ['plant','period']],
                                var_name='fsli',
                                value_name='value')

    solar_dispatch_df = solar_dispatch_df.reset_index()
    solar_dispatch_df['company'] = portfolio
    solar_dispatch_df['entity'] = solar_dispatch_df['plant']
    solar_dispatch_df['scenario'] = scenario
    solar_dispatch_df['version'] = version

    solar_dispatch_df = solar_dispatch_df[['company','scenario','version','entity','fsli','period','value']]

    # solar_dispatch_df.to_csv("solar_dispatch_df.csv")

    return solar_dispatch_df





def load_nuclear_dispatch(portfolio, scenario, version, plant_name, assumptions_file):
    nuclear_assumptions_df = pd.read_excel(assumptions_file, sheet_name='kean_load_nuclear')
    plant_assumptions_df = nuclear_assumptions_df.loc[nuclear_assumptions_df.plant == plant_name]

    melt_plant_assumptions_df = pd.melt(plant_assumptions_df, id_vars=['plant','fsli'],
                                        value_vars=[item for item in list(plant_assumptions_df.columns) if item not in ['plant','fsli']],
                                        var_name='period',
                                        value_name='value')

    melt_plant_assumptions_df = melt_plant_assumptions_df.reset_index()

    melt_plant_assumptions_df = melt_plant_assumptions_df[['plant','fsli','period','value']]

    melt_plant_assumptions_df = pd.pivot_table(melt_plant_assumptions_df, index=['plant','period'], columns=['fsli'], values='value', aggfunc=np.sum)

    melt_plant_assumptions_df = melt_plant_assumptions_df.reset_index()

    melt_plant_assumptions_df['Generation'] = melt_plant_assumptions_df['ICAP'] * (melt_plant_assumptions_df['Hours - On Peak'] + melt_plant_assumptions_df['Hours - Off Peak']) * melt_plant_assumptions_df['Capacity Factor']
    melt_plant_assumptions_df['Generation - On Peak'] = melt_plant_assumptions_df['ICAP'] * melt_plant_assumptions_df['Hours - On Peak'] * melt_plant_assumptions_df['Capacity Factor']
    melt_plant_assumptions_df['Generation - Off Peak'] = melt_plant_assumptions_df['ICAP'] * melt_plant_assumptions_df['Hours - Off Peak'] * melt_plant_assumptions_df['Capacity Factor']


    melt_plant_assumptions_df['Energy Revenue - On Peak'] = melt_plant_assumptions_df['Generation - On Peak'] * (melt_plant_assumptions_df['Hub Price - On Peak'] * ( 1 + melt_plant_assumptions_df['Basis_% - On Peak']))
    melt_plant_assumptions_df['Energy Revenue - Off Peak'] = melt_plant_assumptions_df['Generation - Off Peak'] * (melt_plant_assumptions_df['Hub Price - Off Peak'] * ( 1 + melt_plant_assumptions_df['Basis_% - Off Peak']))

    melt_plant_assumptions_df['Energy Revenue'] = melt_plant_assumptions_df['Energy Revenue - On Peak'] + melt_plant_assumptions_df['Energy Revenue - Off Peak']
    melt_plant_assumptions_df['Delivered Fuel Expense'] = melt_plant_assumptions_df['Generation'] * melt_plant_assumptions_df['Fuel Costs']
    melt_plant_assumptions_df['Variable O&M Expense'] = melt_plant_assumptions_df['Generation'] * melt_plant_assumptions_df['VOM']
    melt_plant_assumptions_df['Net Emissions Expense'] = 0.0

    melt_plant_assumptions_df['Realized Power Price - Off Peak'] = melt_plant_assumptions_df['Hub Price - Off Peak'] * ( 1 + melt_plant_assumptions_df['Basis_% - Off Peak'])
    melt_plant_assumptions_df['Realized Power Price - On Peak'] = melt_plant_assumptions_df['Hub Price - On Peak'] * ( 1 + melt_plant_assumptions_df['Basis_% - On Peak'])

    melt_plant_assumptions_df['Capacity Factor - On Peak'] = melt_plant_assumptions_df['Capacity Factor']
    melt_plant_assumptions_df['Capacity Factor - Off Peak'] = melt_plant_assumptions_df['Capacity Factor']
    melt_plant_assumptions_df.rename(columns={'Hours - On Peak':'on_hours','Hours - Off Peak':'off_hours'}, inplace=True)

    # solar_dispatch_df = melt_plant_assumptions_df
    # melt_plant_assumptions_df.to_csv("tttt.csv")
    # sys.exit()
    # solar_dispatch_df.to_csv("solar_dispatch_df.csv")

    nuclear_dispatch_df = pd.melt(melt_plant_assumptions_df,id_vars=['plant','period'],
                                  value_vars=[item for item in list(melt_plant_assumptions_df.columns) if item not in ['plant','period']],
                                  var_name='fsli',
                                  value_name='value')



    nuclear_dispatch_df = nuclear_dispatch_df.reset_index()
    nuclear_dispatch_df['company'] = portfolio
    nuclear_dispatch_df['entity'] = nuclear_dispatch_df['plant']
    nuclear_dispatch_df['scenario'] = scenario
    nuclear_dispatch_df['version'] = version

    nuclear_dispatch_df = nuclear_dispatch_df[['company','scenario','version','entity','fsli','period','value']]

    return nuclear_dispatch_df


# #
