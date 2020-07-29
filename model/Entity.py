from database import dbPrices, dbLBO
import numpy as np
from datetime import datetime, date, timedelta
import pandas as pd
import sys



class Entity:

    def __init__(self, name, type):
        self.name = name
        self.type = type




def get_match_signal(row):
    if np.isnan(row['total_lmp_x']) or np.isnan(row['total_lmp_y']):
        return 'Not matched'

    return 'Matched'


class Powerplant(Entity):
    def __init__(self, name, fuel_type, market, node, power_hub,
                 technology=None,
                 power_zone='',
                 power_hub_on_peak='',
                 power_hub_off_peak='',
                 fuel_zone='',
                 fuel_hub='',
                 summer_fuel_basis=0.0,
                 winter_fuel_basis=0.0,
                 summer_duct_capacity=0.0,
                 summer_base_capacity=0.0,
                 winter_duct_capacity=0.0,
                 winter_base_capacity=0.0,
                 first_plan_outage_start=date(1900,1,1),
                 first_plan_outage_end=date(1900,1,1),
                 second_plan_outage_start=date(1900,1,1),
                 second_plan_outage_end=date(1900,1,1),
                 carbon_cost=0.0,
                 source_notes='',
                 retirement_date=date(1900,1,1),
                 ownership=0.0):

        Entity.__init__(self, name, 'plant')
        self.technology = technology
        self.fuelType = fuel_type
        self.market = market
        self.node = node    # power node name
        self.powerHub = power_hub   # power hub name
        self.powerZone = power_zone
        self.powerHubOnPeak = power_hub_on_peak
        self.powerHubOffPeak = power_hub_off_peak
        self.fuelZone = fuel_zone
        self.fuelHub = fuel_hub
        self.summerFuelBasis = summer_fuel_basis
        self.winterFuelBasis = winter_fuel_basis
        self.summerDuctCapacity = summer_duct_capacity
        self.summerBaseCapacity = summer_base_capacity
        self.winterDuctCapacity = winter_duct_capacity
        self.winterBaseCapacity = winter_base_capacity
        self.firstPlanOutageStart = first_plan_outage_start
        self.firstPlanOutageEnd = first_plan_outage_end
        self.secondPlanOutageStart = second_plan_outage_start
        self.secondPlanOutageEnd = second_plan_outage_end
        self.carbonCost = carbon_cost
        self.sourceNotes = source_notes
        self.retirementDate = retirement_date
        self.ownership = ownership


    def build_basis(self, start_date, end_date, dart, outlier_absolute_limit=0.5, replace_inf=np.nan):
        nodal_lmp_df = dbPrices.get_historical_lmp(self.node, start_date, end_date, dart)
        hub_lmp_df = dbPrices.get_historical_lmp(self.powerHub, start_date, end_date, dart)
        print ("------------------------------------------------")
        print (self.market, self.node, len(nodal_lmp_df))
        print (self.market, self.powerHub, len(hub_lmp_df))

        if len(nodal_lmp_df) == 0 or len(hub_lmp_df) == 0 or nodal_lmp_df is None or hub_lmp_df is None:
            return pd.DataFrame(), pd.DataFrame()

        merged_hub_nodal_lmp_df = pd.merge(nodal_lmp_df, hub_lmp_df, on=['delivery_date','hour_ending'], how='inner')

        merged_hub_nodal_lmp_df['signal'] = merged_hub_nodal_lmp_df.apply(lambda row: get_match_signal(row), axis=1)

        merged_hub_nodal_lmp_df.rename(columns={'total_lmp_x':'nodal_lmp','total_lmp_y':'hub_lmp', 'peak_info_x': 'peak_info'}, inplace=True)

        merged_hub_nodal_lmp_df['month'] = merged_hub_nodal_lmp_df.apply(lambda row: row['delivery_date'].month, axis=1)


        merged_hub_nodal_lmp_df = merged_hub_nodal_lmp_df[['delivery_date','hour_ending', 'month', 'nodal_lmp','hub_lmp','signal', 'peak_info']]
        merged_hub_nodal_lmp_df['basis_$'] = (merged_hub_nodal_lmp_df['nodal_lmp'] - merged_hub_nodal_lmp_df['hub_lmp'])
        merged_hub_nodal_lmp_df['basis_%'] = (merged_hub_nodal_lmp_df['nodal_lmp'] - merged_hub_nodal_lmp_df['hub_lmp']) / merged_hub_nodal_lmp_df['hub_lmp']

        merged_hub_nodal_lmp_df['basis_$'] = merged_hub_nodal_lmp_df.apply(lambda row: np.nan if abs(row['basis_%']) > outlier_absolute_limit else row['basis_$'], axis=1)
        merged_hub_nodal_lmp_df['basis_%'] = merged_hub_nodal_lmp_df.apply(lambda row: np.nan if abs(row['basis_%']) > outlier_absolute_limit else row['basis_%'], axis=1)

        merged_hub_nodal_lmp_df = merged_hub_nodal_lmp_df.replace([np.inf, -np.inf], replace_inf)
        merged_hub_nodal_lmp_df['plant'] = self.name
        monthly_onoffpeak_basis_df = merged_hub_nodal_lmp_df.groupby(['month','peak_info'])[['basis_$','basis_%']].mean()
        monthly_onoffpeak_basis_df['plant'] = self.name

        # monthly_onoffpeak_basis_df.to_csv("monthly_onoffpeak_basis_df.csv")
        # merged_hub_nodal_lmp_df.to_csv("merged_hub_nodal_lmp_df.csv")

        return monthly_onoffpeak_basis_df, merged_hub_nodal_lmp_df


    def save(self):
        effective_end = date(2099,12,31)
        today_date = datetime.now().date()
        effective_start = today_date

        id_powerplant = []
        existing_record_df = dbLBO.get_powerplant(self.name, self.fuelType, self.market, self.node, self.powerHub, today_date)

        ready_to_kean_pp_df = pd.DataFrame()
        if existing_record_df is not None and len(existing_record_df) > 0:
            currecord_effective_start = existing_record_df.iloc[0].effective_start
            currecord_effective_end = existing_record_df.iloc[0].effective_end
            id_powerplant.append(str(existing_record_df.iloc[0].id_powerplant))
            if currecord_effective_start < today_date and currecord_effective_end >= today_date:
                effective_start = today_date
                existing_record_df.set_value(0, 'effective_end', today_date - timedelta(1))
            if currecord_effective_start == today_date and currecord_effective_end >= today_date:
                effective_start = today_date
                existing_record_df = existing_record_df.drop([0])

            ready_to_kean_pp_df = existing_record_df


        technology_name = self.technology if isinstance(self.technology, str) else self.technology.name
        added_record_to_kean_pp_df = pd.DataFrame(columns=['name',
                                                           'fuel_type',
                                                           'market',
                                                           'node',
                                                           'power_hub',
                                                           'technology',
                                                           'power_zone',
                                                           'power_hub_on_peak',
                                                           'power_hub_off_peak',
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
                                                           'ownership',
                                                           'effective_start',
                                                           'effective_end'],
                                                    data=[[self.name,
                                                           self.fuelType,
                                                           self.market,
                                                           self.node,
                                                           self.powerHub,
                                                           technology_name,
                                                           self.powerZone,
                                                           self.powerHubOnPeak,
                                                           self.powerHubOffPeak,
                                                           self.fuelZone,
                                                           self.fuelHub,
                                                           self.summerFuelBasis,
                                                           self.winterFuelBasis,
                                                           self.summerDuctCapacity,
                                                           self.summerBaseCapacity,
                                                           self.winterDuctCapacity,
                                                           self.winterBaseCapacity,
                                                           self.firstPlanOutageStart,
                                                           self.firstPlanOutageEnd,
                                                           self.secondPlanOutageStart,
                                                           self.secondPlanOutageEnd,
                                                           self.carbonCost,
                                                           self.sourceNotes,
                                                           self.retirementDate,
                                                           self.ownership,
                                                           effective_start,
                                                           effective_end]])

        ready_to_kean_pp_df = ready_to_kean_pp_df.append(added_record_to_kean_pp_df, sort=False)

        dbLBO.put_powerplant(ready_to_kean_pp_df, id_powerplant)



class Holdco(Entity):
    def __init__(self, name):
        Entity.__init__(self, name, 'holdco')

    # to be implemented


class Technology():
    def __init__(self, name,
                 summer_duct_heatrate=0.0,
                 summer_base_heatrate=0.0,
                 winter_duct_heatrate=0.0,
                 winter_base_heatrate=0.0,
                 lol_capacity=0.0,
                 lol_summer_heatrate=0.0,
                 lol_winter_heatrate=0.0,
                 start_expense=0.0,
                 start_fuel=0.0,
                 start_hours=0.0,
                 emissions_rate=0.0,
                 vom=0.0,
                 uof=0.0):
        self.name = name
        self.summerDuctHeatrate = summer_duct_heatrate
        self.summerBaseHeatrate = summer_base_heatrate
        self.winterDuctHeatrate = winter_duct_heatrate
        self.winterBaseHeatrate = winter_base_heatrate
        self.lolCapacity = lol_capacity
        self.lolSummerHeatrate = lol_summer_heatrate
        self.lolWinterHeatrate = lol_winter_heatrate
        self.startExpense = start_expense
        self.startFuel = start_fuel
        self.startHours = start_hours
        self.emissionsRate = emissions_rate
        self.vom = vom
        self.uof = uof





















# #
