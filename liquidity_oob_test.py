from liquidity.Liquidity import OID, DFC, Liquidity, Debt, OperatingCompany
from datetime import date
import importlib
from reportwriter.ReportWriter import ReportWriter
from scipy.optimize import fsolve

import sys




def lightstone_test():
    # balance = 1725000000
    # begin_date = date(2017,2,1)
    # end_date = date(2024,1,31)
    # oid_discount = 98
    #
    # monthly_oid_payments = OID.calc_monthly_oid_payments(balance, begin_date, end_date, oid_discount)
    #
    # print (monthly_oid_payments)
    #
    #
    # balance = 300000000
    # begin_date = date(2018,9,1)
    # end_date = date(2024,1,31)
    # oid_discount = 99.5
    #
    # monthly_oid_payments = OID.calc_monthly_oid_payments(balance, begin_date, end_date, oid_discount)
    #
    # print (monthly_oid_payments)
    #
    # balance = 1725000000
    # begin_date = date(2017,2,1)
    # end_date = date(2024,1,31)
    # oid_discount = 98
    # initial_oid = OID(balance, begin_date, end_date, oid_discount)
    # monthly_oid_payments = initial_oid.build_monthly_oid_payments()
    #
    # print (monthly_oid_payments)
    #
    # balance = 300000000
    # begin_date = date(2018,9,1)
    # end_date = date(2024,1,31)
    # oid_discount = 99.5
    # upsize_oid = OID(balance, begin_date, end_date, oid_discount)
    # monthly_oid_payments = upsize_oid.build_monthly_oid_payments()
    # print (monthly_oid_payments)


    # balance = 1725000000
    # begin_date = date(2017,1,17)
    # end_date = date(2024,1,31)
    # dfc_rate = 0.04
    # initial_dfc = DFC(balance, begin_date, end_date, dfc_rate)
    # dfc_payments = initial_dfc.build_monthly_dfc_payments()
    #
    # print (dfc_payments)
    #

    portfolio = 'Lightstone'
    liquidity_scenario = '2020 Mar AMR Liquidity Test'
    liquidity_version = 'v1'

    lightstone_liquidity = Liquidity(portfolio,liquidity_scenario,liquidity_version)

    # print (len(lightstone_liquidity.capitalStructure))
    # for item in lightstone_liquidity.capitalStructure:
    #     print ("==================================================")
    #     if isinstance(item, Debt):
    #         print (item.instrumentID)
    #         print (" ---------------------  debt upsizes: -------------------  ")
    #         print (item.upsizes)
    #         print (" ---------------------  debt prepays: -------------------  ")
    #         print (item.prepays)
    #     if isinstance(item, OperatingCompany):
    #         print (" ---------------------  Operating Company: -------------------  ")
    #         print (item.portfolio)
    #         print (item.financialsScenario)
    #         print (item.financialsVersion)
    #         print (item.financialsTable)




    """ preset everything we need for running liquidity waterfall """
    lightstone_liquidity.set_cashflow_with_waterfall()

    """ build key components for liquidity  """
    lightstone_liquidity.analyze_liquidity()

    financials_df = lightstone_liquidity.get_financials()

    annual_cashflow_datarows, monthly_cashflow_datarows = lightstone_liquidity.output_liquidity_results()

    # financials_df.to_csv("lightstone_financials_df.csv")


    """ Step 4, calling reportwrite to write the designed reports """
    wb = 'myfirstkean3report.xlsx'
    filepath = 'myfirstkean3report.xlsx'
    data = {'Annual Summary':annual_cashflow_datarows, 'Monthly Summary': monthly_cashflow_datarows}
    formats = {}
    test_rw = ReportWriter(wb, data, formats)
    test_rw.write_data_to_workbook()
    test_rw.save(filepath)




if __name__ == '__main__':
    """ 20200527 test cases using vistra financials """

    portfolio = 'Vector'
    liquidity_scenario = 'LBO model test'
    liquidity_version = 'v1'

    vector_lbo = Liquidity(portfolio,liquidity_scenario,liquidity_version)

    """ preset everything we need for running liquidity waterfall """
    vector_lbo.set_cashflow_with_waterfall()

    """ build key component for lbo """
    vector_lbo.analyze_leverage_buyout()

    cashflow_df = vector_lbo.metadata['cashflow']
    # cashflow_df.to_csv("cashflow_df_1.csv")

    vector_lbo.solve_purchase_price_by_irr(0.2)

    cashflow_df = vector_lbo.metadata['cashflow']
    """ next step to use ReportWriter to write the formatted report """

    

    sys.exit()

    #
    # """ build key components for liquidity  """
    # lightstone_liquidity.analyze_liquidity()
    #
    # financials_df = lightstone_liquidity.get_financials()
    #
    # annual_cashflow_datarows, monthly_cashflow_datarows = lightstone_liquidity.output_liquidity_results()
    #
    # # financials_df.to_csv("lightstone_financials_df.csv")
    #
    #
    # """ Step 4, calling reportwrite to write the designed reports """
    # wb = 'myfirstkean3report.xlsx'
    # filepath = 'myfirstkean3report.xlsx'
    # data = {'Annual Summary':annual_cashflow_datarows, 'Monthly Summary': monthly_cashflow_datarows}
    # formats = {}
    # test_rw = ReportWriter(wb, data, formats)
    # test_rw.write_data_to_workbook()
    # test_rw.save(filepath)
    #
    #
    #
















# #
