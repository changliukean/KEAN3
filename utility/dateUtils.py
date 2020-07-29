from datetime import datetime, date, timedelta
from calendar import monthrange
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta


# store nerc holidays in kean3
# store dst dates in kean3






def get_one_month_ago(date_obj):
    current_month_begin = date(date_obj.year, date_obj.month, 1)
    previous_month_end = current_month_begin - timedelta(1)
    return previous_month_end




def get_month_list(start_month, end_month):

    start_month = date(start_month.year, start_month.month, 1)
    end_month = date(end_month.year, end_month.month, monthrange(end_month.year, end_month.month)[1])

    loop_month = start_month
    month_list = []

    while loop_month <= end_month:
        loop_month = date(loop_month.year, loop_month.month, monthrange(loop_month.year, loop_month.month)[1])
        month_list.append(loop_month)
        loop_month = loop_month + timedelta(days=1)

    return month_list



def get_one_month_later(date_obj):
    current_month_end = date(date_obj.year, date_obj.month, monthrange(date_obj.year, date_obj.month)[1])
    next_month_begin = current_month_end + timedelta(1)
    next_month_end = date(next_month_begin.year, next_month_begin.month, monthrange(next_month_begin.year, next_month_begin.month)[1])
    return next_month_end



def get_date_obj_from_str(date_str):
    return parse(date_str).date()


def get_months_shift_date(anchor_date, number_of_months):
    return anchor_date + relativedelta(months=number_of_months)



def get_cash_balance_begin_date(as_of_date):
    as_of_date = as_of_date + timedelta(1)
    return date(as_of_date.year, as_of_date.month, monthrange(as_of_date.year, as_of_date.month)[1])




def get_year_month_header(period_month):
    year_str = str(period_month.year)
    month_str = {1:'Jan',
                 2:'Feb',
                 3:'Mar',
                 4:'Apr',
                 5:'May',
                 6:'Jun',
                 7:'Jul',
                 8:'Aug',
                 9:'Sep',
                 10:'Oct',
                 11:'Nov',
                 12:'Dec'}[period_month.month]
    return year_str + "-" + month_str














# #
