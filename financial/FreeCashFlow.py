from dateutil.relativedelta import relativedelta




from financial import FSLI


class FreeCashFlow(FSLI):


    def __init__(self, date_start, date_end, amount, time_zero, discount_rate=0, discounted_amount=0, discount_factor=0):
        FSLI.__init__(self, 'Free Cash Flow', date_start, date_end, amount, credit_sign=1, is_subtotal=True)
        self.discountRate = discount_rate
        self.timeZero = time_zero
        self.discountedAmount = discounted_amount
        self.discountFactor = discount_factor




    def calculate_discount_factor(self):
        difference_in_years = relativedelta(self.end_date, self.time_zero).years
        self.discountFactor = 1 / ((1 + self.discountRate) ** difference_in_years)
        return self.discountFactor



    def calculate_discounted_cashflow(self):
        difference_in_years = relativedelta(self.end_date, self.time_zero).years
        self.discountedAmount = self.amount * (1 / ((1 + self.discountRate) ** difference_in_years))
        return self.discountedAmount



    @staticmethod
    def calculate_wacc(equity_cost_of_capital, debt_cost_of_capital, equity_percentage):
        return equity_percentage * equity_cost_of_capital + (1 - equity_percentage) * debt_cost_of_capital











# #
