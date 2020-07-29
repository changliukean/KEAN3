import pyexcelerate
import sys





class ReportWriter:


    def __init__(self,
                 workbook,
                 data,
                 formats):

        # the workbook that to be write on
        self.workbook = self.__initialize_workbook(workbook)

        # a dictionary of data matrices containing pure data to be written
        self.data = data

        # a dictionary of lists of formats to be applied to different worksheet
        self.formats = formats


    def __initialize_workbook(self, workbook):
        if isinstance(workbook, str):
            return pyexcelerate.Workbook(workbook)

        if isinstance(workbook, pyexcelerate.Workbook):
            return workbook


    def get_lower_right_based_on_data(self, data_rows, range_upper_left):
        max_row = len(data_rows)
        max_row = int(''.join([char for char in range_upper_left if char.isdigit()])) + max_row - 1

        max_column = len(data_rows[0])
        max_column = self.get_col2num(''.join([char for char in range_upper_left if not char.isdigit()])) + max_column - 1

        max_column_letter = ReportWriter.get_num2col(max_column)
        return max_column_letter + str(max_row)


    def create_worksheet(self, sheet_name):
        return self.workbook.new_sheet(sheet_name)


    def write_data_to_workbook(self):
        for key in self.data.keys():
            worksheet = self.workbook.new_sheet(key)
            data_rows = self.data[key]
            range_upper_left = 'A1'
            if key in self.formats:
                if 'RangeUpperLeft' in self.formats[key]:
                    range_upper_left = self.formats[key]['RangeUpperLeft']

            range_lower_right = self.get_lower_right_based_on_data(data_rows, range_upper_left)
            self.write_data_to_worksheet(worksheet, data_rows, range_upper_left, range_lower_right)


    def write_format_to_workbook(worksheet, format):
        pass


    def write_data_to_worksheet(self, worksheet, data_rows, range_upper_left, range_lower_right):
        print (range_upper_left)
        print (range_lower_right)
        worksheet.range(range_upper_left, range_lower_right).value = data_rows



    def save(self, filepath):
        self.workbook.save(filepath)


    @staticmethod
    def get_num2col(column_number):
        string = ""
        while column_number > 0:
            column_number, remainder = divmod(column_number - 1, 26)
            string = chr(65 + remainder) + string
        return string


    @staticmethod
    def get_col2num(column_letter):
        num = 0
        for c in column_letter:
            num = num * 26 + (ord(c.upper()) - ord('A')) + 1
        return num










# #
