import openpyxl as opx
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font



class Format:

    def __init__(self,
                 start_row,
                 start_column,
                 end_row,
                 end_column,
                 font=Font(name='Calibri',size=11,bold=False,italic=False,vertAlign=None,underline='none',strike=False,color='FF000000'),
                 fill=PatternFill(fill_type=None,start_color='FFFFFFFF',end_color='FF000000'),
                 border=Border(left=Side(border_style=None,color='FF000000'),right=Side(border_style=None,color='FF000000'),
                               top=Side(border_style=None,color='FF000000'),bottom=Side(border_style=None,color='FF000000')),
                 alignment=Alignment(horizontal='general',vertical='bottom',text_rotation=0,wrap_text=False,shrink_to_fit=False,indent=0),
                 number_format='General'):

        self.startRow = start_row
        self.startColumn = start_column
        self.endRow = end_row
        self.endColumn = end_column
        self.font = font,
        self.fill = fill,
        self.border = border,
        self.alignment = alignment,
        self.number_format = number_format



    def pack_format(self):
        pass


    def unpack_format(input_obj):
        pass














class ReportWriter:
    def __init__(self, output_filepath, data_matrix=[], formats=[]):
        self.DataMatrix = data_matrix
        self.Formats = formats
        self.outputFilepath = output_filepath



    def write(self):
        pass























# #
