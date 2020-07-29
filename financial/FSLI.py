
class FSLI:


    def __init__(self, name, date_start, date_end, amount=0, credit_sign=1, is_subtotal=False):
        self.name = name
        self.dateStart = date_start
        self.dateEnd = date_end
        self.amount = amount
        self.creditSign = credit_sign
        self.isSubtotal = is_subtotal



    def __str__(self):
        console_text = ''
        console_text += ("---------------------------")
        console_text += ("FSLI object:\n")
        console_text += ("Name:" + self.name + "\n")
        console_text += ("Date Start:" + str(self.dateStart) + "\n")
        console_text += ("Date End: " + str(self.dateEnd) + "\n")
        console_text += ("Amount: " + str(self.amount) + "\n")
        console_text += ("Credit Sign: " + str(self.creditSign) + "\n")
        console_text += ("Is Subtotal: " + str(self.isSubtotal) + "\n")
        return console_text


    def calc_subtotal(self, fslis):
        if self.isSubtotal:
            self.amount = sum([fsli_obj.amount * fsli_obj.creditSign for fsli_obj in fslis])
        else:
            print ("FSLI", self.name, " is not a subtotal FSLI.")
            return None


























# #
