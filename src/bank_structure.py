import json
import os

bankColumnStructure={}

def load_column_structure():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "bankColumnStructure.json")
    with open(file_path,"r") as file:
        global bankColumnStructure
        bankColumnStructure=json.load(file)

def get_bank_columns(bank_name):
    bank_name_upper=bank_name.upper()
    if not bankColumnStructure:
        load_column_structure()
    if bank_name_upper in bankColumnStructure:
        return bankColumnStructure[bank_name_upper]["header"]
    else:
        return None