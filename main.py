from UI import GUI


available_companies = ['Apple', 'NVIDIA', 'AMD', 'OilCompany', 'WarCompany']


available_users = GUI.ask_names()

list_of_companies = GUI.ask_companies(available_companies, available_users)

