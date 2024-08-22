class GUI:

    def __init__(self) -> None:
        pass


    # The function to get the list with user names
    @staticmethod
    def ask_names():
        
        list_of_users = []
        n_users = int(input('Welcome users! How many of you wants to play the game? '))
        print('Please provide your names: ')
        for _ in range(n_users):
            list_of_users.append(input())

        return list_of_users
            
    # The function to get the list with users' companies
    @staticmethod
    def ask_companies(available_companies, list_of_users):
        
        list_of_companies = []

        for name in list_of_users:
            print(f"Here's the list of companies from which you can choose: {available_companies}")
            print(f"""Hello {name}! Provide the name of the company from the list """)
            while True:
                companie = input()
                if companie not in available_companies:
                    print("Please provide the name of the company from the list")
                else:
                    break
            list_of_companies.append(companie)
            available_companies.remove(companie)

        return list_of_companies
        
   

