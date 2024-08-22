class GUI:

    def __init__(self, available_companies) -> None:
        self.available_companies = available_companies
        self.list_of_users = self.ask_names()

    # The function to get the list with user names
    @staticmethod
    def ask_names():
        list_of_users = []
        n_users = int(input('Welcome users! How many of you want to play the game? '))
        print('Please provide your names: ')
        for _ in range(n_users):
            list_of_users.append(input())

        return list_of_users

    # Function to display the company list
    def display_companies(self):
        print("Available companies:")
        for company in self.available_companies:
            print(f"- {company}")

    # Function to prompt user to choose companies
    def user_choose_companies(self, user):
        chosen_companies = []
        while True:
            self.display_companies()
            choices = input(f"{user}, enter the names of the companies you want to choose (comma separated), or 'done' to finish: ").strip()
            
            if choices.lower() == 'done':
                break

            selected_companies = [x.strip() for x in choices.split(',')]
            for company in selected_companies:
                if company in self.available_companies and company not in chosen_companies:
                    chosen_companies.append(company)
                else:
                    print(f"Invalid or duplicate choice: '{company}'. Please choose a valid company name.")
        
        return chosen_companies
    
    def combined_users_choices(self):
        user_choices = {}
        # Main loop to collect choices from each user
        for user in self.list_of_users:
            print(f"\n{user}'s turn to choose companies.")
            user_choices[user] = self.user_choose_companies(user)
        return user_choices

# Example usage:
available_companies = ["Apple", "Google", "Microsoft", "Amazon"]
gui = GUI(available_companies)
user_choices = gui.combined_users_choices()

print("\nUser choices:")
print(user_choices)
