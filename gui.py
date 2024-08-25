import tkinter as tk
from tkinter import ttk, messagebox
from game import Game, Player
from datetime import datetime
from db import DB
from settings import HOST, DB_NAME, PASSWORD, USERNAME, PORT
from visualization import animate_portfolio_values
import matplotlib.pyplot as plt

class GameGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Stock Trading Game")
        self.root.geometry("600x400")
        
        self.players = []
        self.game = None

        # Create the main frame
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        # Add widgets
        self.create_widgets()

    def create_widgets(self):
        # Title Label
        self.title_label = ttk.Label(self.main_frame, text="Stock Trading Game", font=("Helvetica", 16))
        self.title_label.pack(pady=10)

        # Number of players
        self.num_players_label = ttk.Label(self.main_frame, text="Number of Players:")
        self.num_players_label.pack(pady=5)
        self.num_players_entry = ttk.Entry(self.main_frame)
        self.num_players_entry.pack(pady=5)

        # Start game button
        self.start_button = ttk.Button(self.main_frame, text="Start Game", command=self.start_game)
        self.start_button.pack(pady=10)

    def start_game(self):
        num_players = self.num_players_entry.get()
        if not num_players.isdigit() or int(num_players) <= 0:
            messagebox.showerror("Invalid Input", "Please enter a valid number of players.")
            return
        
        self.num_players = int(num_players)
        self.player_names = []
        self.setup_game()

    def setup_game(self):
        self.clear_frame()

        for i in range(self.num_players):
            label = ttk.Label(self.main_frame, text=f"Enter name for Player {i+1}:")
            label.pack(pady=5)
            entry = ttk.Entry(self.main_frame)
            entry.pack(pady=5)
            self.player_names.append(entry)

        self.confirm_button = ttk.Button(self.main_frame, text="Confirm", command=self.confirm_players)
        self.confirm_button.pack(pady=10)

    def confirm_players(self):
        self.players = [Player(entry.get()) for entry in self.player_names]
        
        # Initialize game parameters
        start_date = datetime(2014, 1, 1)
        end_date = datetime(2023, 1, 31)
        db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)
        db.init_connection(db.db_name)

        # Create game instance
        self.game = Game(start_date, end_date, db=db)

        for player in self.players:
            self.game.add_player(player)

        self.select_companies()

    def select_companies(self):
        self.clear_frame()

        # Display available companies
        self.company_label = ttk.Label(self.main_frame, text="Select Companies for each Player:")
        self.company_label.pack(pady=10)

        # Get the random companies from the database
        self.game.get_random_companies(self.game.n_comp_pp * self.game.players_num)

        self.current_player_index = 0
        self.current_company_index = 0

        self.ask_player_for_company()

    def ask_player_for_company(self):
        if self.current_company_index < self.game.n_comp_pp:
            if self.current_player_index < len(self.players):
                player = self.players[self.current_player_index]
                self.clear_frame()
                label = ttk.Label(self.main_frame, text=f"{player.name}, select a company:")
                label.pack(pady=10)

                # Create a Combobox for the player's company selection
                self.company_combobox = ttk.Combobox(self.main_frame)
                self.company_combobox['values'] = [f"{company.name} ({company.symbol})" for company in self.game.available_companies.values()]
                self.company_combobox.pack(pady=10)

                self.confirm_button = ttk.Button(self.main_frame, text="Confirm Selection", command=self.confirm_selection)
                self.confirm_button.pack(pady=10)
            else:
                self.current_player_index = 0
                self.current_company_index += 1
                self.ask_player_for_company()
        else:
            self.run_game()

    def confirm_selection(self):
        selected_company_name = self.company_combobox.get()
        selected_company = next((company for company in self.game.available_companies.values() if f"{company.name} ({company.symbol})" == selected_company_name), None)

        if selected_company:
            player = self.players[self.current_player_index]
            player.add_company(selected_company)

            # Add the selected company to the game's picked companies
            self.game.picked_companies[selected_company.company_id] = selected_company

            # Remove the selected company from the available companies
            del self.game.available_companies[next(key for key, value in self.game.available_companies.items() if value == selected_company)]

            self.current_player_index += 1
            print(f"Player {player.name} selected {selected_company.name} ({selected_company.symbol})")
            print(f"Picked companies: {self.game.picked_companies}")
            print(f"Remaining companies: {self.game.available_companies}")
            self.ask_player_for_company()
        else:
            messagebox.showerror("Invalid Selection", "Please select a valid company.")

    def run_game(self):
        self.clear_frame()
        self.game.get_companies_stock_price()
        self.game.allocate_shares()
        anim = animate_portfolio_values(self.players)
        plt.show()

        # Determine and display the winner
        winner = self.game.calculate_winner()
        print(f"The winner is {winner.name}!")

    def clear_frame(self):
        for widget in self.main_frame.winfo_children():
            widget.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    app = GameGUI(root)
    root.mainloop()
