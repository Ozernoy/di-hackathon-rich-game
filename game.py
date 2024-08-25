
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import psycopg2
import requests
from datetime import datetime, timedelta
from ui import UI
from db import DB
from settings import *
from visualization import animate_portfolio_values
import tkinter as tk
from tkinter import simpledialog, messagebox, Toplevel, Listbox, SINGLE, Button, Label

class  Player:
    def __init__(self, name, initial_budget=1000):
        self.name = name
        self.companies = []

        self.budget = initial_budget
        self.portfolio_history = None

    @property
    def company_ids(self):
        return [company.company_id for company in self.companies]
    
    def add_company(self, company):
        self.companies.append(company)
    

class Company:
    def __init__(self, company_id, name, symbol, description=None):
        self.company_id = company_id
        self.name = name
        self.symbol = symbol
        self.description = None

    def __str__(self) -> str:
        return f"{self.name} ({self.symbol})"

    def __repr__(self):
        return f"Company({self.name}, {self.symbol})"

class Game:
    value_column = 'adjusted_close'
    def __init__(self, start_date, end_date, n_comp_pp=5, db: DB=None):
        self.players = []
        self.n_comp_pp = n_comp_pp
        self.available_companies = None
        self.picked_companies = {}
        self.start_date = start_date
        self.end_date = end_date
        self.db = db

    def add_player(self, player):
        self.players.append(player)

    @property
    def players_num(self):
        return len(self.players)
    
    @property
    def company_ids(self):  
        return [company.company_id for company in self.picked_companies.values()]

    def get_random_companies(self, n):
        self.available_companies = {i: company for i, company in enumerate(self.db.get_random_companies(n))}

    def get_companies_stock_price(self):
        res = self.db.get_companies_stock_price(self.company_ids, self.start_date, self.end_date) 
        self.stock_price = pd.DataFrame(res, columns=res[0]._fields).drop_duplicates(["company_id", "month", "year"])
        self.stock_price.rename(columns={self.value_column: 'price'}, inplace=True)
        self.stock_price.price = self.stock_price.price.astype(float)

    def calculate_player_shares(self, player):
        shares = {}
        for company_id in player.company_ids:
            initial_stock_price = player.portfolio_history[(
                (player.portfolio_history["company_id"] == company_id)
                & (player.portfolio_history["month"] == self.start_date.month)
                & (player.portfolio_history["year"] == self.start_date.year)
            )]
            invesment = player.budget / len(player.company_ids)
            shares[company_id] = invesment / initial_stock_price["price"].values[0]
        return shares

    def calculate_player_portfolio(self, player):
        player.portfolio_history = self.stock_price[self.stock_price.company_id.isin(player.company_ids)]
        player.shares = self.calculate_player_shares(player)
        player.portfolio_history["shares"] = player.portfolio_history.company_id.map(player.shares)
        player.portfolio_history["value"] = player.portfolio_history.price * player.portfolio_history.shares

    def allocate_shares(self):
        for player in self.players:
            self.calculate_player_portfolio(player)

    def calculate_winner(self):
        end_date_sum = lambda p: p.portfolio_history[(
            (p.portfolio_history.month == self.end_date.month) 
            & (p.portfolio_history.year == self.end_date.year)
        )]["value"].sum()

        return max(self.players, key=end_date_sum)

    def run_game(self):
        self.get_random_companies(self.n_comp_pp * self.players_num)
        # Company selection phase
        for _ in range(5):  # Each player selects 5 companies
            for player in self.players:
                while True:
                    UI.display_available_companies(self.available_companies)
                    company_index = UI.get_player_choice(player, self.available_companies)
                    if company_index not in self.available_companies:
                        print("Error! Enter again.")
                        continue
                    picked_company = self.available_companies.pop(company_index)
                    print(f"Selected: {picked_company.name} ({picked_company.symbol})")
                    self.picked_companies.update({company_index: picked_company})
                    player.add_company(picked_company)
                    break

        self.get_companies_stock_price()
        self.allocate_shares()
        # Simulation and visualization phase
        anim = animate_portfolio_values(self.players)
        plt.show()

        # Determine and display winner
        winner = self.calculate_winner()
        # self.visualizer.display_final_results(self.players)
        print(f"The winner is {winner.name}!")

    def run_test(self):
        self.get_random_companies(self.n_comp_pp * self.players_num)
        # Company selection phase
        i = 0
        for _ in range(5):  # Each player selects 5 companies
            for player in self.players:
                UI.display_available_companies(self.available_companies)
                company_index = i
                picked_company = self.available_companies.pop(company_index)
                self.picked_companies.update({company_index: picked_company})
                player.add_company(picked_company)
                i += 1

        self.get_companies_stock_price()
        self.allocate_shares()
        # Simulation and visualization phase
        anim = animate_portfolio_values(self.players)
        plt.show()

        # Determine and display winner
        winner = self.calculate_winner()
        # self.visualizer.display_final_results(self.players)
        print(f"The winner is {winner.name}!")


def main_test():

    # Initialize game parameters
    start_date = datetime(2014, 1, 1)
    end_date = datetime(2023, 1, 31)
    db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)
    db.init_connection(db.db_name)
    # Create game instance
    game = Game(start_date, end_date, db=db)

    # Add players
    # num_players = int(input("Enter the number of players: "))
    num_players = 2
    for i in range(num_players):
        # name = input(f"Enter name for Player {i+1}: ")
        name = f"Player {i+1}"
        game.add_player(Player(name))

    # Run the game
    game.run_test()

def main():

    # Initialize game parameters
    start_date = datetime(2013, 1, 1)
    end_date = datetime(2024, 1, 31)
    print(HOST, DB_NAME, PASSWORD, USERNAME, PORT)
    db = DB(HOST, DB_NAME, PASSWORD, USERNAME, PORT)
    db.init_connection(db.db_name)
    # Create game instance
    game = Game(start_date, end_date, db=db)

    # Add players
    num_players = int(input("Enter the number of players: "))
    for i in range(num_players):
        name = input(f"Enter name for Player {i+1}: ")
        game.add_player(Player(name))

    # Run the game
    game.run_game()

if __name__ == "__main__":
    main()
    # main_test()
