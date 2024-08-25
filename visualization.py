import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pandas as pd
import numpy as np
from matplotlib.dates import YearLocator, MonthLocator, DateFormatter
import tkinter as tk
from tkinter import messagebox

def show_winner_stats(players):
    winner = max(players, key=lambda p: p.portfolio_history_sum.iloc[-1].value)
    root = tk.Tk()
    root.title("Game Results")
    root.withdraw()

    # Show the message box
    msg = "Game Results:\n"
    for i, player in enumerate(players):
        msg += f"{i+1}. {player.name}: {player.portfolio_history_sum.iloc[-1].value:.2f} \n"
    msg += f"\nThe winner is {winner.name} with a final portfolio value of {winner.portfolio_history_sum.iloc[-1].value:.2f}!"
    messagebox.showinfo("Game Results", msg)
    
    # Destroy the root window
    root.destroy()

def plot_portfolio_pie(players):
    for player in players:
        
        # Convert 'month' and 'year' into a datetime object
        player.portfolio_history['date'] = pd.to_datetime(player.portfolio_history[['year', 'month']].assign(day=1))
        
        # Get the latest date in the portfolio history
        latest_date = player.portfolio_history['date'].max()
        
        # Filter data for the latest date
        latest_portfolio = player.portfolio_history[player.portfolio_history['date'] == latest_date]
        
        # Group by company_name and sum the values
        value_by_company = latest_portfolio.groupby('name')['value'].sum()
        value_by_company = value_by_company.sort_values(by='value', ascending=False)
        
        # Plotting the pie chart
        plt.figure(figsize=(8, 6))
        plt.pie(value_by_company, labels=value_by_company.index, autopct='%1.1f%%', startangle=140)
        plt.title(f"{player.name}'s final portfolio distribution")
        plt.show()


def animate_portfolio_values(players, start_date, end_date):
    # Set up the figure and axis
    fig, ax = plt.subplots(figsize=(12, 6))

    unique_dates = pd.date_range(start_date, end_date, freq='ME')

    colors = plt.cm.tab10(np.linspace(0, 1, len(players)))

    lines = []
    annotations = []

    for player, color in zip(players, colors):
        line, = ax.plot([], [], lw=2, color=color, label=player.name)
        annotation = ax.annotate('', xy=(0,0), xytext=(5,5), textcoords="offset points",
                                 bbox=dict(boxstyle="round", fc="w", ec="0.5", alpha=0.8))
        lines.append(line)
        annotations.append(annotation)

    # Set up the axes
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Portfolio Value ($)', fontsize=12)
    ax.set_title('Portfolio Value Over Time', fontsize=14, fontweight='bold')
    
    # Set up date formatting for x-axis
    ax.xaxis.set_major_locator(YearLocator())
    ax.xaxis.set_minor_locator(MonthLocator())
    ax.xaxis.set_major_formatter(DateFormatter('%Y'))
    
    # Add grid
    ax.grid(True, linestyle='--', alpha=0.7)
    
    # Add legend
    ax.legend(loc='upper left', fontsize=10)


    def animate(frame):
        current_date = unique_dates[frame]
        min_value = 1e6
        max_value = 0
        offset_min = 5
        offset_max = 15
        for i, player, line, annotation, color in zip(range(len(players)), players, lines, annotations, colors):
            player_data = player.portfolio_history_sum
            mask = (pd.to_datetime(player_data[['year', 'month']].assign(day=1)) <= current_date)
            dates = pd.to_datetime(player_data.loc[mask, ['year', 'month']].assign(day=1))
            values = player_data.loc[mask, 'value']

            min_value = min(min_value, values.min())
            max_value = max(max_value, values.max())            

            line.set_data(dates, values)
            
            # Update annotation
            if len(values) > 0:
                annotation.xy = (dates.iloc[-1], values.iloc[-1])
                annotation.set_text(f"{player.name}: ${values.iloc[-1]:,.0f}")
                annotation.get_bbox_patch().set_facecolor(color)
                annotation.get_bbox_patch().set_alpha(0.8)
            else:
                annotation.set_text("")

        ax.set_ylim(min_value * 0.9, max_value * 1.1)
    
        offset = round((frame / len(unique_dates)) * (offset_max - offset_min) + offset_min)
        ax.set_xlim(unique_dates[0], current_date + pd.DateOffset(months=offset))
        
        # vertical dashed red line on end_date
        ax.axvline(x=end_date.replace(day=1), color='red', linestyle='--')

        # return lines + annotations
        # plt.plot()


    anim = animation.FuncAnimation(fig, animate, frames=len(unique_dates),
                                   interval=150, blit=False, repeat=False)
    

    plt.tight_layout()
    return anim

# Example usage:
# anim = animate_portfolio_values(list_of_players)
# anim.save('portfolio_animation.gif', writer='pillow', fps=10)
# plt.show()