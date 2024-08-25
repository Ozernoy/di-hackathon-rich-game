import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pandas as pd
import numpy as np
from matplotlib.dates import YearLocator, MonthLocator, DateFormatter

def animate_portfolio_values(players):
    # Set up the figure and axis
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.set_facecolor('#f0f0f0')
    ax.set_facecolor('#f8f8f8')

    # Combine all players' portfolio histories
    all_data = pd.concat([player.portfolio_history for player in players])
    
    # Get unique dates and sort them
    dates = pd.to_datetime(all_data[['year', 'month']].assign(day=1))
    unique_dates = sorted(dates.unique())

    # Create a color map for players
    colors = plt.cm.tab10(np.linspace(0, 1, len(players)))

    lines = []
    annotations = []

    for player, color in zip(players, colors):
        line, = ax.plot([], [], lw=2, color=color, label=player.name)
        annotation = ax.annotate('', xy=(0,0), xytext=(5,5), textcoords="offset points",
                                 bbox=dict(boxstyle="round", fc="w", ec="0.5", alpha=0.8),
                                 arrowprops=dict(arrowstyle="->"))
        lines.append(line)
        annotations.append(annotation)

    # Set up the axes
    ax.set_xlim(unique_dates[0], unique_dates[-1])
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

    players_data = []
    max_value = 0
    for player in players:
        players_data.append(player.portfolio_history.groupby(['year', 'month']).sum(['value']).reset_index())
        max_value = max(max_value, players_data[-1]['value'].max())
    
    ax.set_ylim(0, max_value * 1.1)

    def init():
        for line in lines:
            line.set_data([], [])
        return lines

    def animate(frame):
        current_date = unique_dates[frame]
        for i, player, line, annotation, color in zip(range(len(players)), players, lines, annotations, colors):
            player_data = players_data[i]
            mask = (pd.to_datetime(player_data[['year', 'month']].assign(day=1)) <= current_date)
            dates = pd.to_datetime(player_data.loc[mask, ['year', 'month']].assign(day=1))
            values = player_data.loc[mask, 'value']
            
            line.set_data(dates, values)
            
            # Update annotation
            if len(values) > 0:
                annotation.xy = (dates.iloc[-1], values.iloc[-1])
                annotation.set_text(f"{player.name}: ${values.iloc[-1]:,.2f}")
                annotation.get_bbox_patch().set_facecolor(color)
                annotation.get_bbox_patch().set_alpha(0.8)
            else:
                annotation.set_text("")
        
        return lines + annotations

    anim = animation.FuncAnimation(fig, animate, init_func=init, frames=len(unique_dates),
                                   interval=100, blit=False, repeat=False)

    plt.tight_layout()
    return anim

# Example usage:
# anim = animate_portfolio_values(list_of_players)
# anim.save('portfolio_animation.gif', writer='pillow', fps=10)
# plt.show()