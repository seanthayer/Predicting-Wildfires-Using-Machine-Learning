import sys
import os
import math
sys.path.append(os.getcwd())

import pandas as pd
import matplotlib.pyplot as plot
import seaborn as sns
import numpy as np
import const as const

# # #                             # # #
#                                     #
# This file is pending documentation! #
#                                     #
# # #                             # # #

df_wildfire = pd.read_csv("./data/Oregon_Fire_Record.csv", sep=',', header=0)

plot_month_hist = sns.histplot(x = df_wildfire["Month"], bins = 12, color = "dodgerblue")
plot_month_hist.set_title("Wildfire Occurrence by Month (n = {})".format(df_wildfire["Month"].count()))

plot_month_boxplot = sns.boxplot(x = df_wildfire["Month"], color = "dodgerblue")
plot_month_boxplot.set_title("Wildfire Occurrence by Month (n = {})".format(df_wildfire["Month"].count()))

print()