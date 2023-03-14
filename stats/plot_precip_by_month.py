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

dfs_precip_counties = [pd.read_csv("./data/Precipitation_By_County/{}_Prec.csv".format(county), sep = ',', header = 0) for county in const.county_list]

plot_months_hist, ax_hist = plot.subplots(3, 4, figsize = (25, 25))
plot_months_boxplot, ax_boxplot = plot.subplots(3, 4, figsize = (25, 25))

j = k = 0
for i in range(1, 13):
  df_precip_month = np.matrix([df.query("Month == {}".format(i))["Precipitation"].to_numpy() for df in dfs_precip_counties])
  df_precip_month = np.ndarray.flatten(df_precip_month).tolist()[0]

  sns.histplot(ax = ax_hist[j][k], x = df_precip_month, color = "dodgerblue")
  ax_hist[j][k].set_title(const.month_list[i - 1] + " (n = {})".format(len(df_precip_month)))
  ax_hist[j][k].set_xlim(0, 20)
  ax_hist[j][k].set_ylim(0, 120)

  sns.boxplot(ax = ax_boxplot[j][k], x = df_precip_month, color = "dodgerblue")
  ax_boxplot[j][k].set_title(const.month_list[i - 1] + " (n = {})".format(len(df_precip_month)))

  k = i % 4
  j += 1 if k == 0 else 0

plot_months_hist.suptitle("Monthly Precipitation (inches) — Fixed Scales")
plot_months_hist.show()

plot_months_boxplot.suptitle("Monthly Precipitation (inches) — Dynamic Scales")
plot_months_boxplot.show()

print()