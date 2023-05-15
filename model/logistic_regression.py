# # #                   # # #
#                           #
# Logistic regression model #
#                           #
# # #                   # # #

# # #                                                         # # #
#                                                                 #
# This file is tagged: [WORK IN PROGESS], [PENDING DOCUMENTATION] #
#                                                                 #
# # #                                                         # # #

import sys
import os
sys.path.append(os.getcwd())

import const
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import util

SHOW_PLOTS = False

# # #

def logit(z):
  return 1 / (1 + np.exp(-z))

def gradient(X, y, w):
  return X.T @ (logit(X @ w) - y)

def negativeLL(X, y, w):
  return np.sum(((-y * np.log2(logit(X @ w) + 0.0001)) - ((1 - y) * np.log2(1 - logit(X @ w) + 0.0001))))

def trainLogistic(X, y):
  a = 0.000000005
  w = np.zeros((X.shape[1], 1))
  nlls = [negativeLL(X, y, w)]

  for _ in range(10000):
    w = w - (a * gradient(X, y, w))
    nlls.append(negativeLL(X, y, w))

  return w, nlls

def kFoldCrossValidation(X, y, k):
  fold_size = int(np.ceil(len(X) / k))

  acc = []
  for i in range(k):

    start = i * fold_size
    end = min((i + 1) * fold_size, len(X))

    trainX = np.concatenate([X[0:start], X[end:len(X)]])
    trainY = np.concatenate([y[0:start], y[end:len(y)]])

    testX = X[start:end]
    testY = y[start:end]

    w, nlls = trainLogistic(trainX, trainY)

    acc.append(np.mean((testX @ w > 0) == testY))

  acc_mean, acc_std = np.mean(acc), np.std(acc)

  return acc_mean, acc_std

def main():

  bias = 200 # Bias selected by cursory experimentation

  unit_to_decimal_lat_approx = util.returnUnitDistanceLat(const.grid_unit_size, const.earth_radius)
  unit_to_decimal_lng_approx = (util.returnUnitDistanceLng(const.grid_unit_size, const.earth_radius, const.oregon_northmost_lat) + util.returnUnitDistanceLng(1, const.earth_radius, const.oregon_southmost_lat)) / 2

  unit_grid_rows = int(np.ceil((const.oregon_northmost_lat - const.oregon_southmost_lat) / unit_to_decimal_lat_approx))
  unit_grid_columns = int(np.ceil((const.oregon_eastmost_lng - const.oregon_westmost_lng) / unit_to_decimal_lng_approx))

  data = pd.read_csv("./data/datasets/Oregon_Unit_Grid_Train__{}{}__{}x{}.csv".format(const.grid_unit_size, const.info_unit_distance, unit_grid_rows, unit_grid_columns))

  trainX = data.iloc[:, 13:-1].to_numpy()
  trainX = np.concatenate((np.full((trainX.shape[0], 1), bias), trainX), axis = 1)
  trainY = data.iloc[:, -1:].to_numpy()

  # # #

  w, nlls = trainLogistic(trainX, trainY)

  if SHOW_PLOTS:
    plt.figure(figsize = (16,9))
    plt.plot(range(len(nlls)), nlls)
    plt.xlabel("Epoch")
    plt.ylabel("Negative Log Likelihood")
    plt.show()

  predictions = (trainX @ w > 0)
  pred_correct = (predictions == trainY)

  pred_acc_mean, pred_acc_std = (np.mean(pred_correct) * 100), (np.std(pred_correct) * 100)

  print("[INFO] One-shot accuracy (bias = {}): {:.4}% ({:.4}%)".format(bias, pred_acc_mean, pred_acc_std))

  print()

  # # #

  for k in [2, 5, 10, 20]:
    acc, std = kFoldCrossValidation(trainX, trainY, k)
    print("[INFO] {}-Fold accuracy: {:.4}% ({:.4}%)".format(k, acc * 100, std * 100))

  return 0

if __name__ == "__main__":
  sys.exit(main())