# # #                   # # #
#                           #
# Logistic regression model #
#                           #
# # #                   # # #

# # #                                # # #
#                                        #
# This file is tagged: [WORK IN PROGESS] #
#                                        #
# # #                                # # #

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
  # Logistic/Sigmoid function
  return 1 / (1 + np.exp(-z))

def gradient(X, y, w):
  # Gradient of the negative log likelihood with respect to parameters
  return X.T @ (logit(X @ w) - y)

def negativeLL(X, y, w):
  # Negative Log Likelihood for logistic regression
  return np.sum(((-y * np.log2(logit(X @ w) + 0.0001)) - ((1 - y) * np.log2(1 - logit(X @ w) + 0.0001))))

def trainLogistic(X, y):
  a = 0.000000005 # Learning rate
  w = np.zeros((X.shape[1], 1)) # Initialize weights to 0
  nlls = [negativeLL(X, y, w)]

  # Optimize for 10,000 iterations
  for _ in range(10000):
    w = w - (a * gradient(X, y, w)) # Learn a fraction of the gradient
    nlls.append(negativeLL(X, y, w))

  return w, nlls

def kFoldCrossValidation(X, y, k):
  fold_size = int(np.ceil(len(X) / k))

  # Permuting the order of the dataset helps mitigate its non-uniformity across
  # folds and gives a more accurate idea of its actual performance
  idx = np.random.permutation(len(X))

  X = X[idx]
  y = y[idx]

  acc_mean = []
  acc_std = []
  for i in range(k):

    start = i * fold_size
    end = min((i + 1) * fold_size, len(X))

    trainX = np.concatenate([X[0:start], X[end:len(X)]])
    trainY = np.concatenate([y[0:start], y[end:len(y)]])

    testX = X[start:end]
    testY = y[start:end]

    w, nlls = trainLogistic(trainX, trainY)

    predictions = (testX @ w > 0)
    pred_correct = (predictions == testY)

    acc_mean.append(np.mean(pred_correct))
    acc_std.append(np.std(pred_correct))

  acc_mean, acc_std, acc_std_mean, acc_std_std = np.mean(acc_mean), np.std(acc_mean), np.mean(acc_std), np.std(acc_std)

  return acc_mean, acc_std, acc_std_mean, acc_std_std

def main():

  bias = 200 # Bias selected by cursory experimentation

  unit_to_decimal_lat_approx = util.returnUnitDistanceLat(const.grid_unit_size, const.earth_radius)
  unit_to_decimal_lng_approx = (util.returnUnitDistanceLng(const.grid_unit_size, const.earth_radius, const.oregon_northmost_lat) + util.returnUnitDistanceLng(1, const.earth_radius, const.oregon_southmost_lat)) / 2

  unit_grid_rows = int(np.ceil((const.oregon_northmost_lat - const.oregon_southmost_lat) / unit_to_decimal_lat_approx))
  unit_grid_columns = int(np.ceil((const.oregon_eastmost_lng - const.oregon_westmost_lng) / unit_to_decimal_lng_approx))

  data = pd.read_csv("./data/datasets/Oregon_Unit_Grid_Train__{}{}__{}x{}.csv".format(const.grid_unit_size, const.info_unit_distance, unit_grid_rows, unit_grid_columns), sep = ',', header = 0)

  # Train on "GridX, GridY, Month, Precip, Temp Mean Max, Temp Mean, Temp Mean Min, Wind Speed Mean Max, Wind Gusts Mean Max, Evapotranspiration Mean"
  trainX = np.hstack((data.iloc[:, 0:2].to_numpy(), data.iloc[:, 8:9].to_numpy(), data.iloc[:, 13:-1].to_numpy()))
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

  print("[INFO] Self-reported accuracy (bias = {}): {:.4}% ({:.4}%)".format(bias, pred_acc_mean, pred_acc_std))

  print()

  # Confusion matrix

  pred_tp = sum((predictions & pred_correct))
  pred_tn = sum(((~predictions) & pred_correct))
  pred_fp = sum((predictions & (~pred_correct)))
  pred_fn = sum(((~predictions) & (~pred_correct)))

  # This looks awful, but it seems to print fine
  print("[INFO] Confusion matrix:")
  print("[INFO]   \t\t\t\t      | Predicted |")
  print("[INFO]   \t\t\t      [Positive]\t[Negative]")
  print("[INFO]   \t      [Positive]\t{}\t\t  {}".format(pred_tp, pred_fn))
  print("[INFO]   | Actual |")
  print("[INFO]   \t      [Negative]\t{}\t\t  {}".format(pred_fp, pred_tn))

  # # #

  for k in [2, 5, 10, 20]:
    acc_mean, acc_std, std_mean, std_std = kFoldCrossValidation(trainX, trainY, k)
    print("[INFO] {}-Fold accuracy: {:.4}% ({:.4}%) , {:.4}% ({:.4}%)".format(k, acc_mean * 100, acc_std * 100, std_mean * 100, std_std * 100))

  return 0

if __name__ == "__main__":
  sys.exit(main())