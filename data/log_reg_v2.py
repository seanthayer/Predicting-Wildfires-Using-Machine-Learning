from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_curve, roc_curve, auc
from sklearn.metrics import classification_report
from matplotlib import pyplot
from matplotlib import pylab
import pandas as pd
import numpy as np

def log_reg_v2():
    df = pd.read_csv("training_v2.csv")
    df = df.drop(columns='Unnamed: 0', axis=1)
    df = df.drop(columns='County', axis=1)
    df = df.drop(columns='Date', axis=1)
    train_df = df.iloc[0:3400]
    test_df = df.iloc[3400:]
    
    X_train = train_df[['Temperature', 'Prev 1 Temp', 'Prev 2 Temp', 'Precipitation', 'Prev 1 Prec', 'Prev 2 Prec']]
    Y_train = train_df[['Is_Burned']]
    Y_train = Y_train.to_numpy()
    Y_train = Y_train.reshape(-1)
    X_test = test_df[['Temperature', 'Prev 1 Temp', 'Prev 2 Temp', 'Precipitation', 'Prev 1 Prec', 'Prev 2 Prec']]
    Y_test = test_df[['Is_Burned']]
    Y_test = Y_test.to_numpy()
    Y_test = Y_test.reshape(-1)

    lr = LogisticRegression()
    lr.fit(X_train, Y_train)
    Y_pred = lr.predict(X_test)
    p = 0
    for i in range(len(Y_test)):
        if Y_test[i] == Y_pred[i]:
            p += 1
    p /= len(Y_test)
    print(p)



log_reg_v2()
