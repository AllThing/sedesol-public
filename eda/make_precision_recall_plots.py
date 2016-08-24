import math
import numpy as np
import pandas as pd
from numpy import interp
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import roc_auc_score

def eval_enigh_income_precision_and_recall(y,yhat):
    '''
    param: y numpy.array of true responses
    param: yhat numpy.array of response predictions
    output: json object of threshholds list, along with 
     recall list and precision list for each threshhold
    '''
    ymin, ymax = int(yhat.min()), int(yhat.max())
    ks = np.linspace(ymin, ymax, 25).tolist()
    
    recall_at_k = []
    precision_at_k = []
    pct_below = []
    ylabels = y < 3.079181
    for k in ks:

        # for each income cutoff in ks, see how many people would 
        # actually be considered in poverty vs what we predicted
        yhatlabels = yhat < k
        number_scored = len(yhat) * 1.0
        cur_pct_below = len(yhat[yhat < k]) / number_scored
        pct_below.append(cur_pct_below)

        tp = 0.0     # truepostive count
        pp = 0.0     # predicted positive count
        ap = 0.0     # actual positive count

        for l in range(len(ylabels)):
            if ylabels[l] == True and yhatlabels[l] == True:
                tp += 1
            if ylabels[l] == True:
                ap += 1
            if yhatlabels[l] == True:
                pp += 1

        if ap == 0:
            recall = 0
        else:
            recall = tp / ap

        if pp == 0:
            precision = 0
        else:
            precision = tp / pp

        recall_at_k.append(recall)
        precision_at_k.append(precision)

    threshholds_scaled = np.array([np.interp(k, 
        [ymin, ymax], [0, 1]) for k in ks])

    yhat_bin = yhat < 3.079181                     #HARDCODE LOG POVERTY LINE!! 
    auc_score = roc_auc_score(ylabels, yhat_bin)
    
    return {"threshholds": threshholds_scaled.tolist(),
                       "recall": recall_at_k,
                       "precision": precision_at_k,
                       "percentage_of_population":pct_below,
                       "auc": auc_score}

# THIS IS WHERE THE ACTUAL GRAPHING FUNCTIONALITY BEGINS

# allmatrix = pd.read_csv("/Users/dolano/Downloads/eval_study_data.csv")
# 5 rows × 24 columns

# EACH ROW IS A DIFFERENT RUN OF LUIGI WITH COLUMNS 0.1, 0.2, .. 0.9  WHICH ARE DIFFERENT MODELS FITTED ON THE LUIGI RUN

# models = ["0.1", "0.2","0.3","0.4","0.5","0.6","0.7","0.8","0.9"]
y = allmatrix["spending"]
names = ["rf_reg-nestimators10", "tree_reg-criterionmse", "ridge-alpha0.01", 
    "rf_reg-nestimators4", "ridge-alpha0.0001", "ridge-alpha1e06", 
    "rf_reg-nestimators5", "ridge-alpha1e08", "ridge-alpha0.001", 
    "ridge-alpha1e05", "rf_reg-nestimators12"]


i = 0
for m in models:
    yhat = allmatrix[m]
    curpr = eval_enigh_income_precision_and_recall(y, yhat)   #returns dictionary of evaluated metrics, this is currently in run_models_funs

    pct_below = curpr["percentage_of_population"]
    precision_curve = curpr["precision"]
    recall_curve = curpr["recall"]
    auc = curpr["auc"]

    plt.clf()
    fig, ax1 = plt.subplots()
    ax1.plot(pct_below, precision_curve, 'b')
    ax1.set_xlabel('percent of population')

    ax1.set_ylabel('precision', color='b')
    ax2 = ax1.twinx()
    ax2.plot(pct_below, recall_curve, 'r')
    ax2.set_ylabel('recall', color='r')
    
    name = "%s, auc: %s" % (names[i],auc)
    plt.title(name)
    plt.show()
    i = i + 1
