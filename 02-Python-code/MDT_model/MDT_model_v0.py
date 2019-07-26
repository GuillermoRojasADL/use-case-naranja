import sys
import pandas as pd
import pickle
import os
import numpy as np

from sklearn.tree import export_graphviz

import scikitplot as skplt
import seaborn as sns

from IPython.display import Image
from sklearn.externals.six import StringIO
import pydot

from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix,classification_report,precision_score
pd.options.mode.chained_assignment = None  # Disable annoying pandas warning


if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")
###################
# transform text files to h5 files
INPUT_EXTENSION = '.pkl'
OUT_EXTENSION = 'h5'

INPUT_DATA_PATH = '/mnt/work/mdt/'
FILE_NAME = '20190725-final-master_table_churn'
OUTPUT_DATA_PATH = '/mnt/work/mdt/results/'
print('loading data from' + INPUT_DATA_PATH + FILE_NAME + INPUT_EXTENSION)

with open(INPUT_DATA_PATH + '20190725-final-master_table_churn.plk', 'rb') as input:
    dataset = pickle.load(file=input)

#First model implementation with sample
sample = dataset.sample(120000, random_state=777)
sample = sample.fillna(0)
sample.drop(columns=['DIAS_RAIS'], inplace=True)
#sample_num = sample.loc[:, sample.dtypes == np.float64  sample.dtypes == np.bool]

X = sample.drop('CHURN',axis = 1).loc[:, sample.dtypes == np.float64]
y = sample['CHURN']
X_trainset, X_testset, y_trainset, y_testset = train_test_split(X, y, test_size=0.30, random_state=777)

#FIT MODEL
churnTree = DecisionTreeClassifier(criterion="entropy", max_depth = 7 )
#churnTree
churnTree.fit(X_trainset,y_trainset)

# Model Evaluation
predChurn = churnTree.predict(X_testset)
predChurn_prob = churnTree.predict_proba(X_testset)
predChurn_prob.sum()


# Grafica cumulative gain
sns_plot=skplt.metrics.plot_cumulative_gain(y_testset, predChurn_prob)
fig=sns_plot.get_figure()
fig.savefig(OUTPUT_DATA_PATH + 'gain_curve.png')

# Generate tree graphic
features = list(X.columns)
# features
dot_data = '/mnt/work/mdt/results/tree_1.dot'
export_graphviz(churnTree, out_file=dot_data,feature_names=features,filled=True,rounded=True)

# presicion and recall
print(classification_report(y_true=y_testset,y_pred=predChurn))

#plot and save consusion matrix
sns_plot = sns.heatmap(confusion_matrix(y_testset,predChurn),cmap="viridis", lw = 2, annot=True, cbar=False)
fig=sns_plot.get_figure()
fig.savefig(OUTPUT_DATA_PATH + 'confusion_matrix_v2.png')






