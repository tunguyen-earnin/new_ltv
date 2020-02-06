import sys
import os
import pickle
import pandas as pd
import modeling.misc.data_preparation as dp
import numpy as np
import csv
from collections import defaultdict
import lightgbm


class Predictor():
    def __init__(self, feature, filepath='',
                 filename=''):

        if filename == 'old_logisticRegression.pkl':
            self.newmodel = 3
        elif filename == 'lightgbm.pkl':
            self.newmodel = 2
        else:
            self.newmodel = 1
        self.filehandler = open(filepath + filename, 'rb')
        self.__loadModel__()
        self.prepareData(feature)

    def __selectSegment__(self):

        # lightgbm
        if self.newmodel == 2:
            self.dummy_rules = self.dummy_rules_list[self.isFirstActivation]
            self.risky_group = self.risky_group_list[self.isFirstActivation]
            self.f_columns = self.f_columns_list[self.isFirstActivation]
        else:
            self.dummy_rules = self.dummy_rules_list[self.isFirstActivation]
            self.normalizer = self.normalizer_list[self.isFirstActivation]
            self.predictor = self.predictor_list[self.isFirstActivation]
            self.f_columns = self.f_columns_list[self.isFirstActivation]
            if self.newmodel == 3:
                self.risky_group = self.risky_group_list[self.isFirstActivation]

    def __loadModel__(self):

        # lightgbm
        if self.newmodel == 2:
            self.dummy_rules_list = pickle.load(self.filehandler)
            self.risky_group_list = pickle.load(self.filehandler)
            self.f_columns_list = pickle.load(self.filehandler)
            self.predictor = pickle.load(self.filehandler)
        else:
            self.dummy_rules_list = pickle.load(self.filehandler)
            self.normalizer_list = pickle.load(self.filehandler)
            self.predictor_list = pickle.load(self.filehandler)
            self.f_columns_list = pickle.load(self.filehandler)
            if self.newmodel == 3:
                self.risky_group_list = pickle.load(self.filehandler)


    def prepareData(self, features):
        # self.isFirstActivation = features['activation_firstRestore']
        self.features = features
        self.isFirstActivation = 0
        self.__selectSegment__()
        df = pd.DataFrame(features, index=[0])
        df.replace(['True', 'False'], [1, 0], inplace=True)
        cats = list(self.dummy_rules.keys())
        ##################
        df['derived_transactionAmountTrendDisToOne'] = \
            np.abs(df['bank_transactionAmountTrend'] - 1)
        df['derived_transactionCountTrendDisToOne'] = \
            np.abs(df['bank_transactionCountTrend'] - 1)
        if df['payroll_lastPayrollSetupBy'][0] == 'System':
            df['payroll_lastPayrollSetupBy_APS'] = 1
        else:
            df['payroll_lastPayrollSetupBy_APS'] = 0
        if self.newmodel == 2 or self.newmodel == 3:
            if df['bank_employer'][0] in self.risky_group:
                df['risky_group'] = 1
            else:
                df['risky_group'] = 0
        ###############

        df = dp.dummyCoding(df, cats, self.dummy_rules)
        #lightgbm
        if self.newmodel == 2:
            self.X_predict = df[self.f_columns]
            self.X_predict[self.X_predict.select_dtypes(['object']).columns] = self.X_predict.select_dtypes(['object']).apply(
                lambda x: x.astype('float'))
        else:
            self.X_predict = dp.dataNormalization_Predict(
                df[self.f_columns], self.normalizer)

    def getReasonCode(self):
        X_predict = self.X_predict
        predictor = self.predictor
        rc = X_predict.values*predictor.coef_
        rc = rc[0]

        f_columns = X_predict.columns.tolist()
        reasonCodes = [(x, y) for (y, x) in
                       sorted(zip(rc, f_columns), reverse=True)]
        reasonCode = ["%s:%.2f" % x for x in reasonCodes[:5]]
        return reasonCode

    def getReasonCodeForNegativeOne(self):
        X_predict = self.X_predict
        reasons = X_predict.isnull().any(axis=0)
        reasonCode = reasons[reasons==True].index.tolist()
        if len(reasonCode) >= 20:
            reasonCode = reasonCode[:20]
        return reasonCode

    def getScore(self):
        X_predict = self.X_predict
        #lightgbm
        if self.newmodel == 2:
            y_pred = self.predictor.predict_proba(X_predict, num_iteration=self.predictor.best_iteration_)[:, 1]
            r = y_pred[0]
        else:
            y_pred = self.predictor.predict_proba(X_predict)
#           y_pred = lr.predict_proba(X_predict)
            r = y_pred[0, -1]

        return r

    def getTipRate(self):
        return self.features['activation_tipRateExp']

    def getTotalAmount(self):
        return self.features['activation_aveAmtAct_Wk12']*min(12,int(self.features['activation_wkSinceFirstAct']))

    def getAvgPayroll(self):
        return self.features['bank_avgPayRollAmountWithinNweeksN_12']

    def getReasonCategory(self):
        filename = '/app/src/modeling/model/max_adjustment/var2reasonCategory.csv'

        real_cols = self.X_predict.columns.tolist()

        reasonMap = defaultdict(list)
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['reason_cat'] is not '':
                    for col in real_cols:
                        if col.startswith(row['feature_name']):
                            reasonMap[row['reason_cat']].append(col)

        reasonCat = dict()
        for cat in reasonMap:
            X_predict = self.X_predict.copy()
            cols = [x for x in X_predict.columns if x not in reasonMap[cat]]
            # cols = [x for x in predictor.X_predict.columns]
            X_predict[cols] = 0
            y_cat = self.predictor.predict_proba(X_predict)
            reasonCat[cat] = y_cat[0][-1]

        return reasonCat

    def __writeDB__(self, modelName, conn, featureTuples):
        # CreatedOn = featureTuples['predTime']
        # CreatedOn = CreatedOn[:10] + ' ' + CreatedOn[11:19]
        # CreatedOn = datetime.strptime(CreatedOn, "%Y-%m-%d %H:%M:%S")

        if os.getenv("PROD") is not None:
            if modelName == "activation_risk":
                sql = '''INSERT INTO DataScience.UserModelFeatures
                (ModelName, UserId, CreatedOn, FeatureName, FeatureValue, ActivationId)
                VALUES '''

                for x in featureTuples:
                    if x != 'uid' and x != 'predTime' and x != 'activation_activationid':
                        sql += '''('%s', '%s','%s','%s','%s', '%s'), ''' % (modelName,
                                                                            featureTuples['uid'],
                                                                            featureTuples['predTime'][:19],
                                                                            x, str(featureTuples[x]),
                                                                            str(featureTuples['activation_activationid']))
            else:
                sql = '''INSERT INTO DataScience.UserModelFeatures
                (ModelName, UserId, CreatedOn, FeatureName, FeatureValue)
                VALUES '''

                for x in featureTuples:
                    if x != 'uid' and x != 'predTime':
                        sql += '''('%s', '%s','%s','%s','%s'), ''' % (modelName,
                                                                      featureTuples['uid'],
                                                                      featureTuples['predTime'][:19],
                                                                      x, str(featureTuples[x]))

            cursor = conn.cursor()
            cursor.execute(sql.strip(', '))
            conn.commit()
