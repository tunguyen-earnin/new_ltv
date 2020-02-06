import pandas as pd
import ah_config
import ah_db
import numpy as np
from modeling.misc.misc import getSQLdata, getESdata, getESdatawithAccountId
from modeling.misc.misc import getESbankbalance, getESdataForPayroll
from modeling.misc import misc
import re
import time
import traceback


class BankFeature(object):
    def __init__(self, UserId):
        self.UserId = UserId
        self.balance = feature_balance(UserId)
        self.bankconnection = feature_bankConnection(UserId)
        self.payroll = feature_payroll(UserId)
        self.restoreaccount = \
            feature_restoreAccountTransaction(UserId)

    def buildFeatures(self, time):
        features = dict()
        features.update(self.balance.assembleFeatures(time))
        features.update(self.bankconnection.assembleFeatures(time))
        features.update(self.payroll.assembleFeatures(time))
        features.update(self.restoreaccount.assembleFeatures(time))
        return features


class BankFeature_predict(object):
    def __init__(self, UserId):
        self.UserId = UserId
        self.balance = feature_balance_predict(UserId)
        self.bankconnection = feature_bankConnection(UserId)

    def __get_ESFeatures__(self):

        sql = '''SELECT * FROM DataScience.EsFeatures 
                  WHERE UserId =''' + str(self.UserId)

        start = time.time()
        with ah_db.open_db_connection('datascience') as conn:
            df = getSQLdata(sql, conn)

        timeelapsed = time.time() - start
        log = ah_config.getLogger('ah.feature_bank')
        if df.shape[0] == 0:
            log.debug('No transactions found. Time elapsed: %d sec for user %s', timeelapsed, self.UserId)
            return(dict(zip(df.columns.values, [np.nan]*len(df.columns.values))))
        else:
            log.debug('Time elapsed: %d sec for user %s', timeelapsed, self.UserId)
            return(dict(zip(df.columns.values, df.iloc[0])))

    def buildFeatures(self, time):
        features = dict()
        features.update(self.balance.assembleFeatures(time))
        features.update(self.bankconnection.assembleFeatures(time))
        features.update(self.__get_ESFeatures__())
        return features


def assembleFeatures(object, time):
    features = dict()
    for name in (dir(object)):
        if callable(getattr(object, name)) and \
           '__' not in name and \
           name != 'getDataFrame' and \
           name != 'assembleFeatures' and \
           name != 'get_transactions':
            featurename = 'bank_' + name
            if 'nnday' in featurename.lower() or \
               'nnweek' in featurename.lower():
                featurename += 'N_' + str(object.N)
            try:
                features[featurename] =\
                    getattr(object, name)(object.df.copy(), time)
            except (IndexError, AttributeError, TypeError):
                if pd.to_datetime(object.earlistDate) <= time:
                    features[featurename] = 0
                else:
                    features[featurename] = np.nan
    return features


# ----------------------------------------#
#            Balance part                 #
# ----------------------------------------#
class feature_balance(object):

    def __init__(self, UserId, N=14):
        self.features = dict()
        self.UserId = UserId
        self.N = N
        self.df = self.getDataFrame()
        try:
            self.earlistDate = self.__earlistDate__(self.df.copy())
        except:
            self.earlistDate = pd.to_datetime('2100-01-01')

    def getDataFrame(self):
        sql = '''
              SELECT UserId, uba.BfpProvidedBankAccountId
              FROM bankfeed.UserProvidedBankAccount AS uba
              JOIN bankfeed.UserProvidedBankConnection AS ubc
              ON uba.UserProvidedBankConnectionId = ubc.UserProvidedBankConnectionId
              WHERE UserId = {}
              AND uba.BfpProvidedBankAccountId IS NOT NULL
              '''.format(self.UserId)
        try:
            with ah_db.open_db_connection('bankConnection') as conn_bf:
                df_tmp1 = getSQLdata(sql, conn_bf)

            bfpProvidedBankAccountId = pd.to_numeric(df_tmp1['BfpProvidedBankAccountId']).unique().tolist()
            df_tmp2 = getESbankbalance(self.UserId, bfpProvidedBankAccountId, False)
            df_tmp = pd.merge(df_tmp2, df_tmp1, on='BfpProvidedBankAccountId', how='left')

        except:
            df_tmp = pd.DataFrame(columns=['UserId', 'BfpProvidedBankAccountId',
                                           'Balance', 'CreatedOn'])
        return df_tmp.sort_values(by="CreatedOn").reset_index(drop=True)

    def getCurrentBalance(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time)]
        return df_tmp[(df_tmp.CreatedOn == df_tmp.CreatedOn.max())].\
            Balance.values[0]

    def hadNegBalinNDays(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N)+'days')) &
                       (df_in['Balance'] < 0)]
        return int(df_tmp.shape[0] > 0)

    def highestBalinNDays(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N)+'days'))]
        return df_tmp['Balance'].max()

    def lowestBalinNDays(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N)+'days'))]
        return df_tmp['Balance'].min()

    def countNegBalinNDays(self, df_in, time):
        return df_in[(df_in['CreatedOn'] <= time) &
                     (df_in['CreatedOn'] >=
                      time - pd.to_timedelta(str(self.N)+'days')) &
                     (df_in['Balance'] < 0)].CreatedOn.\
            map(lambda x: x.date()).unique().shape[0]

    def __earlistDate__(self, df_in):
        df_tmp = df_in[(pd.notnull(df_in.CreatedOn))]
        return pd.to_datetime(df_tmp.CreatedOn.min())

    def earlistBalanceDate(self, df_in, time):
        return self.earlistDate

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features


class feature_balance_predict(feature_balance):

    def __init__(self, UserId, N=14):
        self.features = dict()
        self.UserId = UserId
        self.N = N
        start = time.time()
        self.df = self.getDataFrame()
        timeelapsed = time.time() - start
        log = ah_config.getLogger('ah.feature_bank')
        log.debug('Total time elapsed for feature balance predict: %d sec for user %s', timeelapsed, self.UserId)

        try:
            self.earlistDate = self.__earlistDate__(self.df.copy())
        except:
            self.earlistDate = pd.to_datetime('2100-01-01')

    def getDataFrame(self):
        sql = '''
        SELECT UserId, uba.BfpProvidedBankAccountId
        FROM bankfeed.UserProvidedBankAccount AS uba
        JOIN bankfeed.UserProvidedBankConnection AS ubc
        ON uba.UserProvidedBankConnectionId = ubc.UserProvidedBankConnectionId
        WHERE UserId = {}
        AND uba.BfpProvidedBankAccountId IS NOT NULL
        ORDER BY uba.CreatedOn DESC
        LIMIT 1'''.format(self.UserId)

        log = ah_config.getLogger('ah.feature_bank')

        try:
            with ah_db.open_db_connection('bankConnection') as conn_bf:
                df_tmp1 = getSQLdata(sql, conn_bf)

            bfpProvidedBankAccountId = int(df_tmp1['BfpProvidedBankAccountId'][0])
            df_tmp2 = getESbankbalance(self.UserId, bfpProvidedBankAccountId)
            df_tmp = pd.merge(df_tmp2, df_tmp1, on='BfpProvidedBankAccountId', how='left')

            if df_tmp.shape[0] == 0:
                log.debug('Bank Balance in feature balance predict is empty for user %s', self.UserId)
            else:
                log.debug('Bank Balance in feature balance predict is updated for user %s', self.UserId)
        except Exception as e:
            log.exception('UserId is {0}. There is an exception in Bank Balance Predict and the exception is {1}'.format(self.UserId, e))
            log.exception('UserId is {0}. The traceback is {1}'.format(self.UserId, traceback.format_exc()))
            df_tmp = pd.DataFrame(columns=['UserId', 'BfpProvidedBankAccountId',
                                      'Balance', 'CreatedOn'])
            # UserId, balance.BfpProvidedBankAccountId,
            # balance.Balance, balance.CreatedOn
        return df_tmp

# -----------End of Balance----------#


# ----------------------------------------#
#            BankConnection               #
# ----------------------------------------#
class feature_bankConnection(object):
    def __init__(self, UserId):
        self.features = dict()
        self.UserId = UserId
        self.sql = 0
        start = time.time()
        self.df = self.getDataFrame()
        timeelapsed = time.time() - start
        log = ah_config.getLogger('ah.feature_bank')
        log.debug('Time elapsed: %d sec for user %s', timeelapsed, self.UserId)

        self.__detectAccountType__()
        try:
            self.earlistDate = self.__earlistDate__(self.df.copy())
        except:
            self.earlistDate = pd.to_datetime('2100-01-01')

    def getDataFrame(self):
        sql = '''
            SELECT UserId, FinancialInstitutionId,
            p.UserProvidedBankConnectionId, a.BfpProvidedBankConnectionId,
            p.isActive, p.CreatedOn AS connectionCreatedOn,
            p.LastUpdatedOn AS connectionLastUpdatedOn,
            a.AccountNumber_H, a.AccountNickName,
            a.AccountTypeAtProvider, a.AccountDescriptionAtProvider,
            a.CreatedOn AS accountCreatedOn
            FROM bankfeed.UserProvidedBankConnection AS u
            JOIN bankfeed.BfpProvidedBankConnection AS p
            ON p.UserProvidedBankConnectionId = u.UserProvidedBankConnectionId
            JOIN bankfeed.BfpProvidedBankAccount AS a
            ON p.BfpProvidedBankConnectionId = a.BfpProvidedBankConnectionId
            WHERE UserId =''' + str(self.UserId)

        try:
            with ah_db.open_db_connection('bankConnection') as conn_bf:
                df_tmp = getSQLdata(sql, conn_bf)
        except:
            df_tmp = pd.DataFrame(columns=['UserId', 'FinancialInstitutionId',
                                           'UserProvidedBankConnectionId', 'BfpProvidedBankConnectionId',
                                           'isActive', 'connectionCreatedOn', 'connectionLastUpdatedOn',
                                           'AccountNumber_H', 'AccountNickName', 'AccountTypeAtProvider',
                                           'AccountDescriptionAtProvider', 'accountCreatedOn'])
        return df_tmp

    def __earlistDate__(self, df_in):
        df_tmp = df_in[(pd.notnull(df_in.connectionCreatedOn))]
        return pd.to_datetime(df_tmp.connectionCreatedOn.min())

    def earlistBankConnectionDate(self, df_in, time):
        return self.earlistDate

    def getInstitutionId(self, df_in, time):
        df_tmp = df_in[(df_in['connectionCreatedOn'] <= time)]
        return df_tmp[df_tmp['connectionCreatedOn'] ==
                      df_tmp['connectionCreatedOn'].max()].\
            FinancialInstitutionId.values[0]

    def countOfAccounts(self, df_in, time):
        df_tmp = df_in[(df_in['connectionCreatedOn'] <= time)]
        bankConnectionId = df_tmp[(df_tmp['connectionCreatedOn'] ==
                                   df_tmp['connectionCreatedOn'].max())].\
            BfpProvidedBankConnectionId.iloc[0]
        self.bank = bankConnectionId
        df_tmp = df_tmp[(df_tmp['BfpProvidedBankConnectionId'] ==
                         bankConnectionId)]
        return df_tmp.AccountNumber_H.unique().shape[0]

    def countConnectionChange(self, df_in, time):
        df_tmp = df_in[(df_in['connectionCreatedOn'] <= time)]
        return df_tmp['UserProvidedBankConnectionId'].unique().shape[0] - 1

    def __detectAccountType__(self):
        self.df['detectedAccountType'] = \
            self.df[['AccountNickName', 'AccountTypeAtProvider',
                     'AccountDescriptionAtProvider']].fillna('unknown').\
            sum(axis=1).map(misc.detectAccountKeyWord)

    def hasCreditCard(self, df_in, time):
        df_tmp = df_in[(df_in['connectionCreatedOn'] <= time)]
        return int('credit' in df_tmp.detectedAccountType.values)

    def hasLoan(self, df_in, time):
        df_tmp = df_in[(df_in['connectionCreatedOn'] <= time)]
        return int('loan' in df_tmp.detectedAccountType.values)

    def weeksSinceLastConnectionChange(self, df_in, time):
        df_tmp = df_in[(df_in['connectionCreatedOn'] <= time)]
        return (time - df_tmp['connectionCreatedOn'].max()).days/7

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features
# -----------End of bankConnection----------#


# ----------------------------------------#
#            Pay Roll                     #
# ----------------------------------------#
class feature_payroll(object):
    def __init__(self, UserId, N=12):
        self.features = dict()
        self.UserId = UserId
        self.N = N
        self.df = self.getDataFrame()
        try:
            self.earlistDate = self.__earlistDate__(self.df.copy())
        except:
            self.earlistDate = pd.to_datetime('2100-01-01')

    def getDataFrame(self):
        sql = '''
            SELECT UserId, PayDate, EvaluationDate,
            (CASE WHEN TransactionId IS NOT NULL THEN TransactionId ELSE -1 END) AS TransactionId,
            ProviderTransactionId
            FROM payroll.PayrollActiveStatusTracking AS pr
            WHERE (ProviderTransactionId IS NOT NULL OR TransactionId IS NOT NULL)
            AND UserId = {}
            ORDER BY EvaluationDate ASC'''.format(self.UserId)

        try:
            with ah_db.open_db_connection('payroll') as conn_pr:
                df_tmp = getSQLdata(sql, conn_pr)

            df_trsct = getESdataForPayroll(self.UserId, predict=False)

            df_tmp['TransactionId'] = df_tmp['TransactionId'].astype('str')

            df_tmp1 = pd.merge(df_tmp, df_trsct, on='ProviderTransactionId', how='inner')

            df_tmp1 = df_tmp1[['UserId', 'PayDate', 'EvaluationDate',
                               'CreatedOn', 'Amount', 'Description']]

            df_tmp2 = pd.merge(df_tmp, df_trsct, left_on='TransactionId',
                               right_on='id', how='inner')
            df_tmp2 = df_tmp2[['UserId', 'PayDate', 'EvaluationDate',
                               'CreatedOn', 'Amount', 'Description']]

            df_tmp = pd.concat([df_tmp1, df_tmp2])
            df_tmp = df_tmp.drop_duplicates(['PayDate', 'Amount', 'Description'])
            return df_tmp
        except:
            return pd.DataFrame(columns=['UserId', 'PayDate', 'EvaluationDate',
                                         'CreatedOn', 'Amount', 'Description'])

    def paidEarlierRatioWithinNWeeks(self, df_in, time, N=12):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]

        if df_tmp.shape[0] == 0:
            return 0

        earlyCount = df_tmp[df_tmp['CreatedOn'] <
                            df_tmp['PayDate'].
                            map(lambda x: pd.to_datetime(x))].shape[0]
        totalCount = df_tmp.shape[0]
        return earlyCount/max(0.5, totalCount)

    def paidEarlierCountWithinNWeeks(self, df_in, time, N=12):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        if df_tmp.shape[0] == 0:
            return 0

        earlyCount = df_tmp[(df_tmp['CreatedOn'] <
                            df_tmp['PayDate'].
                             map(lambda x: pd.to_datetime(x)))].shape[0]
        return earlyCount

    def payRollCountwithinNWeeks(self, df_in, time, N=12):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        totalCount = df_tmp.shape[0]
        return totalCount

    def mostRecentPayRollAmountinNWeeks(self, df_in, time, N=12):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        if df_tmp.shape[0] == 0:
            return 0
        else:
            return df_tmp[(df_tmp['CreatedOn'] == df_tmp['CreatedOn']
                           .max())].Amount.values[0]

    def mostRecentPayRollIsPartialinNWeeks(self, df_in, time, N=12):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        if df_tmp.shape[0] == 0:
            return 0
        else:
            income = df_tmp[(df_tmp['CreatedOn'] == df_tmp['CreatedOn']
                             .max())].Amount.values[0]
            if income % 5 == 0 or (income % 1 == 0 and income < 400):
                return 1
            else:
                return 0

    def mostRecentPayRollAmountToPreviousAVGinNWeeks(self, df_in, time, N=12):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        if df_tmp.shape[0] == 0:
            return 0
        else:
            recentpay = df_tmp[(df_tmp['CreatedOn'] == df_tmp['CreatedOn']
                                .max())].Amount.values[0]
            avgpay = df_tmp[(df_tmp['CreatedOn'] <= df_tmp['CreatedOn']
                             .max())].Amount.mean()
            return recentpay/avgpay

    def avgPayRollAmountWithinNweeks(self, df_in, time, N=12):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        if df_tmp.shape[0] == 0:
            return 0
        else:
            return df_tmp.Amount.mean()

    def stdPayRollAmountWithinNweeks(self, df_in, time, N=12):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        result = df_tmp.Amount.std()
        return [result, 0][np.isnan(result)]

    def payRollHistGreaterThanNweeks(self, df_in, time, N=12):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (pd.notnull(df_in.CreatedOn))]
        if df_tmp.shape[0] == 0:
            return 0
        else:
            return int(time - df_tmp['CreatedOn'].min() >=\
                pd.to_timedelta(str(N*7)+' days'))

    def avgPayRollCycle(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (pd.notnull(df_in.CreatedOn))]
        return misc.computeAvgCycle(df_tmp['CreatedOn'])

    def stdPayRollCycle(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (pd.notnull(df_in.CreatedOn))]
        return misc.computeCycleStd(df_tmp['CreatedOn'])

    def __earlistDate__(self, df_in):
        df_tmp = df_in[(pd.notnull(df_in.CreatedOn))]
        return pd.to_datetime(df_tmp.CreatedOn.min())

    def earlistPayRollDate(self, df_in, time):
        return self.earlistDate

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features


# ----------------------------------------#
class feature_payroll_predict(feature_payroll):
    def __init__(self, UserId, N=12):
        self.features = dict()
        self.UserId = UserId
        self.N = N
        start = time.time()
        self.df = self.getDataFrame()
        self.duration = time.time() - start
        
        try:
            self.earlistDate = self.__earlistDate__(self.df.copy())
        except:
            self.earlistDate = pd.to_datetime('2100-01-01')

    def getDataFrame(self):
        sql = '''
            SELECT UserId, PayDate, EvaluationDate,
            TransactionId, ProviderTransactionId
            FROM payroll.PayrollActiveStatusTracking AS pr
            WHERE ProviderTransactionId IS NOT NULL
            AND UserId = {}
            ORDER BY EvaluationDate ASC'''.format(self.UserId)

        try:
            with ah_db.open_db_connection('payroll') as conn_pr:
                df_tmp = getSQLdata(sql, conn_pr)

            df_trsct = getESdataForPayroll(self.UserId, predict=True)
            df_tmp = pd.merge(df_tmp, df_trsct, left_on='ProviderTransactionId',
                              right_on='ProviderTransactionId', how='inner')
            df_tmp = df_tmp[['UserId', 'PayDate', 'EvaluationDate',
                             'CreatedOn', 'Amount', 'Description']]
            df_tmp = df_tmp.drop_duplicates(['PayDate', 'Amount', 'Description'])
            return df_tmp
        except:
            return pd.DataFrame(columns=['UserId', 'PayDate', 'EvaluationDate',
                                         'CreatedOn', 'Amount', 'Description'])

# -----------End of pay roll----------#


# ----------------------------------------#
#       restore account transaction       #
# ----------------------------------------#
class feature_restoreAccountTransaction(object):
    def __init__(self, UserId, N=2):
        self.features = dict()
        self.UserId = UserId
        self.N = N
        self.df = self.getDataFrame()
        try:
            self.earlistDate = self.__earlistDate__(self.df.copy())
        except:
            self.earlistDate = pd.to_datetime('2100-01-01')

    def get_transactions(self, bfp_account_ids):
        #   Get all posted transactions for user for a specific set of bank account ids
        df = getESdatawithAccountId(self.UserId, bfp_account_ids, False)
        return df


    def getDataFrame(self):
        #   Get distinct AccountNumber/RoutingNumber pairs
        sql = '''
                SELECT uba.BfpProvidedBankAccountId, uba.RoutingNumber, uba.AccountNumber_H
                FROM bankfeed.UserProvidedBankAccount AS uba
                JOIN bankfeed.UserProvidedBankConnection AS ubc
                ON ubc.UserProvidedBankConnectionId = uba.UserProvidedBankConnectionId
                WHERE BfpProvidedBankAccountId IS NOT NULL
                AND UserId = {}
                ORDER BY uba.CreatedOn DESC'''.format(self.UserId)

        try:
            with ah_db.open_db_connection('bankConnection') as conn_bf:
                df = getSQLdata(sql, conn_bf)

            df = df.drop_duplicates()

            df_out1 = self.get_transactions(df.BfpProvidedBankAccountId.unique().tolist())

            df_out = pd.merge(df_out1, df, left_on='bfpProvidedBankAccountId',
                              right_on='BfpProvidedBankAccountId', how='inner')
            df_out = pd.DataFrame(df_out)[['UserId', 'PostedDate', 'CreatedOn', 'Amount',
                                           'Description', 'AccountNumber_H', 'RoutingNumber', 'categories']]
            df_out = df_out.drop_duplicates(['PostedDate', 'Amount', 'Description']).reset_index(drop=True)
            #   Get transactions for user, for specific bank account
            return df_out
        except:
            return pd.DataFrame(columns=['UserId', 'PostedDate', 'CreatedOn', 'Amount', 'Description',
                                         'AccountNumber_H', 'RoutingNumber','categories'])

    # --------------Account tenure -----------#
    def restoreAccountTenureinWeek(self, df_in, time):
        df_in = df_in.copy()
        df_tmp = df_in[(pd.notnull(df_in.PostedDate)) &
                       (df_in.CreatedOn <= time)]
        ActNum = re.sub(r'\D','',str(df_tmp[(df_tmp.CreatedOn == df_tmp.CreatedOn.max())].iloc[-1]['AccountNumber_H']))
        RtNum = df_tmp[(df_tmp.CreatedOn == df_tmp.CreatedOn.max())].iloc[-1]['RoutingNumber']

        df_tmp = df_tmp[(df_tmp['AccountNumber_H'].map(lambda x: re.sub(r'\D', '', str(x)) == ActNum)) &
                        (df_tmp['RoutingNumber'] == RtNum)]

        earlistDate = pd.to_datetime(
            df_tmp.PostedDate.min())

        tenure = (time - earlistDate).days/7
        if tenure >= 0:
            return min(tenure, 25)
        else:
            return np.nan

    # --------------Absolute amount -----------#
    def avgTransactionCountWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        df_out = misc.dropAHTransactions(df_tmp)
        return df_out.shape[0]/N

    def avgTransactionAmountWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        df_out = misc.dropAHTransactions(df_tmp)
        return df_out.Amount.abs().sum()/N

    def avgAmountPerTransactionWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        df_out = misc.dropAHTransactions(df_tmp)
        return df_out.Amount.abs().mean()

    # --------------Balance amount -----------#
    def TransactionBalanceWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        df_out = misc.dropAHTransactions(df_tmp)
        return df_out.Amount.sum()

    # --------------spending amount (-) -----------#
    def avgSpendingCountWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in['Amount'] < 0)]
        df_out = misc.dropAHTransactions(df_tmp)
        return df_out.shape[0]/N

    def avgSpendingAmountWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in['Amount'] < 0)]
        df_out = misc.dropAHTransactions(df_tmp)
        return df_out.Amount.sum()/N

    def avgAmountPerSpendingWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in['Amount'] < 0)]
        df_out = misc.dropAHTransactions(df_tmp)
        if df_out.shape[0] == 0:
            return 0
        else:
            return df_out.Amount.mean()

    def spendingAmountStdWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in['Amount'] < 0)]
        df_out = misc.dropAHTransactions(df_tmp)
        result = df_out.Amount.std()
        return [result, 0][np.isnan(result)]

    # --------------earning amount (+) -----------#
    def avgEarningCountWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in['Amount'] > 0)]
        df_out = misc.dropAHTransactions(df_tmp)
        return df_out.shape[0]/N

    def avgEarningAmountWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in['Amount'] > 0)]
        df_out = misc.dropAHTransactions(df_tmp)
        return df_out.Amount.sum()/N

    def avgAmountPerEarningWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in['Amount'] > 0)]
        df_out = misc.dropAHTransactions(df_tmp)
        if df_out.shape[0] == 0:
            return 0
        else:
            return df_out.Amount.mean()

    def earningAmountStdWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in['Amount'] > 0)]
        df_out = misc.dropAHTransactions(df_tmp)
        result = df_out.Amount.std()
        return [result, 0][np.isnan(result)]

    # --------------Percentage -----------#
    def earningCountRatioWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        return self.avgEarningCountWithinNWeeks(df_in, time, N) / \
            max(self.avgTransactionCountWithinNWeeks(df_in, time, N), 0.5)

    def spendingCountRatioWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        return self.avgSpendingCountWithinNWeeks(df_in, time, N) / \
            max(self.avgTransactionCountWithinNWeeks(df_in, time, N), 0.5)

    def earningAmountRatioWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        return self.avgEarningAmountWithinNWeeks(df_in, time, N) / \
            max(self.avgTransactionAmountWithinNWeeks(df_in, time, N), 0.5)

    def spendingAmountRatioWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        return self.avgSpendingAmountWithinNWeeks(df_in, time, N) / \
            max(self.avgTransactionAmountWithinNWeeks(df_in, time, N), 0.5)

    def AHCountOverOtherCountinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        df_out = misc.extractAHTransactions(df_tmp)
        return df_out.shape[0] / N / \
            max(self.avgTransactionCountWithinNWeeks(df_in, time, N), 0.5)

    def AHAmountOverOtherAmountinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days'))]
        df_out = misc.extractAHTransactions(df_tmp)
        return df_out.Amount.abs().sum() / N / \
            max(self.avgTransactionAmountWithinNWeeks(df_in, time, N), 0.5)

    # --------------Trend -----------#
    def transactionAmountTrend(self, df_in, time):
        return self.avgTransactionAmountWithinNWeeks(df_in, time, 2) / \
            max(self.avgTransactionAmountWithinNWeeks(df_in, time, 4), 0.5)

    def transactionCountTrend(self, df_in, time):
        return self.avgTransactionCountWithinNWeeks(df_in, time, 2) / \
            max(self.avgTransactionCountWithinNWeeks(df_in, time, 4), 0.5)

    def abandoningAccount(self, df_in, time):
        return int(self.avgTransactionCountWithinNWeeks(df_in, time, 2) <
                   max(self.avgTransactionCountWithinNWeeks(df_in,
                                                            time, 8)/2, 0.5))

    # --------------Indicator-----------#
    def __earlistDate__(self, df_in):
        df_tmp = df_in[(pd.notnull(df_in.PostedDate))]
        return pd.to_datetime(df_tmp.PostedDate.min())

    def earlistRestoreAccountTransactionDate(self, df_in, time):
        return self.earlistDate

    def daySinceLastTransaction(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (pd.notnull(df_in.PostedDate))]
        return (time - pd.to_datetime(df_tmp.PostedDate.max())).days

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features

    # --------------competitor-----------#

    def __cntCompetitor__(self, desc):
        p = re.compile(
            r'.*(check\sn\sgo|check.*n.*go|'
            'lendup|'
            'moneylion|money\slion|'
            'dave.*inc|dave.*com|'
            'uber|raiser|'
            'cash\snet|cashnet|'
            'payactiv|'
            'clear.*banc|daily.*pay|'
            'check\scard.*ace|'
            'hundy|'
            'even\s.*|even|.*even\sresponsible|'
            'paydaylender'
            ').*')
        if p.match(desc.lower()):
            return p.findall(desc.lower())[0]
        else:
            return 0

    def __isCompetitor__(self, desc):
        p = re.compile(
            r'.*(check\sn\sgo|check.*n.*go|'
            'lendup|'
            'moneylion|money\slion|'
            'dave.*inc|dave.*com|'
            'uber|raiser|'
            'cash\snet|cashnet|'
            'payactiv|'
            'clear.*banc|daily.*pay|'
            'check\scard.*ace|'
            'hundy|'
            'even\s.*|even|.*even\sresponsible|'
            'paydaylender'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def __isDave__(self, desc):
        p = re.compile(
            r'.*(dave.*inc|dave.*com'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasDaveInTwoWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(2 * 7) + ' days')) & (df_in['Amount'] > 0) &
                       (df_in['Amount'] <= 250)]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isDave__))].shape[0] > 0)

    def cntActCompetitorInTwoWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(2 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isCompetitor__))]
        if df_out.shape[0] > 0:
            return df_out.shape[0]
        else:
            return 0

    def hasDaveInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0) &
                       (df_in['Amount'] <= 250)]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isDave__))].shape[0] > 0)

    def actCompetitorInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isCompetitor__))]
        if df_out.shape[0] > 0:
            return sum(df_out['Amount'])
        else:
            return 0

    def avgActCompetitorInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isCompetitor__))]
        if df_out.shape[0] > 0:
            return df_out['Amount'].mean()
        else:
            return 0

    def cntActCompetitorInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isCompetitor__))]
        if df_out.shape[0] > 0:
            return df_out.shape[0]
        else:
            return 0

    def lastDaySinceActCompetitorInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isCompetitor__))]
        if df_out.shape[0] > 0:
            return (time - df_out['PostedDate'].max()).days
        else:
            return 8 * 7

    def firstDaySinceActCompetitorInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isCompetitor__))]
        if df_out.shape[0] > 0:
            return (time - df_out['PostedDate'].min()).days
        else:
            return 8 * 7

    def intervalActCompetitorInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isCompetitor__))]
        if df_out.shape[0] > 0:
            df_out = df_out.sort_values(by='PostedDate')
            return df_out['PostedDate'].diff().mean().days
        else:
            return 0

    def avgActDaveInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isDave__))]
        if df_out.shape[0] > 0:
            return df_out['Amount'].mean()
        else:
            return 0

    def cntActDaveInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isDave__))]

        if df_out.shape[0] > 0:
            return df_out.shape[0]
        else:
            return 0

    def cntCompetitorInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] > 0)]
        competitor_list = list(df_tmp.Description.map(self.__cntCompetitor__))
        unique_competitor_set = set(competitor_list)

        if 0 in unique_competitor_set:
            if len(unique_competitor_set) == 1:
                return 0
            else:
                return len(unique_competitor_set) - 1
        else:
            return len(unique_competitor_set)


    # --------------categories-----------#

    def __isTravel__(self, desc):
        p = re.compile(
            r'.*(travel'
            ').*')
        if p.match(desc[0].lower()):
            return True
        else:
            return False

    def __isTransfer__(self, desc):
        p = re.compile(
            r'.*(transfer'
            ').*')
        if p.match(desc[0].lower()):
            return True
        else:
            return False

    def __isShops__(self, desc):
        p = re.compile(
            r'.*(shops'
            ').*')
        if p.match(desc[0].lower()):
            return True
        else:
            return False

    def __isRestaurant__(self, desc):
        p = re.compile(
            r'.*(food'
            ').*')
        if p.match(desc[0].lower()):
            return True
        else:
            return False

    def cntActTravelInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] < 0)]
        df_out = df_tmp[(df_tmp.categories.map(self.__isTravel__))]
        if df_out.shape[0] > 0:
            return df_out.shape[0]
        else:
            return 0

    def cntActTransferInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] < 0)]
        df_out = df_tmp[(df_tmp.categories.map(self.__isTransfer__))]
        if df_out.shape[0] > 0:
            return df_out.shape[0]
        else:
            return 0

    def intervalActTransferInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] < 0)]
        df_out = df_tmp[(df_tmp.categories.map(self.__isTransfer__))]
        if df_out.shape[0] > 0:
            df_out = df_out.sort_values(by='PostedDate')
            return df_out['PostedDate'].diff().mean().days
        else:
            return 0

    def cntActShopInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] < 0)]
        df_out = df_tmp[(df_tmp.categories.map(self.__isShops__))]
        if df_out.shape[0] > 0:
            return df_out.shape[0]
        else:
            return 0

    def intervalActShopInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] < 0)]
        df_out = df_tmp[(df_tmp.categories.map(self.__isShops__))]
        if df_out.shape[0] > 0:
            df_out = df_out.sort_values(by='PostedDate')
            return df_out['PostedDate'].diff().mean().days
        else:
            return 0

    def cntActRestaurantInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] < 0)]
        df_out = df_tmp[(df_tmp.categories.map(self.__isRestaurant__))]
        if df_out.shape[0] > 0:
            return df_out.shape[0]
        else:
            return 0

    def intervalActRestaurantInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8 * 7) + ' days')) & (df_in['Amount'] < 0)]
        df_out = df_tmp[(df_tmp.categories.map(self.__isRestaurant__))]
        if df_out.shape[0] > 0:
            df_out = df_out.sort_values(by='PostedDate')
            return df_out['PostedDate'].diff().mean().days
        else:
            return 0

    # ----------------------Tax Refund ---------------------------#

    def __isTaxRefund__(self, desc):

        p = re.compile(
            r'.*(irs|tax'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasTaxRefundInTwoWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(2 * 7) + ' days')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isTaxRefund__))]

        if df_out.shape[0] > 0:
            return 1
        else:
            return 0

    def hasTaxRefundInThisYear(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= pd.to_datetime(str(time.year) + '-01-01')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isTaxRefund__))]
        if df_out.shape[0] > 0:
            return 1
        else:
            return 0

    def ActTaxRefundInThisYear(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= pd.to_datetime(str(time.year) + '-01-01')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isTaxRefund__))]
        if df_out.shape[0] > 0:
            return df_out['Amount'].sum()
        else:
            return 0
    def hasTaxRefundInLastYear(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] < pd.to_datetime(str(time.year) + '-01-01')) &
                       (df_in['PostedDate'] >= pd.to_datetime(str(time.year-1) + '-01-01')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isTaxRefund__))]
        if df_out.shape[0] > 0:
            return 1
        else:
            return 0

    def ActTaxRefundInLastYear(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] < pd.to_datetime(str(time.year) + '-01-01')) &
                       (df_in['PostedDate'] >= pd.to_datetime(str(time.year-1) + '-01-01')) & (df_in['Amount'] > 0)]
        df_out = df_tmp[(df_tmp.Description.map(self.__isTaxRefund__))]
        if df_out.shape[0] > 0:
            return df_out['Amount'].sum()
        else:
            return 0



    # --------------Existence of specific transactions-----------#
    def hasSpendingGreaterOneThousandWithinNWeeks(self, df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in.Amount <= -1000)]
        return int(df_tmp.shape[0] > 0)

    def hasSpendingGreaterThreeHundredWithinNWeeks(self,
                                                   df_in, time, N=None):
        N = N or self.N
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(N*7)+' days')) &
                       (df_in.Amount <= -1000)]
        return int(df_tmp.shape[0] > 0)

    def hasPayDayLoaninEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isPayDayLoan__))].shape[0] > 0)

    def __isPayDayLoan__(self, desc):
        p = re.compile(
            r'.*(loan by phone|ace cash|cash central|lendup|'
            'firstpayloans|loan.*last|check n go|mobiloans|'
            'spot.*loan|mountain loan|payday.*loan|cass loan|'
            'greenline loans|green line lns|speedy cash|stone lake lending|'
            'plain.*green|max.*lend|advance america|clear Loan|'
            'green gate services|vbs maxlend|net pay advance|'
            'progressive leasing|progressive rent payments|'
            'green.*cashe|cashnetusa|'
            'money.*key|money.*tree|speedy cash|snap finance|garner.*loan|loan.*garner|'
            'stone.*lake.*len|vbs.*spotloan|'
            'houston.*finance|credit.*acce|cashnet|harvest.*moon|courtesy.*financ|'
            'opportunity.*fina|blueknight.*fina|usawebcash|power.*fin|'
            'mobiloans|dollar.*loan|green.*stream|5050pay|Sentral Financial|'
            'net.*credit|lend.*green|kings.*cash|solid.*oak.*fund|ocwen.*loan|'
            'rapid.*cash'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasTitleLoaninEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isTitleLoan__))].shape[0] > 0)

    def __isTitleLoan__(self, desc):
        p = re.compile(
            r'.*(title loan',
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasCashAdvanceinEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isCashAdvance__))].shape[0] > 0)

    def __isCashAdvance__(self, desc):
        p = re.compile(
            r'.*(cash.*advance',
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasClearBancDailyPayinTwoWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(2*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isClearBancDailyPay__))].shape[0] > 0)

    def __isClearBancDailyPay__(self, desc):
        p = re.compile(
            r'.*(clear.*banc|daily.*pay'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasAutoLoaninEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isAutoLoan__))].shape[0] > 0)

    def __isAutoLoan__(self, desc):
        p = re.compile(
            r'.*(auto.*loan|nmac.*loan|tmcc.*loan'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasUhaulinEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isUhaul__))].shape[0] > 0)

    def __isUhaul__(self, desc):
        p = re.compile(
            r'.*(u.*haul'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasRentersInsinEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isRenters__))].shape[0] > 0)

    def __isRenters__(self, desc):
        p = re.compile(
            r'.*(renter'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasCasinoinEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isCasino__))].shape[0] > 0)

    def __isCasino__(self, desc):
        p = re.compile(
            r'.*(casino'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasBadTransactioninEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isBad__))].shape[0] > 0)

    def __isBad__(self, desc):
        p = re.compile(
            r'.*(card trans|point of s|teller cas|preauthori|'
            'with cash|verifybank|maintenanc|atm surcha'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasFeeinEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isFee__))].shape[0] > 0)

    def __isFee__(self, desc):
        p = re.compile(
            r'.*(\s+fee'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasPhoneBillInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isPhoneBill__))].shape[0] > 0)

    def __isPhoneBill__(self, desc):
        p = re.compile(
            r'.*(\W+att|at&t|t.*mobile|verizon'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasRecurringPaymentInEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isRecurring__))].shape[0] > 0)

    def __isRecurring__(self, desc):
        p = re.compile(
            r'.*(recurr'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasOverDraftinEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isOverDraft__))].shape[0] > 0)

    def __isOverDraft__(self, desc):
        p = re.compile(
            r'.*(overdraft fee|overdraft item|od protection|'
            'negative balance fee|return.*item'
            'overdraft/return item|overdraft transfer|courtesy pay fee|'
            'overdraft charge|overdraft chrg'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False

    def hasR08InEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days')) &
                       (df_in['Amount'] > 0)]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isR08__))].shape[0] > 0)

    def __isR08__(self, desc):
        p = re.compile(
            r'(ach r.*t.*n|adjustment|returned stop pay item|'
            'reverse ach|reverse paid item|stop payme|'
            'paymentret|return.*ite|rev ach|dispute credit|stop pay|re.*v.*k'
            ').*')

        q = re.compile(r'(?!overdraft).*')
        r = re.compile(r'(?!activehour).*')
        if p.match(desc.lower()) and \
           q.match(desc.lower()) and \
           r.match(desc.lower()):
            return True
        else:
            return False

    def hasUnemploymentDepositinTwoMonth(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(60)+' days')) &
                       (df_in['Amount'] > 0)]
        return int(df_tmp[(df_tmp.Description.
                           map(self.__isUnemployment__))].shape[0] > 0)

    def __isUnemployment__(self, desc):
        p = re.compile(
            r'(uemploy|unemploy'
            ').*')

        if p.match(desc.lower()):
            return True
        else:
            return False

# -------------------- supplimentary ------------------------- #
    def countOfATMInsinEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        return df_tmp[(df_tmp.Description.
                       map(self.__isATM__))].shape[0]

    def amountOfATMInsinEightWeeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['PostedDate'] >= time -
                        pd.to_timedelta(str(8*7)+' days'))]
        if df_tmp[(df_tmp.Description.
                   map(self.__isATM__))].shape[0] == 0:
            return 0
        else:
            return df_tmp[(df_tmp.Description.
                           map(self.__isATM__))].Amount.sum()

    def __isATM__(self, desc):
        p = re.compile(
            r'.*(atm'
            ').*')
        if p.match(desc.lower()):
            return True
        else:
            return False


class feature_restoreAccountTransaction_predict(
        feature_restoreAccountTransaction):
    def __init__(self, UserId, N=2):
        self.features = dict()
        self.UserId = UserId
        self.N = N
        start = time.time()
        self.df = self.getDataFrame()
        self.duration = time.time() - start
        
        try:
            self.earlistDate = self.__earlistDate__(self.df.copy())
        except:
            self.earlistDate = pd.to_datetime('2100-01-01')

    def getDataFrame(self):
        sql = '''
            SELECT uba.BfpProvidedBankAccountId, ubc.isActive AS bank_active,
            uba.isActive AS account_active,
            uba.RoutingNumber, uba.AccountNumber_H
            FROM bankfeed.UserProvidedBankAccount AS uba
            JOIN bankfeed.UserProvidedBankConnection AS ubc
            ON ubc.UserProvidedBankConnectionId = uba.UserProvidedBankConnectionId
            WHERE ubc.isActive = true AND uba.isActive = true
            AND BfpProvidedBankAccountId IS NOT NULL
            AND UserId = {}
            ORDER BY uba.CreatedOn DESC'''.format(self.UserId)

        try:
            with ah_db.open_db_connection('bankConnection') as conn_bf:
                df_tmp = getSQLdata(sql, conn_bf)

            ActNum = int(re.sub(r'\D','',df_tmp.iloc[0]['AccountNumber_H']))
            RtNum = df_tmp.iloc[0]['RoutingNumber']

            df_tmp = df_tmp[(df_tmp['AccountNumber_H'].map(lambda x: int(re.sub(r'\D','',x)) == ActNum)) &
                            (df_tmp['RoutingNumber'] == RtNum)]

            df_tmp = df_tmp.drop_duplicates()

            df_trsct = getESdata(self.UserId, predict=True)

            df_tmp = pd.merge(df_tmp, df_trsct, left_on='BfpProvidedBankAccountId',
                              right_on='bfpProvidedBankAccountId', how='inner')

            df_tmp = df_tmp[['UserId', 'PostedDate',
                             'CreatedOn', 'Amount', 'Description','categories']]
            df_tmp = df_tmp.drop_duplicates(['PostedDate',
                                                 'Amount', 'Description'])
            return df_tmp

        except:
            return pd.DataFrame(columns=['UserId', 'PostedDate',
                                         'CreatedOn', 'Amount', 'Description','categories'])

    # def __earlistDate__(self, UserId, conn):
    #     # sql = '''
    #     # SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
    #     # SELECT
    #     # ba.UserId, uas.EarlestTransactionDate AS EarliestDate, ba.CreatedOn
    #     # FROM
    #     # (SELECT TOP 1
    #     # ubc.UserId, uba.BfpProvidedBankAccountId,
    #     # uba.createdOn
    #     # FROM
    #     # BankFeed.UserProvidedBankConnection AS ubc
    #     # JOIN
    #     # BankFeed.UserProvidedBankAccount AS uba
    #     # ON
    #     # ubc.UserProvidedBankConnectionId = uba.UserProvidedBankConnectionId
    #     # WHERE uba.IsActive = 'True'
    #     # AND uba.BfpProvidedBankAccountId IS NOT NULL
    #     # AND UserId = ''' + str(UserId) + '''
    #     # ORDER BY createdOn DESC) AS ba
    #     # JOIN
    #     # DataScience.UserAccountSummary AS uas
    #     # ON
    #     # uas.BfpProvidedBankAccountId = ba.BfpProvidedBankAccountId
    #     # '''

    #     # df_tmp = getSQLdata(sql, conn)
    #     # if df_tmp.shape[0] > 0:
    #     #     return pd.to_datetime(df_tmp.EarliestDate[0].date())
    #     # else:
    #     sql = '''
    #         SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
    #         SELECT
    #         ba.UserId, t.BfpProvidedBankAccountId,
    #         min(t.PostedDate) AS EarliestDate, ba.CreatedOn
    #         FROM
    #         (SELECT TOP 1
    #         ubc.UserId, uba.BfpProvidedBankAccountId,
    #         uba.createdOn
    #         FROM
    #         BankFeed.UserProvidedBankConnection AS ubc
    #         JOIN
    #         BankFeed.UserProvidedBankAccount AS uba
    #         ON
    #         ubc.UserProvidedBankConnectionId = uba.UserProvidedBankConnectionId
    #         WHERE uba.IsActive = 'True'
    #         AND uba.BfpProvidedBankAccountId IS NOT NULL
    #         AND UserId = ''' + str(UserId) + '''
    #         ORDER BY createdOn DESC) AS ba
    #         JOIN
    #         BankFeed.BfpProvidedBankAccountTransaction_View AS t
    #         ON
    #         t.BfpProvidedBankAccountId = ba.BfpProvidedBankAccountId
    #         GROUP BY UserID, t.BfpProvidedBankAccountId, ba.CreatedOn
    #         '''

    #     df_tmp = getSQLdata(sql, conn)
    #     df_tmp = df_tmp[df_tmp['CreatedOn'] == df_tmp['CreatedOn'].max()]
    #     # sql = '''
    #     #     INSERT INTO DataScience.UserAccountSummary
    #     #     VALUES (%d,'%s')''' % (df_tmp['BfpProvidedBankAccountId'][0],
    #     #                            str(df_tmp['EarliestDate'][0]))

    #     #             cursor = conn.cursor()
    #     #             cursor.execute(sql)
    #     # #            getSQLdata(sql, conn)
    #     #             conn.commit()
    #     return pd.to_datetime(df_tmp.EarliestDate[0].date())

    def restoreAccountTenureinWeek(self, df_in, time):
        tenure = (time - self.earlistDate).days/7
        if tenure >= 0:
            return min(tenure, 25)
        else:
            return np.nan

    # ------------------End of restore account transactions-------------- #


# ---------------------------Unit Test ----------------------------#
if __name__ == "__main__":
    from modeling.model.predictor import Predictor
    from modeling.model.risk_model.max_adjustment_model.MaxAdjustmentModel import MaxAdjustmentModel
    import pymssql, os, copy
    # from line_profiler import LineProfiler
    ah_config.initialize()

    userid = 1311868
    date = '2019-05-10'

    # bank_feature = feature_balance_predict(conn = conn, UserId = userid)

    # df = bank_feature.getDataFrame(UserId= userid, conn = conn)
    date = pd.to_datetime(date)
    start = time.time()
    # bf = BankFeature_predict(conn, userid)
    bf = BankFeature(userid)
    ft = bf.buildFeatures(date)
    print(userid)
    print(ft)

    end = time.time()

    eclipsed_time = end - start
    print("eclipsed time:", eclipsed_time)

    # restoreaccount = feature_restoreAccountTransaction(userid)
