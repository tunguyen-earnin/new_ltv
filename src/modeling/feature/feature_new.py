import ah_db
import pandas as pd
from modeling.misc.misc import getSQLdata
from modeling.misc import misc

class NewFeature(object):
    def __init__(self, UserId):
        self.auth = feature_authIssue(UserId)
        self.credit = feature_credit(UserId)
        self.note = feature_note(UserId)

    def buildFeatures(self, time):
        features = dict()
        features.update(self.auth.assembleFeatures(time))
        features.update(self.credit.assembleFeatures(time))
        features.update(self.note.assembleFeatures(time))
        return features

def assembleFeatures(object, time):
    features = dict()
    for name in (dir(object)):
        if callable(getattr(object, name)) and \
           '__' not in name and \
           name != 'getDataFrame' and \
           name != 'assembleFeatures':
            featurename = 'new_' + name
            if 'nnday' in featurename.lower() or \
               'nnweek' in featurename.lower():
                featurename += 'N_' + str(object.N)
            features[featurename] =\
                getattr(object, name)(object.df.copy(), time)
    return features


# ----------------------------------------#
#            authIssue part               #
# ----------------------------------------#
class feature_authIssue(object):
    def __init__(self, UserId, N=12):
        self.features = dict()
        self.UserId = UserId
        # self.conn_bf = conn_bf
        self.N = N
        self.df = self.getDataFrame()
        self.df['CreatedOn'] = pd.to_datetime(self.df['CreatedOn'])
        self.df['ResolvedOn'] = pd.to_datetime(self.df['ResolvedOn'])

    def getDataFrame(self):
        with ah_db.open_db_connection('bankConnection') as conn_bf:

            sql = '''
            SELECT UserBankId, AuthenticationIssueTypeId,
            AuthenticationIssueStatusId, CreatedOn, ResolvedOn
            FROM AuthenticationIssues
            WHERE UserId = {}
            ORDER BY CreatedOn ASC'''.format(self.UserId)

            df_out = getSQLdata(sql, conn_bf)
            if df_out.shape[0] == 0:
                df_out = pd.DataFrame(columns=['UserBankId',
                                               'AuthenticationIssueTypeId',
                                               'AuthenticationIssueStatusId',
                                               'CreatedOn', 'ResolvedOn'])
            return df_out

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features

    def countAuthIssuesInNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        return df_tmp.shape[0]

    def countUnresolvedAuthIssuesInNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days')) &
                       (df_in['AuthenticationIssueStatusId'] != 3)]
        return df_tmp.shape[0]

    def percentResolvedAuthIssues(self, df_in, time):
        return df_in[(df_in['CreatedOn'] <= time) &
                     (df_in['CreatedOn'] >=
                      time - pd.to_timedelta(str(self.N*7)+'days')) &
                     (df_in['AuthenticationIssueStatusId'] != 3)].shape[0] / \
                     (df_in[(df_in['CreatedOn'] <= time) &
                            (df_in['CreatedOn'] >=
                             time - pd.to_timedelta(str(self.N*7)+'days'))].
                      shape[0] + 0.5)

    def avgResolveLatencyInHours(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days')) &
                       (df_in['AuthenticationIssueStatusId'] == 3)]
        if df_tmp.shape[0] == 0:
            return 0
        else:
            return (df_tmp['ResolvedOn'] - df_tmp['CreatedOn']).\
                mean().total_seconds()/3600

    def avgAuthIssueFrequency(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        self.df_tmp = df_tmp
        return 1/misc.computeAvgCycle(df_tmp['CreatedOn'].
                                      map(lambda x: x.date()).
                                      drop_duplicates())


# ----------------------------------------#
#              credit part                #
# ----------------------------------------#
class feature_credit(object):
    def __init__(self, UserId, N=12):
        self.features = dict()
        self.UserId = UserId
        # self.conn_mm = conn_mm
        self.N = N
        self.df = self.getDataFrame()
        self.df['CreatedOn'] = pd.to_datetime(self.df['CreatedOn'])

    def getDataFrame(self):
        with ah_db.open_db_connection('moneyMovement') as conn_mm:
            sql = '''
            SELECT ft.CreatedOn, ft.Amount, ft.PostingDate, c.CreditReasonId
            FROM FundsTransfers ft
            JOIN CustomerServiceCredits c
            ON ft.FundsTransferId = c.TransactionId
            WHERE UserId = {}
            ORDER BY CreatedOn ASC'''.format(self.UserId)

            df_out = getSQLdata(sql, conn_mm)
            if df_out.shape[0] == 0:
                df_out = pd.DataFrame(columns=['CreatedOn',
                                               'Amount',
                                               'PostingDate',
                                               'CreditReasonId'])
            return df_out

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features

    def countCSCreditsInNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        return df_tmp.shape[0]

    def totalCreditAmountInNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        return df_tmp['Amount'].sum()

    def countCSCredits(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time)]
        return df_tmp.shape[0]

    def totalCreditAmount(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time)]
        return df_tmp['Amount'].sum()

    def mostRecentCreditReasonIdInNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['CreatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        if df_tmp.shape[0] == 0:
            return 0
        else:
            return df_tmp.iloc[-1]['CreditReasonId']


# ----------------------------------------#
#               notes part                #
# ----------------------------------------#
class feature_note(object):
    def __init__(self, UserId, N=12):
        self.features = dict()
        self.UserId = UserId
        # self.conn_misc = conn_misc
        self.N = N
        self.df = self.getDataFrame()
        self.df['CreatedOn'] = pd.to_datetime(self.df['Date'])

    def getDataFrame(self):
        with ah_db.open_db_connection('miscellaneous') as conn_misc:
            sql = '''
            SELECT IsHeadLine, NoteCategoryId, Date, UserId
            FROM Notes
            WHERE UserId = {}
            ORDER BY Date ASC'''.format(self.UserId)

            df_out = getSQLdata(sql, conn_misc)
            if df_out.shape[0] == 0:
                df_out = pd.DataFrame(columns=['IsHeadLine',
                                               'NoteCategoryId',
                                               'Date'])
            return df_out

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features

    def daySinceLastNote(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time)]

        if df_tmp.shape[0] == 0:
            days = 365
        else:
            days = min((time - df_tmp.iloc[-1]['CreatedOn']).days, 365)
        return days

    def countOfNotes(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time)]
        return df_tmp.shape[0]

    def hasHeadlineNote(self, df_in, time):
        df_tmp = df_in[(df_in['IsHeadLine'] == 1)]
        return int(df_tmp.shape[0] > 0)

    def mostRecentNoteCategoryId(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time)]
        if df_tmp.shape[0] == 0:
            return 0
        else:
            return df_tmp.iloc[-1]['NoteCategoryId']

# 
#
# if __name__ == "__main__":
#     from datetime import datetime, timedelta
#     import pandas as pd
#     import ah_config
#     ah_config.initialize()
#     print("here")
#     dat = datetime.utcnow() +timedelta(hours=-5)
#
#     dat = pd.to_datetime(dat)
#
#     ts = NewFeature(420)
#     print("here2")
#     ft = ts.buildFeatures(dat)
#     print(ft)
#     print("here3")
