import ah_db
import pandas as pd
import numpy as np
from modeling.misc.misc import getSQLdata

class TimeSheetFeature(object):

    def __init__(self, UserId):
        self.hours = feature_hours(UserId)
        self.pics = feature_pics(UserId)

    def buildFeatures(self, time):
        features = dict()
        features.update(self.hours.assembleFeatures(time))
        features.update(self.pics.assembleFeatures(time))
        return features


class TimeSheetFeature_predict(object):

    def __init__(self, UserId):
        self.hours = feature_hours(UserId)
        self.pics = feature_pics(UserId)

    def buildFeatures(self, time):
        features = dict()
        features.update(self.hours.assembleFeatures(time))
        features.update(self.pics.assembleFeatures(time))
        return features


def assembleFeatures(object, time):
    features = dict()
    for name in (dir(object)):
        if callable(getattr(object, name)) and \
           '__' not in name and \
           name != 'getDataFrame' and \
           name != 'assembleFeatures':
            featurename = 'timesheet_' + name
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
#              hours part                 #
# ----------------------------------------#
class feature_hours(object):

    def __init__(self, UserId, N=2):
        self.features = dict()
        self.UserId = UserId
        self.N = N
        self.df = self.getDataFrame()
        self.df['CreatedOn'] = pd.to_datetime(self.df['CreatedOn'])
        self.df['Date'] = pd.to_datetime(self.df['Date'])
        try:
            self.earlistDate = self.__earlistDate__(self.df.copy())
        except:
            self.earlistDate = pd.to_datetime('2100-01-01')

    def getDataFrame(self):
        with ah_db.open_db_connection('miscellaneous') as conn_misc:
            sql = '''
            SELECT t.UserId, t.Date, t.TotalHours,
            t.CreatedOn, t.TimeSheetProviderId, tp.Name
            FROM TimeSheets AS t
            JOIN TimeSheetProviders as tp
            ON t.TimeSheetProviderId = tp.TimeSheetProviderId
            WHERE UserId={} '''.format(self.UserId)

            df_out = getSQLdata(sql, conn_misc)

            return df_out

    def __earlistDate__(self, df_in):
        df_tmp = df_in[(pd.notnull(df_in.CreatedOn))]
        return pd.to_datetime(df_tmp.CreatedOn.min())

    def earlistHoursDate(self, df_in, time):
        return self.earlistDate

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features

    def totalHoursinNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['Date'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        return df_tmp['TotalHours'].sum()

    def avgHoursPerTimesheetinNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['Date'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        result = df_tmp['TotalHours'].mean()
        return [result, 0][np.isnan(result)]

    def stdHoursinNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['Date'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        result = df_tmp['TotalHours'].std()
        return [result, 0][np.isnan(result)]

    def recentTimeSheetProvider(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time)]
        df_tmp = df_tmp[(df_tmp['CreatedOn'] == df_tmp['CreatedOn'].max())]
        return df_tmp['Name'].values[0]

    def countOfUniqueTimeSheetProviderinNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['CreatedOn'] <= time) &
                       (df_in['Date'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        return len(df_tmp['Name'].unique())


# class feature_hours_predict(feature_hours):

#     def __init__(self, UserId, conn, N=14):
#         self.features = dict()
#         self.UserId = UserId
#         self.conn = conn
#         self.N = N
#         self.df = self.getDataFrame(self.UserId, self.conn)
#         try:
#             self.earlistDate = self.__earlistDate__(self.df.copy())
#         except:
#             self.earlistDate = pd.to_datetime('2100-01-01')

#     def getDataFrame(self, UserId, conn):
#         sql = '''
#         SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
#         SELECT t.UserId, t.Date, t.TotalHours,
#         t.CreatedOn, t.TimeSheetProviderId, tp.Name
#         FROM dbo.TimeSheets AS t
#         JOIN Payroll.TimeSheetProviders as tp
#         ON t.TimeSheetProviderId = tp.TimeSheetProviderId
#         ORDER BY CREATEDOn DESC
#         WHERE UserId = ''' + str(UserId)

#         df_tmp = getSQLdata(sql, conn)

#         return df_tmp


# ----------------------------------------#
#               pics part                 #
# ----------------------------------------#
class feature_pics(object):

    def __init__(self, UserId, N=2):
        self.features = dict()
        self.UserId = UserId
        self.N = N
        self.df = self.getDataFrame()
        self.df['LastUpdatedOn'] = pd.to_datetime(self.df['LastUpdatedOn'])
        try:
            self.earlistDate = self.__earlistDate__(self.df.copy())
        except:
            self.earlistDate = pd.to_datetime('2100-01-01')

    def getDataFrame(self):
        with ah_db.open_db_connection('miscellaneous') as conn_misc:

            sql = '''
            SELECT UserID, pt.PictureTypeId, pt.Typename,
            LastUpdatedOn, ps.StatusName
            FROM UserTimeSheetPictures AS utp
            JOIN PictureStatuses AS ps
            ON utp.PictureStatusId = ps.PictureStatusId
            JOIN PictureTypes as pt
            ON utp.PictureTypeId = pt.PictureTypeId
            WHERE StatusName = 'processed'
            AND UserId={} '''.format(self.UserId)

            df_out = getSQLdata(sql, conn_misc)

            return df_out

    def __earlistDate__(self, df_in):
        df_tmp = df_in[(pd.notnull(df_in.LastUpdatedOn))]
        return pd.to_datetime(df_tmp.LastUpdatedOn.min())

    def earlistPicsDate(self, df_in, time):
        return self.earlistDate

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features

    def countTimeSheetPicsinNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['LastUpdatedOn'] <= time) &
                       (df_in['LastUpdatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days'))]
        return df_tmp.shape[0]

    def countValidTimeSheetPicsinNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['LastUpdatedOn'] <= time) &
                       (df_in['LastUpdatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days')) &
                       (df_in['PictureTypeId'] == 1)]
        return df_tmp.shape[0]

    def countInvalidTimeSheetPicsinNweeks(self, df_in, time):
        df_tmp = df_in[(df_in['LastUpdatedOn'] <= time) &
                       (df_in['LastUpdatedOn'] >=
                        time - pd.to_timedelta(str(self.N*7)+'days')) &
                       (df_in['PictureTypeId'] != 1)]
        return df_tmp.shape[0]

    def percentInvalidTimeSheetPics(self, df_in, time):
        return self.countInvalidTimeSheetPicsinNweeks(df_in, time) / \
            (self.countTimeSheetPicsinNweeks(df_in, time) + 1E-6)

# class feature_pics_predict(feature_pics):
#     def __init__(self, UserId, conn_misc, N=2):
#         self.features = dict()
#         self.UserId = UserId
#         self.conn_misc = conn_misc
#         self.N = N
#         self.df = self.getDataFrame(self.UserId, self.conn_misc)
#         try:
#             self.earlistDate = self.__earlistDate__(self.df.copy())
#         except:
#             self.earlistDate = pd.to_datetime('2100-01-01')


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
#     ts = TimeSheetFeature(420)
#     print("here2")
#     ft = ts.buildFeatures(dat)
#     print(ft)
#     print("here3")
