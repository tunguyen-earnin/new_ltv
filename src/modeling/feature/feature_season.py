import traceback
import pandas as pd
import holidays
from modeling.misc import misc


class SeasonFeature(object):
    def __init__(self):
        self.holiday = feature_holiday()
        self.taxMonth = feature_taxMonth()

    def buildFeatures(self, time):
        features = dict()
        features.update(self.holiday.assembleFeatures(time))
        features.update(self.taxMonth.assembleFeatures(time))
        return features

def assembleFeatures(object, time):
    features = dict()
    for name in (dir(object)):
        if callable(getattr(object, name)) and \
           '__' not in name and \
                name != 'assembleFeatures' and \
            'Table' not in name:
            featurename = 'season_' + name
            features[featurename] =\
                getattr(object, name)(time)
    return features

# ----------------------------------------#
#            holiday part                 #
# ----------------------------------------#

class feature_holiday(object):
    def __init__(self):
        self.features = dict()

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features

    def holidayTable(self):

        holidayMonth = [ptr[0] for ptr in holidays.US(years=(2018, 2019)).items()]
        holidayMonth = pd.DataFrame(holidayMonth)
        holidayMonth.columns = ['date']
        holidayMonth['date'] = pd.to_datetime(holidayMonth['date']).dt.strftime('%Y-%m-%d')
        holidayMonth['month'] = pd.to_datetime(holidayMonth['date']).dt.month
        holidayMonth['year'] = pd.to_datetime(holidayMonth['date']).dt.year
        holidayMonth['week'] = pd.to_datetime(holidayMonth['date']).dt.week
        holidayMonth['weekday'] = pd.to_datetime(holidayMonth['date']).dt.weekday
        holidayMonth['isHolidayWeek'] = 1
        holidayMonthCnt = holidayMonth.groupby(['year', 'month']).size().reset_index()
        holidayMonthCnt.columns = ['year', 'month', 'cntHolidayMonth']
        holidayInMonday = holidayMonth.groupby(['year', 'month'])['weekday'].apply(
            lambda x: x[x == 0].count()).reset_index()
        holidayInMonday.columns = ['year', 'month', 'cntHolidayMonday']
        holiday = pd.merge(holidayMonthCnt, holidayInMonday, how='left', left_on=['year', 'month'],
                           right_on=['year', 'month'])
        holidayInWeek = holidayMonth[['year', 'week', 'isHolidayWeek']]

        return holiday, holidayInWeek

    def weekday(self, time):

        time = pd.to_datetime(time) + pd.to_timedelta('2days')
        df_tmp = pd.DataFrame({'time': [time]})
        df_tmp['weekday'] = df_tmp['time'].dt.weekday

        return df_tmp['weekday'].iloc[0]

    def isHolidayWeek(self, time):

        holiday, holidayInWeek = self.holidayTable()
        time = pd.to_datetime(time) + pd.to_timedelta('2days')
        df_tmp = pd.DataFrame({'time': [time]})
        df_tmp['year'] = pd.to_datetime(df_tmp['time']).dt.year
        df_tmp['week'] = pd.to_datetime(df_tmp['time']).dt.week
        df_tmp = pd.merge(df_tmp, holidayInWeek, how='left', left_on=['year', 'week'], right_on=['year', 'week'])
        df_tmp.loc[df_tmp['isHolidayWeek'].isnull(), 'isHolidayWeek'] = 0

        return df_tmp['isHolidayWeek'].iloc[0]

    def cntHolidayMonth(self, time):

        holiday, holidayInWeek = self.holidayTable()
        time = pd.to_datetime(time) + pd.to_timedelta('2days')
        df_tmp = pd.DataFrame({'time': [time]})
        df_tmp['year'] = pd.to_datetime(df_tmp['time']).dt.year
        df_tmp['month'] = pd.to_datetime(df_tmp['time']).dt.month
        df_tmp = pd.merge(df_tmp, holiday, how='left', left_on=['year', 'month'], right_on=['year', 'month'])
        df_tmp.loc[df_tmp['cntHolidayMonth'].isnull(), 'cntHolidayMonth'] = 0

        return df_tmp['cntHolidayMonth'].iloc[0]

    def cntHolidayMonday(self, time):

        holiday, holidayInWeek = self.holidayTable()
        time = pd.to_datetime(time) + pd.to_timedelta('2days')
        df_tmp = pd.DataFrame({'time': [time]})
        df_tmp['year'] = pd.to_datetime(df_tmp['time']).dt.year
        df_tmp['month'] = pd.to_datetime(df_tmp['time']).dt.month
        df_tmp = pd.merge(df_tmp, holiday, how='left', left_on=['year', 'month'], right_on=['year', 'month'])
        df_tmp.loc[df_tmp['cntHolidayMonday'].isnull(), 'cntHolidayMonday'] = 0

        return df_tmp['cntHolidayMonday'].iloc[0]

# ----------------------------------------#
#            tax part                     #
# ----------------------------------------#

class feature_taxMonth(object):

    def __init__(self):
        self.features = dict()

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features

    def taxMonthTable(self):

        taxMonth = {'month': [2, 3], 'isTaxMonth': [1, 1]}
        taxMonth = pd.DataFrame(taxMonth)

        return taxMonth

    def isTaxMonth(self, time):

        taxMonthTable = self.taxMonthTable()

        time = pd.to_datetime(time) + pd.to_timedelta('2days')
        df_tmp = pd.DataFrame({'time': [time]})
        df_tmp['month'] = pd.to_datetime(df_tmp['time']).dt.month
        df_tmp = df_tmp.merge(taxMonthTable, how='left', on='month')
        df_tmp.loc[df_tmp['isTaxMonth'].isnull(), 'isTaxMonth'] = 0

        return df_tmp['isTaxMonth'].iloc[0]

if __name__ == "__main__":

 from datetime import datetime, timedelta
 import pandas as pd
 import ah_config
 ah_config.initialize()


 dat = pd.to_datetime('2019-03-01')
 ts = SeasonFeature()
 ft = ts.buildFeatures(dat)
 print(ft)
