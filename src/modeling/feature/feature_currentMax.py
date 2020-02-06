import pandas as pd
import ah_db
from modeling.misc import misc

class CurrentMaxFeature(object):
    def __init__(self, UserId):
        self.currentMax = feature_currentMax(UserId)


    def buildFeatures(self, time):
        features = dict()
        features.update(self.currentMax.assembleFeatures(time))
        return features

def assembleFeatures(object, time):
    features = dict()
    for name in (dir(object)):
        if callable(getattr(object, name)) and \
           '__' not in name and \
                name != 'getDataFrame' and \
                name != 'assembleFeatures':
            featurename = 'max_' + name
            features[featurename] =\
                getattr(object, name)(object.currMax.copy(), object.prevMax.copy(), time)
    return features

# ----------------------------------------#
#          Current Max part               #
# ----------------------------------------#
class feature_currentMax(object):
    def __init__(self, UserId):
        self.features = dict()
        self.UserId = UserId
        self.currMax, self.prevMax = self.getDataFrame()

    def getDataFrame(self):

        sql_curr = '''
                    select MaxLimit
                    from users
                    where userid={} 
                    '''.format(self.UserId)

        currMax = pd.DataFrame(ah_db.execute_to_json('miscellaneous', sql_curr))

        sql_prev = '''
                    SELECT MaxLimitBefore, MaxLimitAfter, AdjustmentAmount, createdon
                    FROM usermodelmaxadjustmentresulthistory
                    WHERE userid={}
                    ORDER BY createdon 
                    '''.format(self.UserId)

        prevMax = pd.DataFrame(ah_db.execute_to_json('risk', sql_prev))

        return currMax, prevMax

    def assembleFeatures(self, time):
        self.features = assembleFeatures(self, time)
        return self.features

    def currentMax(self, currMax, prevMax, time):

        if prevMax.shape[0] == 0:
            return currMax['MaxLimit'].iloc[0]
        else:
            prevMax = prevMax[prevMax['createdon'] > time]
            if prevMax.shape[0] > 0:
                return prevMax['MaxLimitBefore'].iloc[0]
            else:
                return currMax['MaxLimit'].iloc[0]


if __name__ == "__main__":
     from datetime import datetime, timedelta
     import ah_config
     ah_config.initialize()

     dat = pd.to_datetime('2019-04-21')
     # 6044174, 5708397, 8726
     ts = CurrentMaxFeature(8726)
     ft = ts.buildFeatures(dat)
     print(ft)




