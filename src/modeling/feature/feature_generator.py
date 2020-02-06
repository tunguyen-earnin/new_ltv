import sys
import time
from modeling.misc.misc import *
from modeling.feature.etl import *
from modeling.feature.feature_bank import *
from modeling.feature.feature_payroll import *
from modeling.feature.feature_device import *
from modeling.feature.feature_user import *
from modeling.feature.feature_employment import *
from modeling.feature.feature_cs import *



class FeatureGenerator:

    def __init__(self, uid, predTime):
        self.f={}

        self.predTime = predTime
        self.f['predTime']=predTime.isoformat()
        self.f['uid']=uid

    def getFeature(self):
        return self.f
    def feature_currentActivation(self,current):
        # assert data[-1]['RequestTime']<predTime

    #the following feature is only useful for activation risk model
        if self.predTime.hour<6: self.f['isActNight']=1
        else: self.f['isActNight']=0

        self.f['actDayOfWeek']=self.predTime.weekday()
        self.f['IsFail']=current['IsFail']
        self.f['IsLoss']=current['IsLoss']
        self.f['ReturnCode']=current['RestoreReturnCodeAch']
        self.f['RestoreDate']=current['RestoreDate'].isoformat()
        self.f['activation_amount']=current['Amount']
        self.f['activation_days2Restore']=(current['RestoreDate']-self.predTime).days
        self.f['activation_activationid']=current['activationid']
        if self.f['activation_days2Restore']<=0: self.f['activation_days2Restore']=14
# self.isActBfPreRestore
        self.f['activation_days2ActivationDate']=(current['ActivationDate']-self.predTime).days

        # self.f['activation_ActivationOriginId'] = current['ActivationOriginId']

    def feature_generator(self, obj):
        self.f.update(obj.buildFeatures(self.predTime))

    def printFeatures(self,writer):
        #assert features in self.f
        writer.writerow(self.f)

