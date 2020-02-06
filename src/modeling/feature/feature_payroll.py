import ah_db
import traceback
from modeling.feature.feature_generic import FeatureGeneric


class Payroll(FeatureGeneric):
    nWkMax=20

    def getDataRangeIndex(self,predTime):
        r=-1
        for i in range(len(self.data)-1,-1,-1):
            if self.data[i]['LastUpdatedOn'] is not None and predTime>self.data[i]['LastUpdatedOn']:
                r=i
                break
        return r

    def buildFeatures(self,predTime):
        f={}
        try:
            lastIndex=self.getDataRangeIndex(predTime)
            f['nPayrollSetup']=lastIndex+1#len(self.data)  #should normalize by wksincesignup
            f['nPayrollSetup_Wk4']=0
            f['nUniquePayrollSetupBy']=0
            f['nUniquePayrollSetupBy_Wk4']=0
            f['daySinceLastPayrollSetup']=Payroll.nWkMax*7
            f['daySinceLastPayrollRecordCreate']=Payroll.nWkMax*7
            f['lastPayrollSetupBy']=''
            f['hourlyRate']=0
            f['paycycleFrequencyId']=0
            f['lastPayrollStatus']=0
            f['isLastPayrollComplete']=0
            if lastIndex>=0:

                try:
                    f['daySinceLastPayrollSetup']=(predTime-self.data[lastIndex]['LastUpdatedOn']).days
                except:
                    f['daySinceLastPayrollSetup']=Payroll.nWkMax*7
                try:
                    f['daySinceLastPayrollRecordCreate']=(predTime-self.data[lastIndex]['CreatedOn']).days
                except:
                    f['daySinceLastPayrollRecordCreate']=Payroll.nWkMax*7

                if  f['daySinceLastPayrollSetup']<0:  f['daySinceLastPayrollSetup']=-1
                if  f['daySinceLastPayrollRecordCreate']<0:  f['daySinceLastPayrollRecordCreate']=-1

                f['lastPayrollSetupBy']=self.data[lastIndex]['SetupBy']
                f['hourlyRate']=self.data[lastIndex]['HourlyRate']
                f['paycycleFrequencyId']=self.data[lastIndex]['PaycycleFrequencyId']
                f['lastPayrollStatus']=self.data[lastIndex]['PayrollStatusId']

                if self.data[lastIndex]['PayrollStatusId']==11: f['isLastPayrollComplete']=1


                setupby={}
                setupbywk4={}

                for i in range(lastIndex+1):
                    l=self.data[i]

                    setupby[self.data[i]['SetupBy']]=1

                    try:
                        wks=(predTime-l['LastUpdatedOn']).days/7
                        if wks<4:
                            f['nPayrollSetup_Wk4']+=1
                            setupbywk4[self.data[i]['SetupBy']]=1

                    except:

                        pass
        #                traceback.print_exc()
                f['nUniquePayrollSetupBy']=len(setupby)
                f['nUniquePayrollSetupBy_Wk4']=len(setupbywk4)
        except:
            print(lastIndex,self.data)
            traceback.print_exc()

        self.reName(f,'payroll_')
        return f

    def getData(self):
        sql='''
        SELECT up.userid, HourlyRate, PayrollStatusId, up.CreatedOn, up.LastUpdatedOn, SetupBy, pt.PaycycleFrequencyId
        FROM payroll.UserToPayrollHistory up
        LEFT JOIN payroll.PayrollDefinition pd ON up.PayrollDefinitionId=pd.payrolldefinitionid
        LEFT JOIN payroll.PayDateTypes pt ON pd.PayDateTypeId=pt.PayDateTypeId
        WHERE up.userid=%s
        ORDER BY up.LastUpdatedOn
        '''

        return ah_db.execute_to_json('payroll', sql, (self.uid,))
