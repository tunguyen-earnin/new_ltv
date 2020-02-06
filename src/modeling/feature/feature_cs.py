import ah_db
import sys,traceback
sys.path.append( '/opt/services/activehours/python-services')
from modeling.feature.feature_generic import FeatureGeneric


class CSFeature(FeatureGeneric):
    nWkMax=20


    def buildFeatures(self,predTime):
        f={}
        try:
            lastIndex=self.getDataRangeIndex(predTime)
            f['nAudit']=lastIndex+1 #len(self.data)  #should normalize by wksincesignup
            f['daySinceLastAudit']=CSFeature.nWkMax*7
            f['lastAuditType']=-1
            f['lastAuditSource']=-1
            f['lastAuditUsername']=''
            f['lastAudit1Type']=0
            f['lastAudit1Username']=''
            f['nCredit']=f['nCredit_Wk4']=0
            f['nAudit1']=f['nAudit2']=f['nAudit3']=0
            f['nAudit_Wk4']=f['nAudit1_Wk4']=f['nAudit2_Wk4']=f['nAudit3_Wk4']=0
            if lastIndex>=0:

                try:
                    f['daySinceLastAudit']=(predTime-self.data[lastIndex]['CreatedOn']).days
                except:
                    pass



                f['lastAuditType']=self.data[lastIndex]['AuditTypeId']
                f['lastAuditSource']=self.data[lastIndex]['auditapplicationid']
                f['lastAuditUsername']=self.data[lastIndex]['username']

                for i in range(lastIndex+1):
                    l=self.data[i]
                    if l['auditapplicationid']==1:
                        f['nAudit1']+=1
                        f['lastAudit1Type']=self.data[lastIndex]['AuditTypeId']
                        f['lastAudit1Username']=self.data[lastIndex]['username']
                    elif l['auditapplicationid']==2: f['nAudit2']+=1
                    else: f['nAudit3']+=1

                    if l['AuditTypeId']==15: f['nCredit']+=1
                    try:
                        wks=(predTime-l['CreatedOn']).days/7

                        if wks<4:
                            f['nAudit_Wk4']+=1
                            if l['auditapplicationid']==1: f['nAudit1_Wk4']+=1
                            elif l['auditapplicationid']==2: f['nAudit2_Wk4']+=1
                            else: f['nAudit3_Wk4']+=1

                            if l['AuditTypeId']==15: f['nCredit_Wk4']+=1
                    except:
                        #print(predTime,l['createdon'])
                        traceback.print_exc()
                       # sys.exit()
                        pass

        except:

            traceback.print_exc()

        self.reName(f,'cs_')
        return f

    def getData(self):
        sql='''
        SELECT userid, AuditTypeId,auditapplicationid, username, CreatedOn
        FROM audit
        where userid={}
        order by createdon
        '''.format(self.uid)
        return ah_db.execute_to_json('miscellaneous', sql)
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
#     cs = CSFeature(420)
#     print("here2")
#     ft = cs.buildFeatures(dat)
#     print(ft)
#     print("here3")
