import ah_db
import sys,traceback
sys.path.append( '/opt/services/activehours/python-services')
from modeling.feature.feature_generic import FeatureGeneric


class EmploymentFeature(FeatureGeneric):
    nWkMax=20

    def buildFeatures(self,predTime):
        f={}
        try:
            lastIndex=self.getDataRangeIndex(predTime)

            f['nUserEmployment']=lastIndex+1 #len(self.data)  #should normalize by wksincesignup
            f['daySinceLastEmployment']=EmploymentFeature.nWkMax*7


            f['employer']=-1
            f['paytypeid']=-1
            f['nEmployer']=0
            if lastIndex>=0:
                f['employer']=self.data[lastIndex]['employerid']
                f['paytypeid']=self.data[lastIndex]['paytypeid']

                try:
                    f['daySinceLastEmployment']=(predTime-self.data[lastIndex]['CreatedOn']).days
                except:
                    f['daySinceLastEmployment']=EmploymentFeature.nWkMax*7


                employer={}


                for i in range(lastIndex+1):
                    l=self.data[i]

                    employer[l['employerid']]=1


                f['nEmployer']=len(employer)
        except:
            print(lastIndex,self.data)
            traceback.print_exc()

        self.reName(f,'employment_')
        return f

    def getData(self):
        sql='''
        SELECT userid, employerid, paytypeid,
        LastUpdatedOn AS CreatedOn
        FROM UserEmploymentDetails
        WHERE userid=%s
        ORDER BY LastUpdatedOn
        '''

        return ah_db.execute_to_json('miscellaneous', sql, (self.uid,))
