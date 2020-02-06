import ah_db
import sys,traceback
sys.path.append( '/opt/services/activehours/python-services')
from utils import *
from modeling.feature.feature_generic import FeatureGeneric


class UserFeature(FeatureGeneric):

    def buildFeatures(self,predTime):
        f={}
        try:
            l=self.data[0]
            f['daysSinceSignup']=(predTime-l['CreatedOn']).days

            local, domain= splitEmail(l['UserName'])
            f['com']=f['edu']=f['net']=0
            if domain[-3:]=='com': f['com']=1
            elif domain[-3:]=='edu': f['edu']=1
            elif domain[-3:]=='net': f['net']=1


            f['domain']=domain


            try:
                if stringMatch([l['FirstName'].strip(),l['LastName'].strip()],local): f['nameinemail']=1
                else: f['nameinemail']=0
            except:
                f['nameinemail']=0
            f['digitInEmail'] = sum(c.isdigit() for c in local)
            f['alphaInEmail'] = sum(c.isalpha() for c in local)
            f['emailLength']=len(local)
        except:
            print(self.data)
            traceback.print_exc()

        self.reName(f,'user_')
        return f

    def getData(self):
        sql='''
        SELECT UserName, FirstName, LastName, CreatedOn, IdentityRiskUserName
        FROM Users
        WHERE userid=%s
        '''

        return ah_db.execute_to_json('miscellaneous', sql, (self.uid,))
