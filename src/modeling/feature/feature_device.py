import ah_db
import sys,traceback
sys.path.append( '/opt/services/activehours/python-services')
from modeling.feature.feature_generic import FeatureGeneric


class DeviceFeature(FeatureGeneric):
    nWkMax=20

    def buildFeatures(self,predTime):
        f={}
        try:

            lastIndex=self.getDataRangeIndex(predTime)

            f['nInstall']=lastIndex+1
            f['nInstall_Wk2']=0
            f['daySinceLastInstall']=DeviceFeature.nWkMax*7
            f['lastInstallOS']=''
            if lastIndex>=0:


                try:
                    f['daySinceLastInstall']=(predTime-self.data[lastIndex]['installDate']).days
                except:
                    pass

                if f['daySinceLastInstall']<0: f['daySinceLastInstall']=-1
                f['lastInstallOS']=self.data[lastIndex]['OS']



                for i in range(lastIndex+1):
                    l=self.data[i]
                    try:
                        wks=(predTime-l['installDate']).days/7
                        if wks<2: f['nInstall_Wk2']+=1

                    except:

                        pass
        #                traceback.print_exc()
        except:
            print(lastIndex,self.data)
            traceback.print_exc()

        self.reName(f,'device_')
        return f

    def getData(self):
        sql='''
        SELECT userid, d.CreatedOn, PhoneNumber, DeviceTypeId, OS, Jailbroken, DevicePhoneNumber
        FROM UserDevices ud
        LEFT JOIN devices d
        ON ud.deviceid=d.deviceid
        WHERE userid=%s
        ORDER BY ud.createdon
        '''

        return ah_db.execute_to_json('miscellaneous', sql, (self.uid,))
