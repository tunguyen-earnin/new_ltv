#!/usr/bin/env python

import ah_db
import editdistance
import re
from utils import *


class IDRiskReason:
    NoRisk, Device, Phone, EmailName, NoDeviceInfoOrNoUserid = range(5)


class IdentityRisk:
    def __init__(self, uid):
        self.uid = uid
        self.score = 0
        self.reasonCode = IDRiskReason.NoRisk
        self.ids = []

    def toString(self):
        return "%.2f%s%s" % (self.score, self.reasonCode, self.ids)

    def toDict(self):
        result = {
            'userId': self.uid,
            'score': round(self.score, 2),
            'reasonCode': self.reasonCode,
            'userIds': self.ids
                }

        return result


def checkIdentity(uid):

    sql = '''
          SELECT u.userid, UserName, FirstName, LastName, OS, Serial, 
                Jailbroken, DevicePhoneNumber, DeviceStaticId
          FROM miscellaneous.users u
          LEFT JOIN miscellaneous.UserDevices ud ON ud.userid = u.userid
          LEFT JOIN miscellaneous.devices d ON d.deviceid = ud.DeviceID
          WHERE u.userid = {} AND (Serial IS NOT NULL OR DeviceStaticId IS NOT NULL) 
          AND (Serial != '' OR DeviceStaticId != '')
          '''.format(uid)

    rows = ah_db.execute_to_json('miscellaneous', sql)

    r = IdentityRisk(uid)
    if len(rows) > 0:
        checkDevice(rows, r)
        if r.score < 1:
            remail = checkUsername(rows[0], r)
    else:
        r.score = 1
        r.reasonCode = IDRiskReason.NoDeviceInfoOrNoUserid

    return r


def checkDevice(user, r):

    DeviceStaticId = {}
    # android = {'unknown': 1, '0123456789ABCDEF': 1} # need special handing in the future

    for u in user:
        if u['DeviceStaticId'] not in DeviceStaticId:
            DeviceStaticId[u['DeviceStaticId']] = 1

            sql = '''SELECT DISTINCT u.userid
                     FROM miscellaneous.devices d 
                     LEFT JOIN miscellaneous.UserDevices ud ON d.deviceid = ud.DeviceID 
                     LEFT JOIN miscellaneous.users u ON ud.userid=u.userid
                     WHERE u.userid != {0} AND u.statusid != 6 
                     AND DeviceStaticId = '{1}' AND d.OS = '{2}'
                  '''.format(u['userid'], u['DeviceStaticId'], u['OS'])

            matcheduser = ah_db.execute_to_json('miscellaneous', sql)

            if len(matcheduser) > 0:
                r.score = 1
                r.reasonCode = IDRiskReason.Device
                r.ids = [m['userid'] for m in matcheduser]
                return


#    Jailbroken  then no confidence

def checkPhone(user, r):

    r = IdentityRisk()

    for u in user:
        if u['OS'] == 'android':
            u['DevicePhoneNumber'][-10:]
            sql = '''SELECT DISTINCT u.userid 
                    FROM miscellaneous.devices d 
                    LEFT JOIN miscellaneous.UserDevices ud ON d.deviceid = ud.DeviceID 
                    LEFT JOIN miscellaneous.users u ON ud.userid = u.userid
                    WHERE u.userid != {0} AND u.statusid != 6 AND DevicePhoneNumber LIKE '{1}'"
                  '''.format(u['userid'], u['DevicePhoneNumber'][-10:])

            matcheduser = ah_db.execute_to_json('miscellaneous', sql)
            if len(matcheduser) > 0:
                r.score = 1
                r.reasonCode = IDRiskReason.Device
                r.ids = [m['userid'] for m in matcheduser]
                return r

    return r


def usernameType(username):
    p = re.compile(
        r'.*(@).*')

    q = re.compile(
        r'(^[0-9]{10}).*')

    # return 0 if email and 1 if phone number
    if p.match(username.lower()):
        return 0
    elif q.match(username.lower()):
        return 1
    else:
        return 2


def checkUsername(user, r):
    # Check if username is email or phone number
    isphone = usernameType(user['UserName'])

    if isphone == 1:
        pass
    else:
        sql = '''SELECT userid, UserName
                      FROM miscellaneous.users 
                      WHERE userid != {0} AND (FirstName = "{1}" OR LastName = "{2}") AND statusid != 6
                      '''.format(user['userid'], user['FirstName'], user['LastName'])

        matcheduser = ah_db.execute_to_json('miscellaneous', sql)

        email = getEmailLocal(user['UserName'])

        if len(matcheduser) > 0:
            for m in matcheduser:
                memail = getEmailLocal(m['UserName'])
                #    print (user['UserName'],m['UserName'],email,memail,editdistance.eval(memail,email))
                # check longest substring
                if editdistance.eval(memail, email) < 2:
                    if checkCurrentEmployer(user, m) == 1:
                        if checkzip(user, m) == 1:
                            r.score = 1
                            r.reasonCode = IDRiskReason.EmailName
                            r.ids.append(m['userid'])
    return

def checkCurrentEmployer(user1, user2):
    sql1 = '''
          SELECT userid, employerid
          FROM miscellaneous.UserEmploymentDetails
          WHERE userid = {}
          ORDER BY LastUpdatedOn
          LIMIT 1
          '''.format(user1['userid'])

    sql2 = '''
           SELECT userid, employerid
           FROM miscellaneous.UserEmploymentDetails
           WHERE userid = {}
           ORDER BY LastUpdatedOn
           LIMIT 1
           '''.format(user2['userid'])
    try:

        User1Employer = ah_db.execute_to_json('miscellaneous', sql1)
        User2Employer = ah_db.execute_to_json('miscellaneous', sql2)

        if len(User2Employer) > 0:

            employerid1 = User1Employer[0]['employerid']
            employerid2 = User2Employer[0]['employerid']

            if employerid1 == employerid2:
                return 1
            else:
                return 0
        else:
            return 1
    except:
        return 1


def checkzip(user1, user2):
    sql1 = '''
                SELECT DISTINCT Zipcode 
                FROM DataScience.UserPIP
                WHERE userid = {0} AND Zipcode IS NOT NULL AND Zipcode != ''
            '''.format(user1['userid'])

    sql2 = '''
                SELECT DISTINCT Zipcode 
                FROM DataScience.UserPIP
                WHERE userid = {0} AND Zipcode IS NOT NULL AND Zipcode != ''
                '''.format(user2['userid'])

    try:
        with ah_db.open_db_connection('datascience') as conn:
            zipcodes1 = conn.execute(sql1).fetchall()
            zipcodes2 = conn.execute(sql2).fetchall()

        ziplist1 = []
        ziplist2 = []

        for zips in zipcodes1:
            ziplist1.append(zips['Zipcode'])

        for zips in zipcodes2:
            ziplist2.append(zips['Zipcode'])

        overlap = set(ziplist1).intersection(set(ziplist2))

        if len(ziplist1) == 0 or len(ziplist2) == 0:
            return 1
        elif len(overlap) > 0:
            return 1
        else:
            return 0
    except:
        return 1
