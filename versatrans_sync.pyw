"""Script to handle syncing of student info to Versatrans.

https://github.com/Philip-Greyson/D118-Versatrans-Sync

Needs oracledb: pip install oracledb --upgrade
Needs pysftp: pip install pysftp --upgrade
"""

# importing modules
import datetime  # used to get current date for course info
import os  # needed to get environement variables
from datetime import *

import oracledb  # used to connect to PowerSchool database
import pysftp  # used to connect to the Versatrans SFTP server and upload the file

DB_UN = os.environ.get('POWERSCHOOL_READ_USER')  # username for read-only database user
DB_PW = os.environ.get('POWERSCHOOL_DB_PASSWORD')  # the password for the database account
DB_CS = os.environ.get('POWERSCHOOL_PROD_DB')  # the IP address, port, and database name to connect to in format x.x.x.x:port/db

#set up sftp login info, stored as environment variables on system
SFTP_UN = os.environ.get('VERSATRANS_SFTP_USERNAME')  # username for the SFTP upload
SFTP_PW = os.environ.get('VERSATRANS_SFTP_PASSWORD')
SFTP_HOST = os.environ.get('VERSATRANS_SFTP_ADDRESS')  # server address for the SFTP upload, in our case its a powerschool.com subdomain
CNOPTS = pysftp.CnOpts(knownhosts='known_hosts')  # connection options to use the known_hosts file for key validation
SFTP_PATH = 'Wauconda Community Unit School District 118, IL'  # remote path on the SFTP server that files will be placed in

print(f'DB Username: {DB_UN} | DB Password: {DB_PW} | DB Server: {DB_CS}')  # debug so we can see where oracle is trying to connect to/with
print(f'SFTP Username: {SFTP_UN} | SFTP Server: {SFTP_HOST}')  # debug so we can see what info sftp connection is using

OUTPUT_FILENAME = 'd118_students.csv'

# DEFINE WHAT PARTS OF THE INFO WE WANT TO SEND. USEFUL FOR TESTING, CUSTOMIZING, ETC

DO_BASIC_INFO = True
BASIC_INFO_HEADER = 'Student ID\tActive\tFamily ID\tLast Name\tFirstName\tMiddleName\tGrade\tSchool\tProgram Code\tHomeStreetAddress\tHome Apt. Number\tHomeCity\tHomeState\tHomeZip\tGender\tBirthdate\tHome Phone'

DO_PICKUP_DROPOFF = True
PICKUP_DROPOFF_HEADER = 'Pickup Street Address\tPickup City\tPickup State\tPickup Zip\tDropoff Street Address\tDropoff City\tDropoff State\tDropoff Zip'

DO_EMERGENCY_CONTACTS = True
EMERGENCY_CONTACTS_NUM = 6
EMERGENCY_CONTACTS_HEADER = 'Emergency Contact # Name\tEmergency Contact # Relationship\tEmergency Contact # Phone Number'

DO_AUTHORIZED_ADULTS = True
AUTHORIZED_ADULTS_NUM = 6
AUTHORIZED_ADULTS_HEADER = 'Authorized Adult # Name\tAuthorized Adult # Phone\tAuthorized Adult # Relationship'

DO_FRIDAY_CHILDCARE = True
FRIDAY_CHILDCARE_HEADER = 'Friday Childcare Provider Address\tFriday Childcare Provider City\tFriday Childcare Provider Zip\tFriday Childcare Provider Name\tFriday Childcare Provider Phone'

DO_ACCOMODATIONS = True
ACCOMODATIONS_HEADER = 'Special Instructions\tSpecial Transportation Info\tNo Adult Supervision\tDivorce Accomodations\tIEP Information\tBIP Information\t504 Information'

def construct_header(existingHeader, newHeader, count=None) -> str:
    """Helper function to make a header string with variable amounts of entries."""
    if count:
        tempHeader = ''
        for i in range(1,7):
            numHeader = newHeader.replace('#', f'#{i}')  # replace the empty number signs with the numerical counter
            tempHeader = tempHeader + numHeader + '\t'  # add on the numbered header to a temp header string
        newHeader = tempHeader[:len(tempHeader)-1]  # replace the initial newHeader with the fully replaced numerical header, taking off the last character which is an extra \t
    # if there was no count, just add the passed string to the existing one, otherwise the newHeader will have the numerical header in it to be added on
    if existingHeader != '':  # if the existing header is not empty
        header = f'{existingHeader}\t{newHeader}'  # add the new header to the existing one with a tab delimiter between
    else:  # if the current header is empty, then just take the new header as the header
        header = newHeader
    return header
if __name__ == '__main__':  # main file execution
    with open('versatrans_log.txt', 'w') as log:  # open logging file
        startTime = datetime.now()
        print(f'INFO: Execution started at {startTime}')
        print(f'INFO: Execution started at {startTime}', file=log)
        with open(OUTPUT_FILENAME, 'w') as output:
            headerString = ''  # start with an empty string for the header
            if DO_BASIC_INFO:
                headerString = construct_header(headerString, BASIC_INFO_HEADER)
            if DO_PICKUP_DROPOFF:
                headerString = construct_header(headerString, PICKUP_DROPOFF_HEADER)
            if DO_ACCOMODATIONS:
                headerString = construct_header(headerString, ACCOMODATIONS_HEADER)
            if DO_FRIDAY_CHILDCARE:
                headerString = construct_header(headerString, FRIDAY_CHILDCARE_HEADER)
            if DO_EMERGENCY_CONTACTS:
                headerString = construct_header(headerString, EMERGENCY_CONTACTS_HEADER, EMERGENCY_CONTACTS_NUM)
            if DO_AUTHORIZED_ADULTS:
                headerString = construct_header(headerString, AUTHORIZED_ADULTS_HEADER, AUTHORIZED_ADULTS_NUM)
            print(headerString, file=output)  # output the header string to the .csv file
            try:
                with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:  # create the connecton to the database
                    print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}')
                    print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}', file=log)
                    with con.cursor() as cur:  # start an entry
                        cur.execute('SELECT s.dcid, s.id, s.student_number, s.enroll_status, s.first_name, s.last_name, s.middle_name, s.grade_level, s.schoolid, s.mailing_street, s.mailing_city, s.mailing_state, s.mailing_zip, s.gender, s.dob, s.home_phone FROM students s WHERE s.student_number = 225352')
                        students = cur.fetchall()
                        for student in students:
                            stuDCID = student[0]
                            stuID = student[1]
                            stuNum = student[2]
                            studentActive = True if student[3] == 0 else False
                            stuFirst = student[4]
                            stuLast = student[5]
                            stuMiddle = student[6]
                            grade = int(student[7])
                            schoolCode = int(student[8])
                            homeAddress = str(student[9])
                            homeCity = str(student[10])
                            homeState = str(student[11])
                            homeZip = str(student[12])
                            gender = student[13]
                            birthdate = student[14].strftime('%m/%d/%Y')
                            homePhone = student[15]
                            print(stuDCID)
                        if DO_EMERGENCY_CONTACTS:
                            cur.execute('SELECT ca.contactpriorityorder, p.firstname, p.lastname, codeset.code, ph.phonenumberasentered, cd.iscustodial, cd.liveswithflg, cd.schoolpickupflg, cd.isemergency, cd.receivesmailflg, ca.personid FROM studentcontactassoc ca INNER JOIN studentcontactdetail cd ON cd.studentcontactassocid = ca.studentcontactassocid AND cd.isactive = 1 LEFT JOIN codeset ON ca.CURRRELTYPECODESETID = codeset.codesetid LEFT JOIN person p ON ca.personid = p.id LEFT JOIN personphonenumberassoc ph ON ca.personid = ph.personid AND ph.ispreferred = 1 WHERE ca.studentdcid = :dcid ORDER BY ca.contactpriorityorder', dcid=stuDCID)
                            contacts = cur.fetchall()
                            for contact in contacts:
                                # change to do subquery with phone numbers to make the first one less obnoxious? and be able to take a phone number even if it isnt preferred
                                print(contact)
            except Exception as er:
                print(f'ERROR while doing initial PS connection or query: {er}')
                print(f'ERROR while doing initial PS connection or query: {er}', file=log)
        # with pysftp.Connection(SFTP_HOST, username=SFTP_UN, password=SFTP_PW, private_key='private.pem', cnopts=CNOPTS) as sftp:  # uses a private key file to authenticate with the server, need to pass the path
        #     print(f'INFO: SFTP connection to {SFTP_HOST} established successfully')
        #     print(f'INFO: SFTP connection to {SFTP_HOST} established successfully', file=log)
        #     # print(sftp.pwd) # debug, show what folder we connected to
        #     # print(sftp.listdir())  # debug, show what other files/folders are in the current directory
        #     sftp.chdir(SFTP_PATH)  # change to the specified folder/path
        #     print(sftp.pwd) # debug, make sure out changedir worked
        #     print(sftp.listdir())
        #     # sftp.put(OUTPUT_FILENAME)  # upload the first file onto the sftp server
        #     print("INFO: Student file placed on remote server")
        #     print("INFO: Student file placed on remote server", file=log)