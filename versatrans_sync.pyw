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
ACCOMODATIONS_HEADER = 'Special Instructions\tSpecial Transportation Info\tNo Adult Supervision\tDivorce Accomodations\tIEP Information\tBIP Information\t504 Information\tMedical Alert'

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
            finalOutputString = None # string to store the final output for each student
            try:
                with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:  # create the connecton to the database
                    print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}')
                    print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}', file=log)
                    with con.cursor() as cur:  # start an entry
                        cur.execute('SELECT s.dcid, s.id, s.student_number, s.enroll_status, s.first_name, s.last_name, s.middle_name, s.grade_level, s.schoolid, s.mailing_street, s.mailing_city, s.mailing_state, s.mailing_zip, s.gender, s.dob, s.home_phone, ext.tran_babysitter_address, ext.tran_babysitter_city, ext.tran_babysitter_state, ext.tran_babysitter_zip FROM students s LEFT JOIN u_def_ext_students0 ext ON s.dcid = ext.studentsdcid WHERE s.student_number = 224206')
                        students = cur.fetchall()
                        for student in students:
                            stuDCID = student[0]
                            stuID = student[1]
                            stuNum = student[2]
                            stuActive = True if student[3] == 0 else False
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
                            if DO_BASIC_INFO:
                                print('DBUG: Starting Basic Info section')
                                print('DBUG: Starting Basic Info section', file=log)
                                basicOutputString = f'{stuNum}\t{stuActive}\t\t{stuLast}\t{stuFirst}\t{stuMiddle}\t{grade}\t{schoolCode}\t\t{homeAddress}\t\t{homeCity}\t{homeState}\t{homeZip}\t{gender}\t{birthdate}\t{homePhone}'
                                finalOutputString = basicOutputString  # since basic info is first, the final output is always set to it if basic info is enabled
                            if DO_PICKUP_DROPOFF:
                                header = 'Pickup Street Address\tPickup City\tPickup State\tPickup Zip\tDropoff Street Address\tDropoff City\tDropoff State\tDropoff Zip'
                                print('DBUG: Starting Pickup/Dropoff section')
                                print('DBUG: Starting Pickup/Dropoff section', file=log)
                                pickupAddress = student[16]
                                pickupCity = student[17]
                                pickupState = student[18]
                                pickupZip = student[19]
                                # we dont currently have data for dropoff addresses so leave them blank
                                dropoffAddress = ''
                                dropoffCity = ''
                                dropoffState = ''
                                dropoffZip = ''
                                pickupOutputString = f'{pickupAddress}\t{pickupCity}\t{pickupState}\t{pickupZip}\t{dropoffAddress}\t{dropoffCity}\t{dropoffState}\t{dropoffZip}'
                                if finalOutputString:  # if there is already something in the final output, append this section
                                    finalOutputString = f'{finalOutputString}\t{pickupOutputString}'
                                else: # if there is no output yet, just set the final output to this sections output
                                    finalOutputString = pickupOutputString
                            if DO_ACCOMODATIONS:
                                print('DBUG: Starting Accomodations section')
                                print('DBUG: Starting Accomodations section', file=log)
                                cur.execute('SELECT suf.custom_specialinstructions, suf.custom_special_trans, ext.tran_noadultsupervision, ext.tran_divorceaccomodations, ext.customalertiep, ext.customalertbip, suf.c_504_description, alert.mba_alert FROM u_studentsuserfields suf LEFT JOIN u_def_ext_students0 ext ON suf.studentsdcid = ext.studentsdcid LEFT JOIN u_aet_customalerts alert ON suf.studentsdcid = alert.studentsdcid AND alert.u_aet_customalerts_typesid = 1161 WHERE suf.studentsdcid = :student', student=stuDCID)
                                entries = cur.fetchall()
                                if len(entries) > 1:
                                    print(f'WARN: Student {stuNum} has multiple entries for their accomodations section, it may be inaccurate!')
                                    print(f'WARN: Student {stuNum} has multiple entries for their accomodations section, it may be inaccurate!', file=log)
                                specialInstructions = str(entries[0][0]) if entries[0][0] else ''
                                specialInstructions = specialInstructions.replace('\r\n', ';')  # replace any CRLF line breaks with a semicolon
                                specialInfo = str(entries[0][1]) if entries[0][1] else ''
                                specialInfo = specialInfo.replace('\r\n', ';')
                                noAdult = entries[0][2]
                                divorce = str(entries[0][3]) if entries[0][3] else ''
                                divorce = divorce.replace('\r\n', ';')
                                iep = str(entries[0][4]) if entries[0][4] else ''
                                iep = iep.replace('\r\n', ';')
                                bip = str(entries[0][5]) if entries[0][5] else ''
                                bip = bip.replace('\r\n', ';')
                                fiveOFour = str(entries[0][6]) if entries[0][6] else ''
                                fiveOFour = fiveOFour.replace('\r\n', ';')
                                medical = str(entries[0][7])
                                medical = medical.replace('\r\n', ';')
                                accomodationOutputString = f'"{specialInstructions}"\t"{specialInfo}"\t{noAdult}\t"{divorce}"\t"{iep}"\t"{bip}"\t"{fiveOFour}"\t"{medical}"'
                                # print(accomodationOutputString)  # debug
                                if finalOutputString:  # if there is already something in the final output, append this section
                                    finalOutputString = f'{finalOutputString}\t{accomodationOutputString}'
                                else: # if there is no output yet, just set the final output to this sections output
                                    finalOutputString = accomodationOutputString
                            if DO_EMERGENCY_CONTACTS:
                                print('DBUG: Starting Emergency Contacts section')
                                print('DBUG: Starting Emergency Contacts section', file=log)
                                # create empty lists that will hold the name, relationship and phone number of the emergency contacts. Each will be related by a same index
                                contactNames = []  # list of the names
                                contactRelationships = []  # list of the relationships
                                contactPhones = []
                                cur.execute('SELECT ca.contactpriorityorder, p.firstname, p.lastname, codeset.code, cd.iscustodial, cd.liveswithflg, cd.schoolpickupflg, cd.isemergency, cd.receivesmailflg, ca.personid FROM studentcontactassoc ca INNER JOIN studentcontactdetail cd ON cd.studentcontactassocid = ca.studentcontactassocid AND cd.isactive = 1 LEFT JOIN codeset ON ca.CURRRELTYPECODESETID = codeset.codesetid LEFT JOIN person p ON ca.personid = p.id WHERE ca.studentdcid = :dcid ORDER BY cd.isemergency DESC, cd.iscustodial DESC, ca.contactpriorityorder', dcid=stuDCID)
                                contacts = cur.fetchall()
                                # print(len(contacts))
                                # print(contacts)
                                if len(contacts) > EMERGENCY_CONTACTS_NUM:
                                    maxContacts = EMERGENCY_CONTACTS_NUM
                                else:
                                    maxContacts = len(contacts)
                                for i in range(maxContacts):  # go through the amount of entries that we are looking for
                                    # print(contacts[i])  # debug
                                    contactName = f'{contacts[i][1]} {contacts[i][2]}'
                                    contactRelationship = contacts[i][3]
                                    contactID = contacts[i][9]
                                    phoneNum = None  # reset to null for each contact
                                    preferredPhone = False # flag to know if we have a preferred phone number stored
                                    cur.execute('SELECT ph.phonenumberasentered, codeset.code, ph.ispreferred, ph.whencreated FROM personphonenumberassoc ph LEFT JOIN codeset ON ph.phonetypecodesetid = codeset.codesetid WHERE ph.personid = :person ORDER BY ph.ispreferred DESC, ph.whencreated', person=contactID)
                                    phoneNums = cur.fetchall()
                                    if len(phoneNums) > 1:
                                        for i in range(len(phoneNums)):
                                            # print(phoneNums[i])
                                            phoneType = phoneNums[i][1]
                                            isPreferred = phoneNums[i][2]
                                            if isPreferred == 1 and phoneType != 'Mobile' and (not phoneNum or not preferredPhone):  # if the current number entry is preferred but not a mobile number, we should store it unless there is already a preferred number in there
                                                phoneNum = phoneNums[i][0]
                                                preferredPhone = True  # we now have a preferred phone number stored, the only thing that should override is a preferred and mobile number
                                            elif isPreferred == 1 and phoneType == 'Mobile':  # if the current number is preferred and mobile, override no matter what. Newer entries should be later so this should be the most recent preferred and mobile number
                                                phoneNum = phoneNums[i][0]
                                                preferredPhone = True
                                            elif isPreferred == 0 and phoneType != 'Mobile' and not phoneNum:  # if entry is not preferred or mobile, only use it if the current number is empty
                                                phoneNum = phoneNums[i][0]
                                                preferredPhone = False
                                            elif isPreferred == 0 and phoneType == 'Mobile' and not preferredPhone: # if entry is mobile but not preferred, override a different non-preferred number
                                                phoneNum = phoneNums[i][0]
                                                preferredPhone = False
                                        if not phoneNum:  # if after we have gone through the entries we havent stored a number, something went wrong
                                            print(f'ERROR while finding good phone number for contact {contactName} in student {stuNum}, taking first entry')
                                            print(f'ERROR while finding good phone number for contact {contactName} in student {stuNum}, taking first entry', file=log)
                                            phoneNum = phoneNums[0][0]
                                    else:  # if we only have one entry total, just take it
                                        phoneNum = phoneNums[0][0]
                                    # print(f'DBUG: Phone number for contact {contactName} is {phoneNum}')  # debug
                                    contactNames.append(contactName)
                                    contactRelationships.append(contactRelationship)
                                    contactPhones.append(phoneNum)
                                # print(f'DBUG: Student {stuNum} contacts are {contactNames} with relationships {contactRelationships} and numbers {contactPhones}')
                                try:
                                    emergencyOutputString = ''  # string that will countain out output from this section
                                    for j in range(EMERGENCY_CONTACTS_NUM):  # now output all the contacts found or blanks if they do not have as many as we are looking for
                                        if j < len(contactNames):  # if there is an entry for this number
                                            emergencyOutputString = emergencyOutputString + f'{contactNames[j]}\t{contactRelationships[j]}\t{contactPhones[j]}\t'
                                        else:  # if there is no entry, we just output tabs for blank fields
                                            emergencyOutputString = emergencyOutputString + '\t\t\t'
                                    emergencyOutputString = emergencyOutputString[:len(emergencyOutputString)-1]  # take the final output minus the final character that is an extra tab
                                    # print(emergencyOutputString)  # debug
                                    if finalOutputString:  # if there is already something in the final output, append this section
                                        finalOutputString = f'{finalOutputString}\t{emergencyOutputString}'
                                    else: # if there is no output yet, just set the final output to this sections output
                                        finalOutputString = emergencyOutputString

                                except Exception as er:
                                    print(f'ERROR while constructing output string for emergency contacts: {er}')
                                    print(f'ERROR while constructing output string for emergency contacts: {er}', file=log)
                            
                        print(finalOutputString)
                        print(finalOutputString, file=output)
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