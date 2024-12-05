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
SFTP_PATH = 'Wauconda Community Unit School District 118-IL'  # remote path on the SFTP server that files will be placed in

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
FRIDAY_CHILDCARE_HEADER = 'Friday Childcare Provider Address\tFriday Childcare Provider City\tFriday Childcare Provider State\tFriday Childcare Provider Zip\tFriday Childcare Provider Name\tFriday Childcare Provider Phone'

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
            try:
                with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:  # create the connecton to the database
                    print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}')
                    print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}', file=log)
                    with con.cursor() as cur:  # start an entry
                        cur.execute('SELECT s.dcid, s.id, s.student_number, s.enroll_status, s.first_name, s.last_name, s.middle_name, s.grade_level, s.schoolid, s.mailing_street, s.mailing_city, s.mailing_state, s.mailing_zip, s.gender, s.dob, s.home_phone, ext.tran_babysitter_address, ext.tran_babysitter_city, ext.tran_babysitter_state, ext.tran_babysitter_zip, ext.tran_babysitter_friday_address, ext.tran_babysitter_friday_city, ext.tran_babysitter_friday_state, ext.tran_babysitter_friday_zip, ext.tran_babysitter_friday_name, ext.tran_babysitter_friday_phone FROM students s LEFT JOIN u_def_ext_students0 ext ON s.dcid = ext.studentsdcid WHERE s.schoolid != 999999 AND s.enroll_status = 0')
                        students = cur.fetchall()
                        for student in students:
                            try:
                                finalOutputString = None # string to store the final output for each student
                                stuDCID = student[0]
                                stuID = student[1]
                                stuNum = str(int(student[2]))
                                stuActive = True if student[3] == 0 else False
                                stuFirst = student[4] if student[4] else ''
                                stuLast = student[5] if student[5] else ''
                                stuMiddle = student[6] if student[6] else ''  # leave blank if no middle name
                                grade = int(student[7]) if student[7] else ''
                                schoolCode = int(student[8]) if student[8] else ''
                                homeAddress = str(student[9]) if student[9] else ''
                                homeCity = str(student[10]) if student[10] else ''
                                homeState = str(student[11]) if student[11] else ''
                                homeZip = str(student[12]) if student[12] else ''
                                gender = student[13] if student[13] else ''
                                birthdate = student[14].strftime('%m/%d/%Y') if student[14] else ''
                                homePhone = student[15] if student[15] else ''
                                # print(stuDCID)
                                if DO_BASIC_INFO:
                                    print(f'DBUG: Starting Basic Info section for student {stuNum}, DCID {stuDCID}')
                                    print(f'DBUG: Starting Basic Info section for student {stuNum}, DCID {stuDCID}', file=log)
                                    basicOutputString = f'{stuNum}\t{stuActive}\t\t{stuLast}\t{stuFirst}\t{stuMiddle}\t{grade}\t{schoolCode}\t\t{homeAddress}\t\t{homeCity}\t{homeState}\t{homeZip}\t{gender}\t{birthdate}\t{homePhone}'
                                    finalOutputString = basicOutputString  # since basic info is first, the final output is always set to it if basic info is enabled
                                if DO_PICKUP_DROPOFF:
                                    print(f'DBUG: Starting Pickup/Dropoff section for student {stuNum}, DCID {stuDCID}')
                                    print(f'DBUG: Starting Pickup/Dropoff section for student {stuNum}, DCID {stuDCID}', file=log)
                                    pickupAddress = student[16] if student[16] else ''
                                    pickupCity = student[17] if student[17] else ''
                                    pickupState = student[18] if student[18] else ''
                                    pickupZip = student[19] if student[19] else ''
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
                                    try:
                                        print(f'DBUG: Starting Accomodations section for student {stuNum}, DCID {stuDCID}')
                                        print(f'DBUG: Starting Accomodations section for student {stuNum}, DCID {stuDCID}', file=log)
                                        cur.execute('SELECT suf.custom_specialinstructions, suf.custom_special_trans, ext.tran_noadultsupervision, ext.tran_divorceaccomodations, ext.customalertiep, ext.customalertbip, suf.c_504_description, alert.mba_alert, alert.expiration FROM u_studentsuserfields suf LEFT JOIN u_def_ext_students0 ext ON suf.studentsdcid = ext.studentsdcid LEFT JOIN u_aet_customalerts alert ON suf.studentsdcid = alert.studentsdcid AND alert.u_aet_customalerts_typesid = 1161 WHERE suf.studentsdcid = :student', student=stuDCID)
                                        entries = cur.fetchall()
                                        if len(entries) > 1:
                                            lengthWithoutExpired = len(entries)  # variable to count how many actual entries we have if we ignore expired medical alerts
                                            specialInstructions = ''
                                            specialInfo = ''
                                            noAdult = 'No'
                                            divorce = 'No'
                                            iep = ''
                                            bip = ''
                                            fiveOFour = ''
                                            medical = ''
                                            for i in range(len(entries)):  # go through each entry one at a time
                                                if entries[i][8]:  # if there is an expiration date on the (medical) alert
                                                    if datetime.now() > entries[i][8]:  # check if today is after the expiration date
                                                        lengthWithoutExpired -= 1  # subtract 1 from our counter
                                                        continue  # skip the current entry so expired alerts are not added to the output
                                                newSpecialInstructions = str(entries[i][0]) if entries[i][0] else ''
                                                newSpecialInfo = str(entries[i][1]) if entries[i][1] else ''
                                                newNoAdult = 'Yes' if entries[i][2] == 1 else 'No'
                                                newDivorce = 'Yes' if entries[i][3] == 1 else 'No'
                                                newIep = str(entries[i][4]) if entries[i][4] else ''
                                                newBip = str(entries[i][5]) if entries[i][5] else ''
                                                newFiveOFour = str(entries[i][6]) if entries[i][6] else ''
                                                newMedical = str(entries[i][7]) if entries[i][7] else ''
                                                if newSpecialInstructions != specialInstructions:  # if the entry does not match what is currently in the string
                                                    specialInstructions = specialInstructions + newSpecialInstructions  # add the entry to the string
                                                if newSpecialInfo != specialInfo:
                                                    specialInfo = specialInfo + newSpecialInfo
                                                if newNoAdult != noAdult:
                                                    noAdult = newNoAdult
                                                if newDivorce != divorce:
                                                    divorce = divorce + newDivorce
                                                if newIep != iep:
                                                    iep = iep + newIep
                                                if newBip != bip:
                                                    bip = bip + newBip
                                                if newFiveOFour != fiveOFour:
                                                    fiveOFour = fiveOFour + newFiveOFour
                                                if newMedical!= medical:
                                                    medical = medical + newMedical
                                            if lengthWithoutExpired > 1:  # if the count is still greater than one ignoring expired medical alerts
                                                print(f'WARN: Student {stuNum} has multiple entries for their accomodations section, attempting to combine them but it may be inaccurate!')
                                                print(f'WARN: Student {stuNum} has multiple entries for their accomodations section, attempting to combine them but it may be inaccurate!', file=log)
                                            # print(entries, file=log)
                                        else:
                                            specialInstructions = str(entries[0][0]) if entries[0][0] else ''
                                            specialInfo = str(entries[0][1]) if entries[0][1] else ''
                                            noAdult = 'Yes' if entries[0][2] == 1 else 'No'
                                            divorce = 'Yes' if entries[0][3] == 1 else 'No'
                                            iep = str(entries[0][4]) if entries[0][4] else ''
                                            bip = str(entries[0][5]) if entries[0][5] else ''
                                            fiveOFour = str(entries[0][6]) if entries[0][6] else ''
                                            medical = str(entries[0][7]) if entries[0][7] else ''
                                        # do replacement of text formatting that may be in the entries and breaks output
                                        specialInstructions = specialInstructions.replace('\r\n', ';').replace('\n', ';').replace('\t', '').replace('"', '\'')  # replace any LF or CRLF line breaks with a semicolon, replace any tabs with a space, double quotes with single
                                        specialInfo = specialInfo.replace('\r\n', ';').replace('\n', ';').replace('\t', '').replace('"', '\'')
                                        divorce = divorce.replace('\r\n', ';').replace('\n', ';').replace('\t', '').replace('"', '\'')
                                        iep = iep.replace('\r\n', ';').replace('\n', ';').replace('\t', '').replace('"', '\'')                               
                                        bip = bip.replace('\r\n', ';').replace('\n', ';').replace('\t', '').replace('"', '\'')
                                        fiveOFour = fiveOFour.replace('\r\n', ';').replace('\n', ';').replace('\t', '').replace('"', '\'')
                                        medical = medical.replace('\r\n', ';').replace('\n', ';').replace('\t', '').replace('"', '\'')

                                        accomodationOutputString = f'"{specialInstructions}"\t"{specialInfo}"\t{noAdult}\t{divorce}\t"{iep}"\t"{bip}"\t"{fiveOFour}"\t"{medical}"'
                                        # print(accomodationOutputString)  # debug
                                        if finalOutputString:  # if there is already something in the final output, append this section
                                            finalOutputString = f'{finalOutputString}\t{accomodationOutputString}'
                                        else: # if there is no output yet, just set the final output to this sections output
                                            finalOutputString = accomodationOutputString
                                    except Exception as er:
                                        print(f'ERROR while getting accomodations for {stuNum}: {er}')
                                        print(f'ERROR while getting accomodations for {stuNum}: {er}', file=log)
                                if DO_FRIDAY_CHILDCARE:
                                    print(f'DBUG: Starting Friday Childcare section for student {stuNum}, DCID {stuDCID}')
                                    print(f'DBUG: Starting Friday Childcare section for student {stuNum}, DCID {stuDCID}', file=log)
                                    fridayAddress = student[20] if (student[20] and student[20] != 'None Listed') else ''
                                    fridayCity = student[21] if (student[21] and student[21] != 'None Listed') else ''
                                    fridayState = student[22] if (student[22] and student[22] != 'None Listed') else ''
                                    fridayZip = student[23] if (student[23] and student[23] != 'None Listed') else ''
                                    fridayName = student[24] if (student[24] and student[24] != 'None Listed') else ''
                                    fridayPhone = student[25] if (student[25] and student[25] != 'None Listed') else ''
                                    fridayOutputString = f'{fridayAddress}\t{fridayCity}\t{fridayState}\t{fridayZip}\t{fridayName}\t{fridayPhone}'
                                    if finalOutputString:  # if there is already something in the final output, append this section
                                        finalOutputString = f'{finalOutputString}\t{fridayOutputString}'
                                    else: # if there is no output yet, just set the final output to this sections output
                                        finalOutputString = fridayOutputString
                                if DO_EMERGENCY_CONTACTS:
                                    try:
                                        print(f'DBUG: Starting Emergency Contacts section for student {stuNum}, DCID {stuDCID}')
                                        print(f'DBUG: Starting Emergency Contacts section for student {stuNum}, DCID {stuDCID}',file=log)
                                        # create empty lists that will hold the name, relationship and phone number of the emergency contacts. Each will be related by a same index
                                        contactNames = []  # list of the names
                                        contactRelationships = []  # list of the relationships
                                        contactPhones = []
                                        try:
                                            cur.execute('SELECT ca.contactpriorityorder, p.firstname, p.lastname, codeset.code, cd.iscustodial, cd.liveswithflg, cd.schoolpickupflg, cd.isemergency, cd.receivesmailflg, ca.personid FROM studentcontactassoc ca INNER JOIN studentcontactdetail cd ON cd.studentcontactassocid = ca.studentcontactassocid AND cd.isactive = 1 LEFT JOIN codeset ON ca.CURRRELTYPECODESETID = codeset.codesetid LEFT JOIN person p ON ca.personid = p.id WHERE ca.studentdcid = :dcid ORDER BY cd.isemergency DESC, cd.iscustodial DESC, ca.contactpriorityorder', dcid=stuDCID)
                                            contacts = cur.fetchall()
                                            # print(len(contacts))
                                            # print(len(contacts), file=log)
                                            # print(contacts)
                                            # print(contacts, file=log)
                                            if len(contacts) > EMERGENCY_CONTACTS_NUM:
                                                maxContacts = EMERGENCY_CONTACTS_NUM
                                            else:
                                                maxContacts = len(contacts)
                                            for i in range(maxContacts):  # go through the amount of entries that we are looking for
                                                try:
                                                    # print(contacts[i])  # debug
                                                    contactName = f'{contacts[i][1]} {contacts[i][2]}' if (contacts[i][1] or contacts[i][2]) else ''  # if there is no name in either first or last just output a blank
                                                    contactRelationship = contacts[i][3] if (contacts[i][3] and contacts[i][3] != "Not Set") else ''  # if there is no relationship type output a blank
                                                    contactID = contacts[i][9]
                                                    phoneNum = None  # reset to null for each contact
                                                    preferredPhone = False # flag to know if we have a preferred phone number stored
                                                    cur.execute('SELECT ph.phonenumberasentered, codeset.code, ph.ispreferred, ph.whencreated FROM personphonenumberassoc ph LEFT JOIN codeset ON ph.phonetypecodesetid = codeset.codesetid WHERE ph.personid = :person ORDER BY ph.ispreferred DESC, ph.whencreated', person=contactID)
                                                    phoneNums = cur.fetchall()
                                                    if(phoneNums):  # check if there are any phone numbers listed, some contacts may not have any
                                                        try:
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
                                                        except Exception as er:
                                                            print(f'ERROR while finding best phone number for contact #{contacts[i][0]} of student {stuNum}: {er}')
                                                            print(f'ERROR while finding best phone number for contact #{contacts[i][0]} of student {stuNum}: {er}', file=log)
                                                    else:
                                                        phoneNum = ''  # if they did not have any phone numbers listed for their contact, just output a blank
                                                    # print(f'DBUG: Phone number for contact {contactName} is {phoneNum}')  # debug
                                                    contactNames.append(contactName)
                                                    contactRelationships.append(contactRelationship)
                                                    contactPhones.append(phoneNum)
                                                except Exception as er:
                                                    print(f'ERROR while getting phone numbers for contact #{contacts[i][0]} of student {stuNum}: {er}')
                                                    print(f'ERROR while getting phone numbers for contact #{contacts[i][0]} of student {stuNum}: {er}', file=log)
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
                                        except Exception as er:
                                            print(f'ERROR while getting contacts during emergency contact processing: {er}')
                                            print(f'ERROR while getting contacts during emergency contact processing: {er}', file=log)
                                    except Exception as er:
                                        print(f'ERROR while processing emergency contacts for {stuNum}: {er}')
                                        print(f'ERROR while processing emergency contacts for {stuNum}: {er}',file=log)

                                if DO_AUTHORIZED_ADULTS:
                                    try:
                                        print(f'DBUG: Starting Authorized Adults section for student {stuNum}, DCID {stuDCID}')
                                        print(f'DBUG: Starting Authorized Adults section for student {stuNum}, DCID {stuDCID}', file=log)
                                        # create empty lists that will hold the name, relationship and phone number of the emergency contacts. Each will be related by a same index
                                        authorizedNames = []
                                        authorizedPhones = []
                                        authorizedRelationships = []
                                        baseQuery = 'SELECT ext.tran_adultsup#_name, ext.tran_adultsup#_phone, tran.adultsup#_relationship FROM u_def_ext_students0 ext LEFT JOIN u_student_transportation tran ON ext.studentsdcid = tran.studentsdcid WHERE ext.studentsdcid = :student'
                                        for i in range(1,AUTHORIZED_ADULTS_NUM+1):  # go through 1 to the max number of entries
                                            try:
                                                numberedQuery = baseQuery.replace('#', f'{i}')
                                                # print(numberedQuery)  # debug
                                                cur.execute(numberedQuery, student=stuDCID)
                                                adults = cur.fetchall()
                                                if adults:  # check that there are any results, some very old or bugged students might not have entries in those tables
                                                    authorizedName = adults[0][0] if adults[0][0] else ''  # take the first row since there should only be one for each student
                                                    authorizedPhone = adults[0][1] if adults[0][1] else ''
                                                    authorizedRelationship = adults[0][2] if adults[0][2] else ''
                                                else:
                                                    authorizedName = ''
                                                    authorizedPhone = ''
                                                    authorizedRelationship = ''
                                                # add each name, phone, relationship into the relevant list
                                                authorizedNames.append(authorizedName)  
                                                authorizedPhones.append(authorizedPhone)
                                                authorizedRelationships.append(authorizedRelationship)
                                            except Exception as er:
                                                print(f'ERROR while processing authorized adult #{i} for student {stuNum}: {er}')
                                                print(f'ERROR while processing authorized adult #{i} for student {stuNum}: {er}', file=log)
                                        try:
                                            authorizedOutputString = ''
                                            for j in range(AUTHORIZED_ADULTS_NUM):  # now output all the authorized adults, going through the lists one at a time
                                                authorizedOutputString = authorizedOutputString + f'{authorizedNames[j]}\t{authorizedPhones[j]}\t{authorizedRelationships[j]}\t'
                                            authorizedOutputString = authorizedOutputString[:len(authorizedOutputString)-1]  # take the final output minus the final character that is an extra tab
                                            if finalOutputString:  # if there is already something in the final output, append this section
                                                finalOutputString = f'{finalOutputString}\t{authorizedOutputString}'
                                            else: # if there is no output yet, just set the final output to this sections output
                                                finalOutputString = authorizedOutputString
                                        except Exception as er:
                                            print(f'ERROR while constructing output string for authorized adults: {er}')
                                            print(f'ERROR while constructing output string for authorized adults: {er}', file=log)
                                    except Exception as er:
                                        print(f'ERROR while getting authorized atdults for student {stuNum}: {er}')
                                        print(f'ERROR while getting authorized atdults for student {stuNum}: {er}', file=log)
                                print(finalOutputString)  # debug
                                print(finalOutputString, file=output)
                            except Exception as er:
                                print(f'ERROR while processing student {student[2]}: {er}')
                                print(f'ERROR while processing student {student[2]}: {er}', file=log)
            except Exception as er:
                print(f'ERROR while doing initial PS connection or query: {er}')
                print(f'ERROR while doing initial PS connection or query: {er}', file=log)
        try:
            with pysftp.Connection(SFTP_HOST, username=SFTP_UN, password=SFTP_PW, private_key='private.pem', cnopts=CNOPTS) as sftp:  # uses a private key file to authenticate with the server, need to pass the path
                print(f'INFO: SFTP connection to {SFTP_HOST} established successfully')
                print(f'INFO: SFTP connection to {SFTP_HOST} established successfully', file=log)
                # print(sftp.pwd) # debug, show what folder we connected to
                # print(sftp.listdir())  # debug, show what other files/folders are in the current directory
                sftp.chdir(SFTP_PATH)  # change to the specified folder/path
                # print(sftp.pwd) # debug, make sure out changedir worked
                # print(sftp.listdir())
                sftp.put(OUTPUT_FILENAME)  # upload the first file onto the sftp server
                print("INFO: Student file placed on remote server")
                print("INFO: Student file placed on remote server", file=log)
        except Exception as er:
            print(f'ERROR while connecting via SFTP or putting file on server: {er}')
            print(f'ERROR while connecting via SFTP or putting file on server: {er}', file=log)