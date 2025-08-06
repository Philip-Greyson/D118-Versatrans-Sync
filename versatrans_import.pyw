# importing modules
import oracledb  # used to connect to PowerSchool database
import pysftp
import datetime  # used to get current date and time
import os  # needed to get environement variables
import acme_powerschool # a library to interact with the PowerSchool REST API. Found and documented here: https://easyregpro.com/acme.php
import json # needed to manipulate the json objects we pass and receive from the API


D118_API_ID = os.environ.get("POWERSCHOOL_API_ID")
D118_API_SECRET = os.environ.get("POWERSCHOOL_API_SECRET")

#set up sftp login info, stored as environment variables on system
SFTP_UN = os.environ.get('VERSATRANS_SFTP_USERNAME')  # username for the SFTP upload
SFTP_PW = os.environ.get('VERSATRANS_SFTP_PASSWORD')
SFTP_HOST = os.environ.get('VERSATRANS_SFTP_ADDRESS')  # server address for the SFTP upload, in our case its a powerschool.com subdomain
CNOPTS = pysftp.CnOpts(knownhosts='known_hosts')  # connection options to use the known_hosts file for key validation
SFTP_PATH = 'Wauconda Community Unit School District 118-IL/Export'  # remote path on the SFTP server that files will be placed in

DB_UN = os.environ.get('POWERSCHOOL_READ_USER')  # username for read-only database user
DB_PW = os.environ.get('POWERSCHOOL_DB_PASSWORD')  # the password for the database account
DB_CS = os.environ.get('POWERSCHOOL_PROD_DB')  # the IP address, port, and database name to connect to in format x.x.x.x:port/db

CUSTOM_FIELD_NAMES = ['student_number', 'custom_pickupbus', 'custom_pickupdescription', 'custom_pickuptime', 'custom_returnbus', 'custom_returndescription', 'custom_returntime']  # what the field names are in powerschool that correlate to the fields from the file

INPUT_FILE_NAME = 'routeImport.csv'  # what name the file will be pulled down via SFTP as
DELIMITER_CHAR = ','  # character used for delimiting the fields. comma for .csv traditionally

print(f'DBUG: DB Username: {DB_UN} | DB Password: {DB_PW} | DB Server: {DB_CS}')  # debug so we can see where oracle is trying to connect to/with
print(f'DBUG: SFTP Username: {SFTP_UN} | SFTP Password: {SFTP_PW} | SFTP Server: {SFTP_HOST}')  # debug so we can see what info sftp connection is using

if __name__ == '__main__': # main file execution
    with open('VtImportLog.txt', 'w') as log:
        startTime = datetime.datetime.now()
        startTime = startTime.strftime('%H:%M:%S')
        print(f'Execution started at {startTime}')
        print(f'Execution started at {startTime}', file=log)

        try:
            with pysftp.Connection(SFTP_HOST, username=SFTP_UN, password=SFTP_PW, private_key='private.pem', cnopts=CNOPTS) as sftp:  # uses a private key file to authenticate with the server, need to pass the path
                print(f'INFO: SFTP connection to {SFTP_HOST} established successfully')
                print(f'INFO: SFTP connection to {SFTP_HOST} established successfully', file=log)
                sftp.chdir(SFTP_PATH)  # change to the specified folder/path
                # print(sftp.pwd) # debug, make sure out changedir worked
                # print(sftp.listdir())
                latestFile = None  # empty variable to hold the name of the latest file
                latestTimestamp = 0  # timestamp of 0 to compare file timestamps against
                for attribute in sftp.listdir_attr():  # get a listing of the files and their attributes in the current directory
                    # print(attribute.st_mtime, attribute.filename)  # debug
                    if attribute.st_mtime > latestTimestamp:  # if the files timestamp is after the current latest timestamp
                        latestTimestamp = attribute.st_mtime  # save the new timestamp as the lates
                        latestFile = attribute.filename  # save the file name in the latest file variable for access later
                if latestFile is not None:
                    sftp.get(latestFile, INPUT_FILE_NAME)  # pull down the file and save it as whatever is defined by the constant
                # sftp.put(OUTPUT_FILENAME)  # upload the first file onto the sftp server
                print("INFO: Student file retrieved from remote server")
                print("INFO: Student file retrieved from remote server", file=log)
        except Exception as er:
            print(f'ERROR while connecting via SFTP or putting file on server: {er}')
            print(f'ERROR while connecting via SFTP or putting file on server: {er}', file=log)


        # use oracledb to get the student number to dcid mapping, then everything else will use the API
        with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con: # create the connecton to the database
            with con.cursor() as cur:  # start an entry cursor
                dcidDict = {}  # create empty dictionary for the student number to dcid mapping
                # Query of all active students
                cur.execute('SELECT dcid, student_number FROM students ORDER BY student_number DESC')
                students = cur.fetchall()
                print('DBUG: Getting student number to DCID mappings')
                print('DBUG: Getting student number to DCID mappings', file=log)
                for student in students:
                    dcid = int(student[0])
                    stuNum = int(student[1])
                    dcidDict.update({stuNum: dcid})  # add the student number to dcid entry in the dict

        with open(INPUT_FILE_NAME) as inFile:
            ps = acme_powerschool.api('d118-powerschool.info', client_id=D118_API_ID, client_secret=D118_API_SECRET) # create ps object via the API to do requests on
            # result = ps.post('ws/v1/student?extensions=u_studentsuserfields', data=json.dumps({'students' : {'student' : [{'@extensions': 'u_studentsuserfields', 'id': '49311', 'client_uid': '49311', 'action': 'UPDATE', '_extension_data': {'_table_extension': [{'name': 'u_studentsuserfields', '_field': [{'name': 'custom_pickupbus', 'value': ''}]}]}}]}}))  # just a test update
            # print(json.dumps(result.json(), indent=4))
            # print(result)
            # print(str(result))
            # print(str(result) == '<Response [200]>')
            for lineNum, line in enumerate(inFile.readlines()):  # go through each line
                if lineNum > 0:  # skip the first header line as its not relevant
                    try:
                        entry = line.strip().split(DELIMITER_CHAR)  # split each line by the delimiter after stripping off special characters
                        stuNum = int(entry[0])  # get the student number from the first column of the input file
                        stuDCID = dcidDict.get(stuNum, None)  # get the corresponding DCID for the student number, return None if we didnt find that student in PS
                        if stuDCID:  # only try to process student numbers we got DCIDs for
                            try:
                                for i in range(1,len(CUSTOM_FIELD_NAMES)):  # go through each field, skipping over the first column which is student number
                                    print(f'DBUG: Updating student number {stuNum}-{stuDCID} with field {CUSTOM_FIELD_NAMES[i]} and value {entry[i]}')
                                    print(f'DBUG: Updating student number {stuNum}-{stuDCID} with field {CUSTOM_FIELD_NAMES[i]} and value {entry[i]}', file=log)
                                    result = ps.post('ws/v1/student?extensions=u_studentsuserfields', data=json.dumps({'students' : {'student' : [{'@extensions': 'u_studentsuserfields', 'id': str(stuDCID), 'client_uid': str(dcid), 'action': 'UPDATE', '_extension_data': {'_table_extension': [{'name': 'u_studentsuserfields', '_field': [{'name': CUSTOM_FIELD_NAMES[i], 'value': entry[i]}]}]}}]}}))
                                    if str(result) != '<Response [200]>':
                                        print(f'ERROR: {json.dumps(result.json(), indent=2)}')
                                        print(f'ERROR: {json.dumps(result.json(), indent=2)}', file=log)
                                    # response = ps.get(f'ws/v1/student/{stuDCID}?extensions=u_studentsuserfields') # get the student info for the current DCID, with the contact info expansion which contains the student email
                                    # info = response.json().get('student').get('_extension_data').get('_table_extension').get('_field') # get the table extension fields. If there is no data in the fields, its just not returned
                                    # print(type(info))
                                    # print(info)
                                    # print(info, file=log)
                                    # if type(info) is list:
                                    #     for entry in info:
                                    #         fieldName = entry.get('name')
                                    #         if fieldName in CUSTOM_FIELD_NAMES:
                                    #             fieldValue = entry.get('value')
                                    #             print(fieldName)
                                    #             print(fieldValue)
                            except Exception as er:
                                print(er)
                    except Exception as er:
                        print(er)
        endTime = datetime.datetime.now()
        endTime = endTime.strftime('%H:%M:%S')
        print(f'Execution ended at {endTime}')
        print(f'Execution ended at {endTime}', file=log)