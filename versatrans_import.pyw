"""Script to pull information from Versatrans/Kiteworks and import it into custom fields in PowerSchool.

https://github.com/Philip-Greyson/D118-Versatrans-Sync

Needs oracledb: pip install oracledb --upgrade
Needs pysftp: pip install pysftp --upgrade
finally needs the ACME powerschool library downloaded from https://easyregpro.com/acme.php
"""

#  importing modules
import datetime  # used to get current date and time
import json  # needed to manipulate the json objects we pass and receive from the API
import os  # needed to get environement variables
import re

import acme_powerschool  # a library to interact with the PowerSchool REST API. Found and documented here: https://easyregpro.com/acme.php
import oracledb  # used to connect to PowerSchool database
import pysftp

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

CUSTOM_FIELD_NAMES = ['custom_pickupbus', 'custom_pickupdescription', 'custom_pickuptime', 'custom_returnbus', 'custom_returndescription', 'custom_returntime']  # what the field names are in powerschool that correlate to the fields from the file
CUSTOM_TABLE_NAME = 'u_studentsuserfields'  # the table name the custom fields above are in

INPUT_FILE_NAME = 'routeImport.csv'  # what name the file will be pulled down via SFTP as
DELIMITER_CHAR = ','  # character used for delimiting the fields. comma for .csv traditionally

print(f'DBUG: DB Username: {DB_UN} | DB Password: {DB_PW} | DB Server: {DB_CS}')  # debug so we can see where oracle is trying to connect to/with
print(f'DBUG: SFTP Username: {SFTP_UN} | SFTP Password: {SFTP_PW} | SFTP Server: {SFTP_HOST}')  # debug so we can see what info sftp connection is using

def ps_update_custom_field(table: str, field: str, dcid: int, value: any) -> str:
    """Function to do the update of a custom field in a student extension table, so that the large json does not need to be used every time an update is needed elsewhere."""
    # print(f'DBUG: table {table}, field {field}, student DCID {dcid}, value {value}')
    try:
        data = {
            'students' : {
                'student': [{
                    '@extensions': table,
                    'id' : str(dcid),
                    'client_uid' : str(dcid),
                    'action' : 'UPDATE',
                    '_extension_data': {
                        '_table_extension': [{
                            'name': table,
                            '_field': [{
                                'name': field,
                                'value': value
                            }]
                        }]
                    }
                }]
            }
        }
        result = ps.post(f'ws/v1/student?extensions={table}', data=json.dumps(data))
        status_code = result.json().get('results').get('result').get('status')
    except Exception as er:
        print(f'ERROR while trying to update custom field {field} in table {table} for student DCID {dcid}: {er}')
        print(f'ERROR while trying to update custom field {field} in table {table} for student DCID {dcid}: {er}')
        return 'ERROR'
    if status_code != 'SUCCESS':
        print(f"ERROR: Could not update field {field}  in table {table} for student DCID {dcid}, status {result.json().get('results').get('result')}")
        print(f"ERROR: Could not update field {field}  in table {table} for student DCID {dcid}, status {result.json().get('results').get('result')}", file=log)
    else:
        print(f'DBUG: Successfully updated field {field} in table {table} for student DCID {dcid} to {value}')
        print(f'DBUG: Successfully updated field {field} in table {table} for student DCID {dcid} to {value}', file=log)
    return status_code

if __name__ == '__main__':  # main file execution
    with open('VtImportLog.txt', 'w') as log:
        startTime = datetime.datetime.now()
        startTime = startTime.strftime('%H:%M:%S')
        print(f'Execution started at {startTime}')
        print(f'Execution started at {startTime}', file=log)
        studentDict = {}  # create empty dictionary for the student info which will have student number to dcid mapping as well as current values of custom fields
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
        with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:  # create the connecton to the database
            with con.cursor() as cur:  # start an entry cursor
                # Setup our "modular" query of custom fields
                customFieldQuery = ''  # string that will hold the custom parts of our query
                for field in CUSTOM_FIELD_NAMES:
                    customFieldQuery = f'{customFieldQuery} {CUSTOM_TABLE_NAME}.{field},'  # append the table.field name to the query
                customFieldQuery = customFieldQuery[0:len(customFieldQuery) - 1]  # remove the final comma character
                print(f'DBUG: Custom query: {customFieldQuery}')
                print(f'DBUG: Custom query: {customFieldQuery}', file=log)

                # get the student dcid, number, and our custom fields from PS. Assumes the custom table does a 1:1 match of students by DCID, otherwise it will need to be changed
                cur.execute(f'SELECT students.dcid, students.student_number, {customFieldQuery} FROM students LEFT JOIN {CUSTOM_TABLE_NAME} ON students.dcid = {CUSTOM_TABLE_NAME}.studentsdcid ORDER BY student_number DESC')

                columns = [col.name.lower() for col in cur.description]  # get the column names as lowercase strings
                cur.rowfactory = lambda *args: dict(zip(columns, args))  # create a dictionary out of the column names and arguments from the results. See https://python-oracledb.readthedocs.io/en/stable/user_guide/sql_execution.html#rowfactories
                students = cur.fetchall()
                print('DBUG: Getting student number to DCID mappings')
                print('DBUG: Getting student number to DCID mappings', file=log)
                for studentDataDict in students:
                    # print(studentDataDict)  # debug
                    stuNum = studentDataDict.get('student_number')
                    studentDict.update({stuNum: studentDataDict})  # append the students id number as the key with a value of the data dict that contains the SQL results

        with open(INPUT_FILE_NAME) as inFile:
            ps = acme_powerschool.api('d118-powerschool.info', client_id=D118_API_ID, client_secret=D118_API_SECRET)  # create ps object via the API to do requests on
            for lineNum, line in enumerate(inFile.readlines()):  # go through each line
                if lineNum > 0:  # skip the first header line as its not relevant
                    try:
                        entry = line.strip().split(DELIMITER_CHAR)  # split each line by the delimiter after stripping off special characters
                        # print(entry)  # debug
                        if re.match(r'^\d+$',entry[0]):  # check to see if our first column is only numeric (which it should be as its student numbers)
                            stuNum = int(entry[0])  # get the student number from the first column of the input file
                            stuDCID = studentDict.get(stuNum, {}).get('dcid', None)  # get the corresponding DCID for the student number, return None if we didnt find that student in PS
                            # print(f'DBUG: Processing student {stuNum}-{stuDCID} and looking for any fields that need to be updated')
                            if stuDCID:  # only try to process student numbers we got DCIDs for
                                try:
                                    for i in range(0,len(CUSTOM_FIELD_NAMES)):  # go through each field
                                        currentValue = studentDict.get(stuNum).get(CUSTOM_FIELD_NAMES[i])  # get the current value of the field in PS from the studentDict
                                        currentValue = '' if currentValue is None else currentValue  # if what we got back from PS is the literal None, we want to interpret that as a blank since thats what the csv parsing will give
                                        newValue = entry[i+1].strip()  # get the new value that it should be from the file, but note that the file includes the student number as the first column so we need to add 1 to our iterable
                                        if currentValue != newValue:
                                            print(f'INFO: Updating student number {stuNum}-DCID {stuDCID} field {CUSTOM_FIELD_NAMES[i]} from "{currentValue}" to new value "{newValue}"')
                                            print(f'INFO: Updating student number {stuNum}-DCID {stuDCID} field {CUSTOM_FIELD_NAMES[i]} from "{currentValue}" to new value "{newValue}"', file=log)
                                            ps_update_custom_field(CUSTOM_TABLE_NAME, CUSTOM_FIELD_NAMES[i], stuDCID, newValue)  # call the helper function to update the field via API
                                        # else:
                                        #     print(f'DBUG: No change needed for field {CUSTOM_FIELD_NAMES[i]} for {stuNum} | Current: "{currentValue}" - New: "{newValue}"')
                                except Exception as er:
                                    print(f'ERROR while processing student {stuNum} and field {CUSTOM_FIELD_NAMES[i]}: {er}')
                                    print(f'ERROR while processing student {stuNum} and field {CUSTOM_FIELD_NAMES[i]}: {er}', file=log)
                        else:
                            print(f'WARN: Found a non-numeric entry where the student number should be: {entry[0]}')
                            print(f'WARN: Found a non-numeric entry where the student number should be: {entry[0]}', file=log)
                    except Exception as er:
                        print(f'ERROR while doing the general processing of the entries on line {lineNum}: {er}')
                        print(f'ERROR while doing the general processing of the entries on line {lineNum}: {er}')
        endTime = datetime.datetime.now()
        endTime = endTime.strftime('%H:%M:%S')
        print(f'Execution ended at {endTime}')
        print(f'Execution ended at {endTime}', file=log)
