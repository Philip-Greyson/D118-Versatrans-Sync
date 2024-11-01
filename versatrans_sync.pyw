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

if __name__ == '__main__':  # main file execution
    with open('versatrans_log.txt', 'w') as log:  # open logging file
        startTime = datetime.now()
        print(f'INFO: Execution started at {startTime}')
        print(f'INFO: Execution started at {startTime}', file=log)

        with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:  # create the connecton to the database
            print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}')
            print(f'INFO: Connection successfully established to PowerSchool at {DB_CS} on version {con.version}', file=log)
            with con.cursor() as cur:  # start an entry
                cur.execute('SELECT s.dcid, s.id, s.student_number, s.enroll_status, s.first_name, s.last_name, s.middle_name, s.grade_level, s.schoolid, s.mailing_street, s.mailing_city, s.mailing_state, s.mailing_zip, s.gender, s.dob, s.home_phone FROM students s')
                students = cur.fetchall()
                for student in students:
                    print(student)
        with pysftp.Connection(SFTP_HOST, username=SFTP_UN, password=SFTP_PW, private_key='private.pem', cnopts=CNOPTS) as sftp:  # uses a private key file to authenticate with the server, need to pass the path
            print(f'INFO: SFTP connection to {SFTP_HOST} established successfully')
            print(f'INFO: SFTP connection to {SFTP_HOST} established successfully', file=log)
            # print(sftp.pwd) # debug, show what folder we connected to
            # print(sftp.listdir())  # debug, show what other files/folders are in the current directory
            sftp.chdir(SFTP_PATH)  # change to the specified folder/path
            print(sftp.pwd) # debug, make sure out changedir worked
            print(sftp.listdir())
            # sftp.put(OUTPUT_FILENAME)  # upload the first file onto the sftp server
            print("INFO: Student file placed on remote server")
            print("INFO: Student file placed on remote server", file=log)