# # D118-Versatrans-Sync

A script used to synchronize student information to VersaTrans from PowerSchool. 

## Overview

This purpose of this script is to get student information from PowerSchool and synchronize it to Versatrans. It does so by first constructing a header row that contains only the specified sections, as well as the correct number of outputs for certain sections. It then does an SQL query to find all students not in our graduated students building. Then each student is iterated through and a series of additional queries is performed to get the specific information like accommodations or childcare instructions. The sections of information are "modular" so they can be skipped by setting some flags to false instead of true. The sections that are selected are compiled into the final output string for each student, which is then output to the .csv file. After all students are processed, it connects to the Versatrans SFTP server and places the file in the specified directory.

## Requirements

The following Environment Variables must be set on the machine running the script:

- POWERSCHOOL_READ_USER
- POWERSCHOOL_DB_PASSWORD
- POWERSCHOOL_PROD_DB
- VERSATRANS_SFTP_USERNAME
- VERSATRANS_SFTP_PASSWORD
- VERSATRANS_SFTP_ADDRESS

These are fairly self explanatory, and just relate to the usernames, passwords, and host IP/URLs for PowerSchool, as well as the SFTP login information for VersaTrans. If you wish to directly edit the script and include these credentials or to use other environment variable names, you can.

Additionally, the following Python libraries must be installed on the host machine (links to the installation guide):

- [Python-oracledb](https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html)
- [pysftp](https://pypi.org/project/pysftp/)
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/#installing-beautiful-soup)

**As part of the pysftp connection to the output SFTP server, you must include the server host key in a file** with no extension named "known_hosts" in the same directory as the Python script. You can see [here](https://pysftp.readthedocs.io/en/release_0.2.9/cookbook.html#pysftp-cnopts) for details on how it is used, but the easiest way to include this I have found is to create an SSH connection from a linux machine using the login info and then find the key (the newest entry should be on the bottom) in ~/.ssh/known_hosts and copy and paste that into a new file named "known_hosts" in the script directory.
If the VersaTrans connection also has a private key (as ours does), it should be included in the same directory as private.pem.

## Customization

This script is extremely specific to our district and use case, and therefore will be somewhat difficult to use with other districts, except as a general outline. If you are going to use it, here are many of the things you would likely want to change and why:

 - `OUTPUT_FILENAME` is the name of the file that will be the final output, and should be changed to what you set up with VersaTrans.
 - `SFTP_PATH` is the path on the VersaTrans SFTP server that will be navigated to and have the file place inside
 - To disable or enable the different "modules", change the `DO_...` constants to `False` and `True`
 - For the emergency contacts and authorized adults section, you can change the number of contacts that it outputs by changing the `EMERGENCY_CONTACTS_NUM` and `AUTHORIZED_ADULTS_NUM`
 - You can change the header row constants `..._HEADER` to have different names for the fields. If you need to add or remove fields, be sure to add/remove them from the header and the actual output line.
- As part of the basic info, we have students that are AM or PM only (Pre-Ks and Ks). To find if a student is in AM or PM we look at the course names they are enrolled in, and you can change `AM_COURSES` and `PM_COURSES` to change which courses count as AM or PM respectively. 
- A lot of the info that it pulls from PowerSchool is in custom fields, you will need to change a lot of the fields in the 'SELECT' SQL queries to match the fields that contain this data in a different instance. Specifically, any of the ext.xyz fileds are extension fields.
- We use MBA custom alerts for medical and life threatening alerts, which we are pulling to send to VersaTrans. This is accomplished in the `DO_ACCOMODATIONS` section, and uses the `u_aet_customalerts` table, finding alerts with the `u_aet_customalerts_typesid = 1161` which correlate to the medical alerts. If there is a different alert you want to pull, or multiple alerts, this would need to be changed.
	- As part of the medical alerts, they will often contain HTML to have embedded links in the alert to action plans or other information. The script uses BeautifulSoup to extract the text and the link separately and get rid of the HTML tags that would otherwise be present. It does this by assuming there is only 2 sections to the alert, the basic text info and then the HTML tag. It splits these two parts apart and does the link extraction from the second part. If there is more HTML to your alerts you will need to edit this portion to better process your setup.
- The `DO_EMERGENCY_CONTACTS` section prioritizes certain phone numbers over other if the student has more contacts listed than the `EMERGENCY_CONTACTS_NUM` constant. If you want to change what it prioritizes, you will need to change the if/elif statements in that section. The current system is described below.
	- It prioritizes numbers that are marked as "Mobile" and "Preferred" more than anything else. Then "Preferred" numbers of other types are prioritized. Following that, any "Mobile" number will be prioritized if there are no "Preferred" numbers. Finally, the oldest phone number will be used if none of the entries are marked as "Mobile" or "Preferred".

