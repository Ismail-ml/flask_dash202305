#!/usr/bin/env python
# Ookla
# Updated 5/30/18

# This Python script queries a list of available data extract files from Speedtest Intelligence,
# determines what data sets are available, and then downloads the most recent version of each.
# By default the files are stored in the directory where the script is running, but modifying
# the storageDir variable will allow you to specify a directory.

try: # Python3
    import urllib.request as compatible_urllib
except ImportError: # Python 2
    import urllib2 as compatible_urllib
import json
import os
import base64
import sys
import subprocess

extracts_url = 'https://intelligence.speedtest.net/extracts'

# Please replace MyApiKey and MyApiSecret below with your organization's API key.
username = 'd21468a7-17d2-4616-bc58-18e5094402f0'
password = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.IjQ0Mzk0ZWMwLWE3ZmYtNDMyOS04NDk5LWQzMTZmMjdjYmE2NiI.ovHdsM6GL25gsrqSWiJXEzvDcwxFDbgN8z5mXpdUPqs'

# By default, the script stores the extract files in the directory where the script is running
# To specify a storage directory, change this value to a string represting the directory where
# the files should be stored.
# Example: storageDir = '/data/ookla/extracts'
storageDir = '/mnt/raw_counters/"Corporate Folder"/CTO/SOC/QA/"RAN QA"/Daily/Raw_counters/SpeedTest'#os.getcwd()
str='/home/ismayil/Downloads/speedtest'

opener = compatible_urllib.build_opener()
compatible_urllib.install_opener(opener)
opener.addheaders = [('Accept', 'application/json')]

# setup authentication
login_credentials = '%s:%s' % (username, password)
base64string = base64.b64encode(login_credentials.encode('utf-8')).decode('ascii')
opener.addheaders = [('Authorization', 'Basic %s' % base64string)]

# makes request for files
print(extracts_url)
try:
    response = compatible_urllib.urlopen(extracts_url).read()
except compatible_urllib.HTTPError as error:
    if error.code == 401:
        print("Authentication Error\nPlease verify that the API key and secrete are correct")
    elif error.code == 404:
        print("The account associated with this API key does not have any files attached to it.\nPlease contact your technical account manager to enable data extracts for this account.")
    elif error.code == 500:
        print("Server Error\nPlease contact your technical account manager")
    sys.exit()

try:
    content = json.loads(response)
except ValueError as err:
    print(err)
    sys.exit()

#############################################################
# loop through contents, sort through files and directories
def sort_files_and_directories(contents, files={}):
    for entry in contents:
        if entry['type'] == 'file' and entry['name'].find('headers') == -1 and '_20' in entry['name']:
            filter(entry, files)
        elif entry['type'] == 'dir':
            subdir = extracts_url + entry['url']
            sub_files = json.loads(compatible_urllib.urlopen(subdir).read())
            sort_files_and_directories(sub_files, files)

    return files

# determine if file should be downloaded - check for new datasets and most current file for exisiting datasets
def filter(data_file, files):
    # identify the dataset by the file name prefix
    dataset = data_file['name'][:data_file['name'].index('_20')]
    if dataset not in files or data_file['mtime'] > files[dataset]['age']:
        files[dataset] = {'name': data_file['name'], 'url': data_file['url'], 'age': data_file['mtime']}

def download(files):
    if not files:
        print("No data extract files found. If this is an error, please contact your technical account manager.")
        return

    for data_set, file in files.items():
        try:
            response = compatible_urllib.urlopen(file['url'])
            flocation = str + '/' + file['name']
            print(("Downloading: %s" % (file['name'])))
            with open(flocation, 'wb') as content:
                content.write(response.read())
        except Exception as e:
            print(e)
            continue
#############################################################
files = sort_files_and_directories(content)
#print(files)
download(files)
os.chdir(str)
process=subprocess.Popen('mv * '+storageDir,shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = process.communicate()
