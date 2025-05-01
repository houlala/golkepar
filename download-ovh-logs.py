import os.path
import requests
import datetime
from dotenv import dotenv_values

# Load .env
print("Read .env")
conf = dotenv_values(".env")
LOG_PATH_BASE = conf['LOG_PATH_BASE']
OVH_STATS_ACCOUNT_NAME = conf['OVH_STATS_ACCOUNT_NAME']
OVH_STATS_CLUSTER = conf['OVH_STATS_CLUSTER']

def downloadAndSave(fileurl, localfilepath, force = False):
        if(os.path.isfile(localfilepath) is False or force):
                print("Download "+ fileurl)
                response = requests.get(fileurl, auth=(conf['OVH_STATS_USER'], conf['OVH_STATS_PASSWORD']))
                if response.status_code == 200:
                        os.makedirs(os.path.dirname(localfilepath), exist_ok=True)
                        with open(localfilepath, mode="wb") as file:
                                file.write(response.content)
                else:
                        print(f'Error {response.status_code}')

base = datetime.datetime.today()
date_list = [base - datetime.timedelta(days=x) for x in range(365)]
date_list.pop(0)

for day in date_list:
        subdir = "logs-"+ day.strftime("%m-%Y") 
        filename = OVH_STATS_ACCOUNT_NAME +"-"+ day.strftime("%d-%m-%Y") +".log"
        localSubdir = day.strftime("%Y/%m")
        localfilename = "apache-"+ day.strftime("%Y-%m-%d") +".log"

        filepath = subdir +"/"+ filename
        fileurl = "https://logs."+ OVH_STATS_CLUSTER +".hosting.ovh.net/"+ OVH_STATS_ACCOUNT_NAME +"/logs/"+ filepath +".gz"
        localfilepath = LOG_PATH_BASE +"/access/"+ localSubdir +"/"+ localfilename
        downloadAndSave(fileurl, localfilepath)

        errorFilePath = subdir +"/error/"+ filename
        errorFileurl = "https://logs."+ OVH_STATS_CLUSTER +".hosting.ovh.net/"+ OVH_STATS_ACCOUNT_NAME +"/logs/"+ errorFilePath +".gz"
        localErrorFilePath = LOG_PATH_BASE +"/error/"+ localSubdir +"/"+ localfilename
        downloadAndSave(errorFileurl, localErrorFilePath)

print("#########################################")
print("GET TODAY LOGS")
date_live_list = [base - datetime.timedelta(days=x) for x in range(2)]

for day in date_live_list:
        filename = OVH_STATS_ACCOUNT_NAME +"-"+ day.strftime("%d-%m-%Y") +".log"
        fileurl = "https://logs."+ OVH_STATS_CLUSTER +".hosting.ovh.net/"+ OVH_STATS_ACCOUNT_NAME +"/osl/"+ filename
        localfilepath = LOG_PATH_BASE +"/today/"+ filename
        downloadAndSave(fileurl, localfilepath, force=True)
