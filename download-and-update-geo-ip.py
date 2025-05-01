import os.path
import requests
import time
from dotenv import dotenv_values

TIME_TO_EXPIRE_GEOLITE_FILES_DAYS = 7

# Load .env
print("Load .env")
conf = dotenv_values(".env")

IP_PATH_BASE = conf['IP_PATH_BASE']

def downloadAndSave(fileurl, localfilepath, force = False):
        if(not os.path.isfile(localfilepath) 
           or force 
           or (time.time() - os.path.getmtime(localfilepath) > TIME_TO_EXPIRE_GEOLITE_FILES_DAYS * 24 * 60 * 60)):
                print("Download "+ fileurl)
                response = requests.get(fileurl)
                if response.status_code == 200:
                        os.makedirs(os.path.dirname(localfilepath), exist_ok=True)
                        with open(localfilepath, mode="wb") as file:
                                file.write(response.content)
                else:
                        print(f'Error {response.status_code}')
        else:
                print(fileurl +" already exists and under "+ str(TIME_TO_EXPIRE_GEOLITE_FILES_DAYS) +" days old.")

city_ipv4_url = 'https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-city-mmdb/geolite2-city-ipv4.mmdb'
city_ipv6_url = 'https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-city-mmdb/geolite2-city-ipv6.mmdb'
asn_ipv4_url  = 'https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-asn-mmdb/geolite2-asn-ipv4.mmdb'
asn_ipv6_url  = 'https://cdn.jsdelivr.net/npm/@ip-location-db/geolite2-asn-mmdb/geolite2-asn-ipv6.mmdb'

downloadAndSave(city_ipv4_url, IP_PATH_BASE +'/geolite2-city-ipv4.mmdb')
downloadAndSave(city_ipv6_url, IP_PATH_BASE +'/geolite2-city-ipv6.mmdb')
downloadAndSave(asn_ipv4_url , IP_PATH_BASE +'/geolite2-asn-ipv4.mmdb')
downloadAndSave(asn_ipv6_url , IP_PATH_BASE +'/geolite2-asn-ipv6.mmdb')
