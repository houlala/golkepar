#%pip install apachelogs pandas numpy maxminddb

import os
import time
import pandas as pd
import pyarrow
import numpy as np
import re
import hashlib
from apachelogs import LogParser
import maxminddb
from dotenv import dotenv_values

# Load .env
print("Load .env")
conf = dotenv_values(".env")
LOG_PATH_BASE = conf['LOG_PATH_BASE']
LOG_FORMAT =  conf['LOG_FORMAT']
IP_PATH_BASE = conf['IP_PATH_BASE']
PARQUET_PATH_BASE = conf['PARQUET_PATH_BASE']

# Load the MMDB files
city_ipv4_reader = maxminddb.open_database(IP_PATH_BASE +'/geolite2-city-ipv4.mmdb')
city_ipv6_reader = maxminddb.open_database(IP_PATH_BASE +'/geolite2-city-ipv6.mmdb')
asn_ipv4_reader  = maxminddb.open_database(IP_PATH_BASE +'/geolite2-asn-ipv4.mmdb')
asn_ipv6_reader  = maxminddb.open_database(IP_PATH_BASE +'/geolite2-asn-ipv6.mmdb')

def get_city_info(ip):
    try:
        if ':' in ip:
            data = city_ipv6_reader.get(ip)
        else:
            data = city_ipv4_reader.get(ip)
        country_code = data['country_code']
        city = data['state1'] +'/'+ data['city']
        return country_code, city
    except:
        return None, None

def get_asn_info(ip):
    try:
        if ':' in ip:
            data = asn_ipv6_reader.get(ip)
        else:
            data = asn_ipv4_reader.get(ip)
        asn = 'AS' +str(data['autonomous_system_number']) +' - '+ data['autonomous_system_organization']
        return asn
    except:
        return None

# country_code, city = get_city_info('34.201.168.78')
# asn = get_asn_info('34.201.168.78')
# print(city, country_code, asn)

# Directory containing log files
log_directory = LOG_PATH_BASE +'/access/'

# Ensure the parquet directory exists
os.makedirs(PARQUET_PATH_BASE, exist_ok=True)

# Pattern below is from the LogFormat setting in apache2.conf/httpd.conf file
# You will likely need to change this value to the pattern your system uses
# parser = LogParser("%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"")
parser = LogParser(LOG_FORMAT)

# List to collect all rows
rows = []

# Recursively walk through the directory tree to collect year directories
for root, dirs, files in os.walk(log_directory):
    for dir_name in dirs:
        year_directory = os.path.join(root, dir_name)
        if dir_name.isdigit() and len(dir_name) == 4:  # Assuming year directories are 4-digit numbers
            year = dir_name
            year_start_time = time.time()

            # List files inside the month directory and sort them alphabetically
            month_names = sorted([f for f in os.listdir(year_directory)], reverse=True)

            # List month directories within the year directory
            for month_name in month_names:
                month_directory = os.path.join(year_directory, month_name)
                if os.path.isdir(month_directory) and month_name.isdigit() and len(month_name) == 2:  # Assuming month directories are 2-digit numbers
                    month = month_name
                    month_start_time = time.time()
                    rows = []

                    # Define the Parquet file name
                    parquet_file = os.path.join(PARQUET_PATH_BASE, f'logs-{year}-{month}.parquet')

                    # Skip parsing if the Parquet file already exists and is newer than all log files
                    if os.path.exists(parquet_file):
                        parquet_creation_time = os.path.getctime(parquet_file)
                        log_files = sorted([f for f in os.listdir(month_directory) if f.endswith(".log")])
                        newest_log_time = max(os.path.getctime(os.path.join(month_directory, f)) for f in log_files)

                        if parquet_creation_time > newest_log_time:
                            print(f"Skipping month {year}-{month} as {parquet_file} is newer than all log files")
                            continue

                    # List files inside the month directory and sort them alphabetically
                    log_files = sorted([f for f in os.listdir(month_directory) if f.endswith(".log")])

                    # List files inside the month directory
                    for filename in log_files:
                        if filename.endswith(".log"):
                            log_file = os.path.join(month_directory, filename)
                            file_start_time = time.time()

                            with open(log_file) as f:
                                for line in f:
                                    d = parser.parse(line)

                                    # Add minimalistic date stamp column
                                    # d.date = d.request_time.date().isoformat()
                                    d.status = d.final_status
                                    d.isAsset = bool(re.search(r'\.[a-zA-Z0-9]{2,3}$', d.request_uri, re.IGNORECASE)) and not bool(re.search(r'\.(php\d?|env|xml)$', d.request_uri, re.IGNORECASE))
                                    d.country, d.city = get_city_info(d.remote_host)
                                    d.asn = get_asn_info(d.remote_host)

                                    # Calculate user_key as MD5 hash of ip and user_agent
                                    user_key = hashlib.md5(f"{d.remote_host}{d.headers_in['User-Agent']}".encode()).hexdigest()

                                    row = (user_key, d.remote_host, d.remote_logname.lower(), d.remote_user, d.status,
                                           d.request_method, d.request_uri, d.request_time.isoformat(),
                                           d.request_query, d.request_protocol, d.headers_in['User-Agent'],
                                           d.bytes_sent, d.headers_in['Referer'], d.isAsset, d.country, d.city, d.asn)
                                    rows.append(row)

                            print(f"Parsed {log_file} in {(time.time() - file_start_time):.2f} seconds")

                    # Export the rows to a Parquet file
                    df = pd.DataFrame(rows, columns=[
                        'user_key', 'ip', 'host', 'user', 'status', 'method', 'path', 'datetime',
                        'query', 'protocol', 'user_agent', 'length', 'referer', 'isasset',
                        'country', 'city', 'asn'
                    ])
                    convert_dict = {
                        'user_key': 'string',
                        'ip': 'string',
                        'host': 'string',
                        'user': 'string',
                        'status': 'int',
                        'method': 'string',
                        'path': 'string',
                        'datetime': 'datetime64[ns, Europe/Paris]',
                        'query': 'string',
                        'protocol': 'string',
                        'user_agent': 'string',
                        'length': 'int',
                        'referer': 'string',
                        'isasset': 'bool',
                        'country': 'string',
                        'city': 'string',
                        'asn': 'string'
                    }
                    # Met des 0 aux valeurs nulles
                    # cf https://medium.com/@felipecaballero/deciphering-the-cryptic-futurewarning-for-fillna-in-pandas-2-01deb4e411a1
                    with pd.option_context('future.no_silent_downcasting', True):
                        df = df.fillna(0)
                    df = df.astype(convert_dict)
                    df.to_parquet(parquet_file, index=False)
                    print(f"Inserted data for month {year}-{month} in {(time.time() - month_start_time):.2f} seconds")

            print(f"Processed year {year} in {(time.time() - year_start_time):.2f} seconds")

    # Break after processing the top-level directories
    break
