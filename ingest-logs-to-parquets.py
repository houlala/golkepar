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

# From http://detectmobilebrowsers.com/ Python example. A METTRE À JOUR RÉGULIÈREMENT !
reg_user_agent_mobile_a = re.compile(r"(android|bb\\d+|meego).+mobile|avantgo|bada\\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od)|iris|kindle|lge |maemo|midp|mmp|mobile.+firefox|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\\.(browser|link)|vodafone|wap|windows ce|xda|xiino", re.I|re.M)
reg_user_agent_mobile_b = re.compile(r"1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\\-(n|u)|c55\\/|capi|ccwa|cdm\\-|cell|chtm|cldc|cmd\\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\\-s|devi|dica|dmob|do(c|p)o|ds(12|\\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\\-|_)|g1 u|g560|gene|gf\\-5|g\\-mo|go(\\.w|od)|gr(ad|un)|haie|hcit|hd\\-(m|p|t)|hei\\-|hi(pt|ta)|hp( i|ip)|hs\\-c|ht(c(\\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\\-(20|go|ma)|i230|iac( |\\-|\\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\\/)|klon|kpt |kwc\\-|kyo(c|k)|le(no|xi)|lg( g|\\/(k|l|u)|50|54|\\-[a-w])|libw|lynx|m1\\-w|m3ga|m50\\/|ma(te|ui|xo)|mc(01|21|ca)|m\\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\\-2|po(ck|rt|se)|prox|psio|pt\\-g|qa\\-a|qc(07|12|21|32|60|\\-[2-7]|i\\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\\-|oo|p\\-)|sdk\\/|se(c(\\-|0|1)|47|mc|nd|ri)|sgh\\-|shar|sie(\\-|m)|sk\\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\\-|v\\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\\-|tdg\\-|tel(i|m)|tim\\-|t\\-mo|to(pl|sh)|ts(70|m\\-|m3|m5)|tx\\-9|up(\\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\\-|your|zeto|zte\\-", re.I|re.M)

class DetectMobileBrowser():
    def process_request(self, request):
        request.mobile = False
        if request.META.has_key('HTTP_USER_AGENT'):
            user_agent = request.META['HTTP_USER_AGENT']
            b = reg_b.search(user_agent)
            v = reg_v.search(user_agent[0:4])
            if b or v:
                return HttpResponseRedirect("http://detectmobilebrowser.com/mobile")
            
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
                                    user_agent = d.headers_in['User-Agent']

                                    # Add minimalistic date stamp column
                                    # d.date = d.request_time.date().isoformat()
                                    d.status = d.final_status
                                    d.isAsset = bool(re.search(r'\.[a-zA-Z0-9]{2,3}$', d.request_uri, re.IGNORECASE)) and not bool(re.search(r'\.(php\d?|env|xml)$', d.request_uri, re.IGNORECASE))
                                    d.country, d.city = get_city_info(d.remote_host)
                                    d.asn = get_asn_info(d.remote_host)
                                    d.isMobile = user_agent is not None and (reg_user_agent_mobile_a.search(user_agent) or reg_user_agent_mobile_b.search(user_agent[0:4]))

                                    # Calculate user_key as MD5 hash of ip and user_agent
                                    user_key = hashlib.md5(f"{d.remote_host}{user_agent}".encode()).hexdigest()

                                    row = (user_key, d.remote_host, d.remote_logname.lower(), d.remote_user, d.status,
                                           d.request_method, d.request_uri, d.request_time.isoformat(),
                                           d.request_query, d.request_protocol, user_agent,
                                           d.bytes_sent, d.headers_in['Referer'], d.isAsset, d.country, d.city, d.asn, d.isMobile)
                                    rows.append(row)

                            print(f"Parsed {log_file} in {(time.time() - file_start_time):.2f} seconds")

                    # Export the rows to a Parquet file
                    df = pd.DataFrame(rows, columns=[
                        'user_key', 'ip', 'host', 'user', 'status', 'method', 'path', 'datetime',
                        'query', 'protocol', 'user_agent', 'length', 'referer', 'is_asset',
                        'country', 'city', 'asn', 'is_mobile'
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
                        'is_asset': 'bool',
                        'country': 'string',
                        'city': 'string',
                        'asn': 'string',
                        'is_mobile': 'bool'
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
