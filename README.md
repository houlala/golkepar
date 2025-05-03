# Golkepar - OVH Logs Analyzer

A Python-based toolchain for processing and analyzing OVH web hosting logs. This project transforms raw Apache logs into an optimized analytics-ready format using modern data processing technologies.

## Overview

This project provides a complete pipeline to:
1. Download and update GeoIP databases
2. Fetch Apache logs from OVH hosting
3. Process logs into optimized Parquet files with enriched GeoIP data
4. Create an analytics-ready DuckDB database

The final DuckDB database can be directly used with analytics tools like Apache Superset for creating dashboards and visualizations.

## Technologies Used

- **Python**: Core scripting language for all processing steps
- **Parquet**: Columnar storage format for efficient data storage and querying
- **DuckDB**: Embedded analytical database that provides SQL capabilities on top of Parquet files
- **MaxMind GeoIP**: For IP geolocation and ASN information
- **Apache Superset**: For creating interactive dashboards and visualizations

## Scripts

### 1. `download-and-update-geo-ip.py`
Downloads and updates MaxMind GeoIP databases (GeoLite2) for IP geolocation and ASN information. These databases are used to enrich log data with geographical and network information.

### 2. `download-ovh-logs.py`
Fetches Apache access and error logs from OVH Web Hosting. Downloads logs for the past year and keeps today's logs up to date.

### 3. `ingest-logs-to-parquets.py`
Processes raw Apache logs into optimized Parquet files. This script:
- Parses Apache log format
- Enriches data with GeoIP information
- Give a user_key to each visitor (IP + User Agent)
- Saves data in efficient Parquet format

### 4. `web-stats-duckdb.py`
Creates an analytics-ready DuckDB database by:
- Loading Parquet files into DuckDB
- Creating optimized tables for analysis
- Identifying different types of visitors (bots, real users, admin access)
- Preparing data for visualization

## Getting Started

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file with your configuration (or copy the `.env.example` to `.env`):
```
IP_PATH_BASE=/path/to/geoip/files
LOG_PATH_BASE=/path/to/logs
PARQUET_PATH_BASE=/path/to/parquet/files
DUCKDB_FILE=/path/to/database.duckdb
OVH_STATS_ACCOUNT_NAME=your_account
OVH_STATS_CLUSTER=your_cluster
OVH_STATS_USER=your_user
OVH_STATS_PASSWORD=your_password
LOG_FORMAT=your_apache_log_format
```

3. Run the scripts in order:
```bash
python download-and-update-geo-ip.py
python download-ovh-logs.py
python ingest-logs-to-parquets.py
python web-stats-duckdb.py
```

## Analytics with Apache Superset

The generated DuckDB database can be directly connected to Apache Superset for creating interactive dashboards. The database includes pre-processed tables for:
- Visitor analysis
- Bot detection
- Geographic distribution
- Access patterns
- Security monitoring

## License

GNU GENERAL PUBLIC LICENSE
Version 3, 29 June 2007

Copyright (C) 2024 Florent Ou

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
