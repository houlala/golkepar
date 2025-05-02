import duckdb
from dotenv import dotenv_values

# Load .env
print("Load .env")
conf = dotenv_values(".env")
DUCKDB_FILE = conf['DUCKDB_FILE']
PARQUET_PATH_BASE = conf['PARQUET_PATH_BASE']

regexpbots = ['googlebot-news', 'googlebot-video', 'mediapartners-google', 'adsBot-google-mobile-apps', 
              'adsBot-google', 'googlebot', 'duckduckbot', 'bingbot', 'applebot', 'wordpress', 'dataprovider', 
              'go-http-client', 'facebookexternalhit', 'whatsapp', 'wp-rocket', 'okhttp', 'wappalyzer', 'apple-pubsub',
              'checkmarknetwork', 'ruby','meta-externalagent', 'node-fetch', 'python', 'qr scanner android', 
              'microsoft office excel', 'klhttpclientcurl', 'threatview', 'owler', 'apidae', 'checker', 'grequests',
              'censysinspect', 'expanse', 'symfony', 'turnitin', 'scrapy', 'postman', 'geedo',
              'chatgpt', 'crawler', '([a-zA-Z0-9]{2,10}bot)', '...bot']
regexpbotsString = '|'.join(regexpbots)

with duckdb.connect(DUCKDB_FILE) as db:
    db.sql('DROP TABLE IF EXISTS logs')
    db.sql('CREATE TABLE logs AS SELECT * FROM "'+ PARQUET_PATH_BASE +'/logs-*.parquet"')

with duckdb.connect(DUCKDB_FILE) as db:
    db.sql('DROP TABLE IF EXISTS ip_marked_as_admin')
    db.sql('DROP TABLE IF EXISTS ip_that_got_bad_status')
    db.sql('DROP TABLE IF EXISTS ips_user_agent_identified_like_bot')
    db.sql('DROP TABLE IF EXISTS ips_real_navigator')
    db.sql('DROP TABLE IF EXISTS visits')
    db.sql(f"""CREATE TABLE ip_marked_as_admin AS
    SELECT DISTINCT ip 
    FROM logs
    WHERE regexp_matches(path,'^/(wp-admin|admin/|user$|node/.*/edit$)')""")
    db.sql(f"""CREATE TABLE ip_that_got_bad_status AS
    SELECT DISTINCT ip 
    FROM logs
    WHERE status IN (403,405,406,412,417,423,428)""")
    db.sql(f"""CREATE TABLE ips_user_agent_identified_like_bot AS
    SELECT DISTINCT ip
    FROM logs
    WHERE regexp_matches(LOWER(user_agent), '{regexpbotsString}')""")
    db.sql(f"""CREATE TABLE ips_real_navigator AS
    SELECT DISTINCT ip
    FROM logs
    WHERE regexp_matches(path, '\\.css$')
    AND status = 200""")
    db.sql(f"""CREATE TABLE visits AS
    SELECT *,
        IF(ip IN (SELECT ip FROM ips_real_navigator), 'real_browser', 'not_a_browser') ip_browser_status,
        IF(ip IN (SELECT ip FROM ips_user_agent_identified_like_bot), 'bot_agent', 'not_bot_agent') is_bot_status,
        IF(ip IN (SELECT ip FROM ip_marked_as_admin), 'ip_went_into_admin_area', 'not_ip_went_into_admin_area') ip_admin_status,
        IF(ip IN (SELECT ip FROM ip_that_got_bad_status), 'ip_bad_status', 'not_ip_bad_status') ip_reputation_status,
        regexp_extract(LOWER(user_agent), '{regexpbotsString}') agent
    FROM logs
    WHERE /* status IN (200,201) /* uniquement les requêtes réussies */ AND */
    NOT regexp_matches(path, '^/wp-') /* Ne pas compter les assets */
    AND NOT regexp_matches(path, '\\....?.?$') /* Ne pas compter les assets */""")

print("done")

with duckdb.connect(DUCKDB_FILE) as db:
    db.sql('SELECT COUNT(1) FROM logs').show()

#with duckdb.connect(DUCKDB_FILE) as db:
#    db.sql('SELECT * FROM logs LIMIT 10').show()
