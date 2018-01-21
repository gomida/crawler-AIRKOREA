from info import info
from bs4 import BeautifulSoup
import mysql.connector as db
import datetime
import requests
import urllib

def airkorea():
	# DB QUERY
	connection = db.connect(host=info['host'], user=info['user'], password=info['password'], database=info['database'])
	cursor = connection.cursor()
	#cursor.execute("CREATE TABLE air_dust (id timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(), pm10_0 smallint(6) NOT NULL, pm02_5 smallint(6) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;ALTER TABLE air_dust ADD UNIQUE KEY id (id);", multi=True)
	cursor.execute("SELECT * FROM air_dust ORDER BY id DESC LIMIT 100")
	db_rows = cursor.fetchall()

	# WEB QUERY
	web_query = "http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty?serviceKey="\
					+ info['serviceKey']\
					+ "&numOfRows=24&pageSize=24&pageNo=1&startPage=1&stationName="\
					+ urllib.parse.quote_plus(info['stationName'])\
					+ "&dataTerm=DAILY&ver=1.3"
	web_xml = requests.get(web_query).text
	soup = BeautifulSoup(web_xml, 'html.parser')

	# COMPARE BASED ON WEB RESPONSE
	for item in soup.find_all('item'):

		datatime = item.find('datatime').text
		if datatime[11:13] == '24':
			# FIX HOUR 24 TO 0 OF NEXT DAY
			datatime = datatime[:11] + '00' + datatime[13:]
			measure_stamp = datetime.datetime.strptime(datatime, '%Y-%m-%d %H:%M')
			measure_stamp += datetime.timedelta(days=1)
		else:
			measure_stamp = datetime.datetime.strptime(datatime, '%Y-%m-%d %H:%M')

		dup = False
		for db_row in db_rows: 
			if db_row[0] == measure_stamp:
				dup = True
				break

		if dup == False:
			pm10_0 = item.find('pm10value').text
			pm02_5 = item.find('pm25value').text
			cursor.execute("INSERT INTO air_dust(id, pm10_0, pm02_5) VALUES (%s, %s, %s)", (measure_stamp, pm10_0, pm02_5))

	connection.commit()

airkorea()
