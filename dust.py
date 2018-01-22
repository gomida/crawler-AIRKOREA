from info import info
from bs4 import BeautifulSoup
import mysql.connector as db
import datetime
import time
import requests
import urllib

tm = time.time()
tz = datetime.timezone(datetime.timedelta(hours=9))
ts = datetime.datetime.fromtimestamp(tm, tz)
ts_str = ts.strftime('%Y-%m-%d %H:00:00')

def airkorea(stationName, cursor):
	# DB QUERY
	cursor.execute("SELECT timestamp FROM air_dust WHERE station LIKE '%s' ORDER BY timestamp DESC LIMIT 24" % (stationName))
	db_rows = cursor.fetchall()

	# DB CHECK	
	if cursor.rowcount > 0:
		for db_row in db_rows:
			if str(db_row[0]) == ts_str:
				print("dup")
				return
		pt = (ts - datetime.timedelta(minutes=80)).replace(tzinfo=None)
		if pt < db_rows[0][0]:
			print("no need to query")
			return
	
	# WEB QUERY
	web_query = "http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty?serviceKey="\
					+ info['serviceKey']\
					+ "&numOfRows=24&pageSize=24&pageNo=1&startPage=1&stationName="\
					+ urllib.parse.quote_plus(stationName)\
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
			if pm10_0 == '-': pm10_0 = -1
			if pm02_5 == '-': pm02_5 = -1
			cursor.execute("INSERT INTO air_dust(timestamp, station, pm10_0, pm02_5) VALUES (%s, %s, %s, %s)", (measure_stamp, stationName, pm10_0, pm02_5))


connection = db.connect(host=info['host'], user=info['user'], password=info['password'], database=info['database'])
cursor = connection.cursor()

for stationName in info['stationName']:
	airkorea(stationName, cursor)

connection.commit()
