import time
import datetime

def write_doc(cloudant_client, data, sport):

	db = cloudant_client['collection_metadata']
	
	date = datetime.datetime.utcnow()

	month = date.month
	day = date.day
	year = date.year
	hour= date.hour
	minute = date.minute
	readable = date.strftime('%m-%d-%y, %H:%M:%S %p')

	timestamp = int(date.strftime("%s")) * 1000
	
	document = {"_id":"new_%s_report_%s" %(sport,timestamp),
				"time_collected":"%s_%s_%s_%s_%s" %(year, month, day, hour, minute),
				"year":year,
				"month":month,
				"day":day,
				"hour":hour,
				"minute":minute,
				"data":data}

	db.create_document(document)

