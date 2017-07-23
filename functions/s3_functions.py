import boto3
import os
import datetime
import pdb
import smart_open
from teams import *
from credentials import *

s3 = boto3.client('s3')


def get_timestamp():

	return int(datetime.datetime.utcnow().strftime("%s")) * 1000

def get_date_obj():

	date = datetime.datetime.utcnow()
	month = date.month
	day = date.day
	year = date.year
	readable = date.strftime('%m-%d-%y, %H:%M:%S %p')
	date_object = dict({"date_object":date, "month":month, "year":year, "day":day, "readable":readable})

	return date_object

def get_s3_metadata(sport, season, team):

	s3_resource = boto3.resource('s3')

	bucket = s3_resource.Bucket('kartees-cloud-collection')

	#teams = get_all_teams(sport)

	team_data_size = {}

	team_data_size = {"team":team,
					"current_time":get_timestamp(),
					"current_date":get_date_obj(),
				"total_kb_event_inventory":0.0,
				"total_kb_event_metadata":0.0,
				"total_kb_weather":0.0,
				"success":True
			}

	try:

		key_prefix = '%s/%s/%s' %(sport, season, team.replace(" ","-"))

		for obj in bucket.objects.filter(Prefix=key_prefix):

			key = obj.key
			size_kb = float(obj.size/1000)
			# print "%s - %s" %(key, size_kb)
			# pdb.set_trace()

			event = key.split("/")[3]
			event_data_type = key.split("/")[4]

			if event_data_type in ['event_inventory', 'event_metadata', 'weather']:

				team_data_size["total_kb_%s"%event_data_type] += size_kb

	except:

		print "Error with %s" %team
		team_data_size['success']=False
		
		# print "%s - %s" %(team, team_data_size)

	return team_data_size