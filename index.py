from flask import Flask, request, jsonify
import json
from functions.s3_functions import *
from functions.teams import *
import os
import pdb
import datetime

app = Flask(__name__)

if 'VCAP_SERVICES' not in os.environ:
	print 'Running on Local'
	from functions.credentials import *
	aws_id = AWS_ACCESS_KEY_ID
	aws_key=AWS_SECRET_ACCESS_KEY
else:
	print 'Running on Bluemix'
	aws_id = os.getenv('AWS_ACCESS_KEY_ID')
	aws_key=os.getenv('AWS_SECRET_ACCESS_KEY')


@app.route('/s3')
def s3():

	sport = str(request.args.get('sport'))
	team = str(request.args.get('team'))
	season = str(request.args.get('season'))
	
	return jsonify(get_s3_metadata(sport,season, team))

@app.route('/get-all-teams')
def get_team():

	sport = request.args.get('sport')
	return jsonify(get_all_teams(sport))

@app.route('/')
def home():
    return "Hey there this is kartees-s3-monitoring python web server running on Bluemix. Time - %s" %datetime.datetime.now()



port = os.getenv('PORT', '5000')
if __name__ == "__main__":



	app.run(host='0.0.0.0', port=int(port), debug=True)

