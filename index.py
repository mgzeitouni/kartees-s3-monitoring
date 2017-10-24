from flask import Flask, request, jsonify
import json
from functions.s3_functions import *
from functions.teams import *
import os
import pdb
# import datetime
import boto3
from flask_apscheduler import APScheduler
import threading
from functions.cloudant_functions import *
from cloudant.client import Cloudant

app = Flask(__name__)


global teams
teams = {"mlb":[],"nba":[],"nhl":[]}

teams['mlb'] = ["Atlanta Braves", "Miami Marlins", "New York Mets", "Philadelphia Phillies", "Washington Nationals", "Chicago Cubs", "Cincinnati Reds", "Houston Astros", "Milwaukee Brewers", "Pittsburgh Pirates", "St. Louis Cardinals", "Arizona Diamondbacks", "Colorado Rockies", "Los Angeles Dodgers", "San Diego Padres", "San Francisco Giants", "Baltimore Orioles", "Boston Red", "New York Yankees", "Tampa Bay Rays", "Toronto Blue Jays", "Chicago White Sox", "Cleveland Indians", "Detroit Tigers", "Kansas City Royals", "Minnesota Twins", "Los Angeles Angels", "Oakland Athletics", "Seattle Mariners", "Texas Rangers"]
teams['nba'] =["Atlanta Hawks","Boston Celtics","Brooklyn Nets","Charlotte Hornets","Chicago Bulls","Cleveland Cavaliers","Dallas Mavericks","Denver Nuggets","Detroit Pistons","Golden State Warriors","Houston Rockets","Indiana Pacers","LA Clippers","Los Angeles Lakers","Memphis Grizzlies","Miami Heat","Milwaukee Bucks","Minnesota Timberwolves","New Orleans Pelicans","New York Knicks","Oklahoma City Thunder","Orlando Magic","Philadelphia 76ers","Phoenix Suns","Portland Trail Blazers","Sacramento Kings","San Antonio Spurs","Toronto Raptors","Utah Jazz","Washington Wizards"]
#teams['nfl'] = ['Arizona Cardinals', 'Atlanta Falcons', 'Baltimore Ravens', 'Buffalo Bills', 'Carolina Panthers', 'Chicago Bears', 'Cincinnati Bengals', 'Cleveland Browns', 'Dallas Cowboys', 'Denver Broncos', 'Detroit Lions', 'Green Bay Packers', 'Houston Texans', 'Indianapolis Colts', 'Jacksonville Jaguars', 'Kansas City Chiefs', 'Los Angeles Rams', 'Los Angeles Chargers', 'Miami Dolphins', 'Minnesota Vikings', 'New England Patriots', 'New Orleans Saints', 'New York Giants', 'New York Jets', 'Oakland Raiders', 'Philadelphia Eagles', 'Pittsburgh Steelers', 'San Francisco 49ers', 'Seattle Seahawks', 'Tampa Bay Buccaneers', 'Tennessee Titans', 'Washington Redskins']
teams['nhl'] = ['Anaheim Ducks', 'Arizona Coyotes', 'Boston Bruins', 'Buffalo Sabres', 'Calgary Flames', 'Carolina Hurricanes', 'Chicago Blackhawks', 'Colorado Avalanche', 'Columbus Blue Jackets', 'Dallas Stars', 'Detroit Red Wings', 'Edmonton Oilers', 'Florida Panthers', 'Los Angeles Kings', 'Minnesota Wild', 'Montreal Canadiens', 'Nashville Predators', 'New Jersey Devils', 'New York Islanders', 'New York Rangers', 'Ottawa Senators', 'Philadelphia Flyers', 'Pittsburgh Penguins', 'San Jose Sharks', 'St. Louis Blues', 'Tampa Bay Lightning', 'Toronto Maple Leafs', 'Vancouver Canucks', 'Vegas Golden Knights', 'Washington Capitals', 'Winnipeg Jets']

if 'VCAP_SERVICES' not in os.environ:
	trigger = {
	'type': 'cron',
	'day_of_week': '*',
	'hour': '3',
	'minute': '15'}

else:
	trigger = eval(os.getenv('trigger'))

@app.route('/get-sizes')
def s3():

	s3 = boto3.client('s3', aws_access_key_id=aws_id,
    aws_secret_access_key=aws_key)

	sport = str(request.args.get('sport'))
	team = str(request.args.get('team'))
	season = str(request.args.get('season'))
	
	return jsonify(get_s3_metadata(s3,sport,season, team))

@app.route('/get-all-teams')
def get_team():

	sport = request.args.get('sport')
	return jsonify(get_all_teams(sport))

@app.route('/')
def home():
    return "Hey there this is kartees-s3-monitoring python web server running on Bluemix. Time - %s" %datetime.datetime.now()

@app.route('/section-to-category')
def section_category():

	section =int(request.args.get('section'))

	section_map = {181979:0,
					181981:1,
					182157:2}

	return str(section_map[section])
 	

def worker(num, s3_client):

	season = 2017

	num_to_sport = {0:'mlb', 1:'nba', 2:'nhl'}
	
	sport = num_to_sport[num]

	all_teams = teams[sport]
	
	size_data = []

	for team in all_teams:
		
 		size_data.append(get_s3_metadata(s3_client,sport,season, team))


 	if 'VCAP_SERVICES' not in os.environ:
		from functions.credentials import *
		aws_id = AWS_ACCESS_KEY_ID
		aws_key=AWS_SECRET_ACCESS_KEY
		from functions.credentials import *
		cloudant_client = Cloudant(CLOUDANT['username'], CLOUDANT['password'], url=CLOUDANT['url'],connect=True,auto_renew=True)



	else:

		aws_id = os.getenv('AWS_ACCESS_KEY_ID')
		aws_key=os.getenv('AWS_SECRET_ACCESS_KEY')
		

		vcap = json.loads(os.getenv('VCAP_SERVICES'))
		
		if 'cloudantNoSQLDB' in vcap:
			creds = vcap['cloudantNoSQLDB'][0]['credentials']
			user = creds['username']
			password = creds['password']
			url = 'https://' + creds['host']
			cloudant_client = Cloudant(user, password, url=url, connect=True)


	write_doc(cloudant_client,size_data, sport)


def monitor_sizes():
	print ('Cron running-------------------')
	s3 = boto3.client('s3', aws_access_key_id=aws_id,aws_secret_access_key=aws_key)

	for i in range(3):
		t = threading.Thread(target=worker, args=(i,s3))
		t.start()



class Config(object):

	JOBS = [{'id': 'consolidate totals',
    'func': monitor_sizes,
     'trigger': trigger}]

	SCHEDULER_API_ENABLED = True

  

port = os.getenv('PORT', '5000')
if __name__ == "__main__":


	app.config.from_object(Config())

	scheduler = APScheduler()
	scheduler.init_app(app)
	scheduler.start()
	app.run(host='0.0.0.0', port=int(port), debug=True, use_reloader=False)

