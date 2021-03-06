import boto3
import os
import datetime
import pdb

def get_all_teams(sport):

    teams = {"mlb":[],"nba":[],"nhl":[],"nfl":[]}

    teams['mlb'] = ["Atlanta Braves", "Miami Marlins", "New York Mets", "Philadelphia Phillies", "Washington Nationals", "Chicago Cubs", "Cincinnati Reds", "Houston Astros", "Milwaukee Brewers", "Pittsburgh Pirates", "St. Louis Cardinals", "Arizona Diamondbacks", "Colorado Rockies", "Los Angeles Dodgers", "San Diego Padres", "San Francisco Giants", "Baltimore Orioles", "Boston Red", "New York Yankees", "Tampa Bay Rays", "Toronto Blue Jays", "Chicago White Sox", "Cleveland Indians", "Detroit Tigers", "Kansas City Royals", "Minnesota Twins", "Los Angeles Angels", "Oakland Athletics", "Seattle Mariners", "Texas Rangers"]
    teams['nba'] =["Atlanta Hawks","Boston Celtics","Brooklyn Nets","Charlotte Hornets","Chicago Bulls","Cleveland Cavaliers","Dallas Mavericks","Denver Nuggets","Detroit Pistons","Golden State Warriors","Houston Rockets","Indiana Pacers","LA Clippers","Los Angeles Lakers","Memphis Grizzlies","Miami Heat","Milwaukee Bucks","Minnesota Timberwolves","New Orleans Pelicans","New York Knicks","Oklahoma City Thunder","Orlando Magic","Philadelphia 76ers","Phoenix Suns","Portland Trail Blazers","Sacramento Kings","San Antonio Spurs","Toronto Raptors","Utah Jazz","Washington Wizards"]
    teams['nfl'] = ['Arizona Cardinals', 'Atlanta Falcons', 'Baltimore Ravens', 'Buffalo Bills', 'Carolina Panthers', 'Chicago Bears', 'Cincinnati Bengals', 'Cleveland Browns', 'Dallas Cowboys', 'Denver Broncos', 'Detroit Lions', 'Green Bay Packers', 'Houston Texans', 'Indianapolis Colts', 'Jacksonville Jaguars', 'Kansas City Chiefs', 'Los Angeles Rams', 'Los Angeles Chargers', 'Miami Dolphins', 'Minnesota Vikings', 'New England Patriots', 'New Orleans Saints', 'New York Giants', 'New York Jets', 'Oakland Raiders', 'Philadelphia Eagles', 'Pittsburgh Steelers', 'San Francisco 49ers', 'Seattle Seahawks', 'Tampa Bay Buccaneers', 'Tennessee Titans', 'Washington Redskins']
    teams['nhl'] = ['Anaheim Ducks', 'Arizona Coyotes', 'Boston Bruins', 'Buffalo Sabres', 'Calgary Flames', 'Carolina Hurricanes', 'Chicago Blackhawks', 'Colorado Avalanche', 'Columbus Blue Jackets', 'Dallas Stars', 'Detroit Red Wings', 'Edmonton Oilers', 'Florida Panthers', 'Los Angeles Kings', 'Minnesota Wild', 'Montreal Canadiens', 'Nashville Predators', 'New Jersey Devils', 'New York Islanders', 'New York Rangers', 'Ottawa Senators', 'Philadelphia Flyers', 'Pittsburgh Penguins', 'San Jose Sharks', 'St. Louis Blues', 'Tampa Bay Lightning', 'Toronto Maple Leafs', 'Vancouver Canucks', 'Vegas Golden Knights', 'Washington Capitals', 'Winnipeg Jets', 'Washington Redskins']

    return teams[sport]

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

def get_s3_metadata(s3, sport, season, team):

	s3_resource = boto3.resource('s3')

	bucket = s3_resource.Bucket('kartees-cloud-collection')

	#teams = get_all_teams(sport)

	team_data_size = {}

	team_data_size = {"team":team,
					# "current_time":get_timestamp(),
					# "current_date":get_date_obj(),
				"total_kb_event_inventory":0.0,
				"total_kb_event_metadata":0.0,
				"total_kb_weather":0.0}


	paths = ["event_inventory", "event_metadata","event_metadata_v2","weather"]

	for path in paths:
	    key_prefix = '%s/%s_%s_%s' %(path,sport, season, team.replace(" ","-"))
	    print (key_prefix)

	    for obj in bucket.objects.filter(Prefix=key_prefix):

	    	try:
				key = obj.key
				size_kb = float(obj.size/1000)
				team_data_size["total_kb_%s"%path] += size_kb
	    	except:
				print ("No data for: %s" %key_prefix)

	print(team_data_size)



	return team_data_size