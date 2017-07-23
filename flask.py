from flask import Flask, request, jsonify
import json
import s3-functions
import credentials
import pdb

app = Flask(__name__)

if 'VCAP_SERVICES' not in os.environ:
	print 'Running on Local'
	from scripts.credentials import *
	aws_id = AWS_ACCESS_KEY_ID
	aws_key=AWS_SECRET_ACCESS_KEY
else:
	print 'Running on Bluemix'
	aws_id = os.getenv('AWS_ACCESS_KEY_ID')
	aws_key=os.getenv('AWS_SECRET_ACCESS_KEY')
