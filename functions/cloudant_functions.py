from datetime import datetime
import time

def write_doc(cloudant_client, data, sport):

	db = cloudant_client['collection_metadata']
	
	timestamp = time.time()
	
	document = {"_id":"%s_report_%s" %(sport,timestamp),"data":data}

	db.create_document(document)

