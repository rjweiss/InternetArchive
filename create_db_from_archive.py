import os, sys, re, csv, argparse
from datetime import datetime
from pymongo import MongoClient
from pymongo import errors

def MongoConnect(db_name, collection_name):
    try: 
        connection = MongoClient()
        database = connection[db_name]
        collection = database[collection_name]
        return (connection, database, collection)
    except pymongo.errors.OperationFailure:
        return False        

class InternetArchiver(object):

	#def __init__(self):
	# connect to mongo here?

	def extract(self, infile, mongo_collection):	

		processed = False		

		try:
			mongo_document = self._process_cc(infile)
			processed = True
		except IOError as e:
			sys.stderr.write('IOError: ' + str(e))

		if processed:
			mongo_collection.save(mongo_document)
			# try:
			# 	mongo_collection.save(mongo_document)
			# except pymongo.errors.OperationFailure:
			# 	processed = False

		return processed

	def _process_cc(self, infile):
		'''
		This function takes an Internet Archive cc input file and extracts metadata and a flattened string of cc text
		'''			
		
		try:
			with open(infile, 'rb') as ccfile:
				ccdata = ccfile.readlines()
		except IOError as e:
			sys.stderr.write('IOError: ' + str(e))
		
		metadata = self._get_metadata(infile)

		return {
			'filename' : infile,
			'channel' : metadata['channel'],
			'date' : metadata['date'].isoformat(),
			'time' : metadata['time'].isoformat(),
			'show': metadata['show'],
			'cc_text' : self._get_cc_data(ccdata)
		}

	def _get_metadata(self, filename_string):
		'''
		This helper function takes in a filename string from the IArchive and returns metadata (Channel, Date, Show)
		'''	
		file_pattern = re.compile(r"rawdata\/([A-Z0-9]+)_(\d+)_(\d+)_(.+)\.cc5\.txt$")


		match = re.search(file_pattern, filename_string)
		date = datetime.strptime(match.group(2), "%Y%m%d") #date
		time = datetime.strptime(match.group(3), "%H%M%S") #time, NOT ACCURATE?

		return {
			'channel': match.group(1),
			'date' : date.date(),
			'time': time.time(),
			'date': date,
			'time': time, #TODO returns from 1900...need to make it just return time?
			'show' : str(match.group(4))		
		}

	def _get_cc_data(self, ccdata_list):
		'''
		This helper function takes in the list of closed caption data and returns a single flattened string
		'''
		
		cc_list = [self._get_caption_text(line.lower()) for line in ccdata_list]

		return ''.join(cc_list)

	def _get_caption_text(self, inputline):
		'''
		This helper function splits a line in the cc text into timestamp and string
		It returns the string value, minus the timestamp
		'''	

		cc_pattern = re.compile(r"^\[(\d+):(\d+):(\d+);(\d+)\][>]*([\w|/.|/|/,|\"| ']+)")
		match = re.search(cc_pattern, inputline)

		try:
			cc_line = match.group(5)
		except AttributeError as e:
			sys.stderr.write('regex processing error: +' + inputline + str(e))
		
		return cc_line.replace('"', '``')

def main(data_dir, mdb, mc):

	sys.stdout.write('Connecting to Mongo...')
	sys.stdout.flush()

	connection, db, collection = MongoConnect(mdb, mc)

	if connection:
		sys.stdout.write('success.\n')
	else:
		sys.stdout.write('failed.\n')
		sys.exit()

	arc = InternetArchiver()

	if not os.path.exists(data_dir):
		sys.exit('ERROR: Directory does not exist.')
	
	for f in os.listdir(data_dir):
		if f.endswith('.cc5.txt'):
			current_file = os.path.join(data_dir, f)

			try:
				document = collection.find_one({"filename": current_file})
			except AttributeError as e: # TODO: This is the wrong error.
				sys.stdout("Error, exiting.\n")
				sys.exit()

			if document:
				print("File has already been processed.")
				pass
			else:
				valid = arc.extract(current_file, collection)
				if valid:
					print 'Processing ' + f
				else:
					sys.exit()
					                        
	print 'Finished processing request.'


if __name__ == "__main__":
	
	parser = argparse.ArgumentParser(description='Process Internet Archive files')
	parser.add_argument('-d','--datadir', help='data directory', default='data/', required=True)
	parser.add_argument('-mdb','--mongodb', help='mongo database', default='test', required=False)
	parser.add_argument('-mc','--mongocoll', help='mongo collection', default='test', required=False)
	
	args = vars(parser.parse_args())

	if args['mongodb'] and args['mongocoll']:
		main(args['datadir'], args['mongodb'], args['mongocoll'])
	else:
		main(args['datadir'], mongodb=None, mongocoll=None)

