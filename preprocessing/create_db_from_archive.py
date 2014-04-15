#!/usr/bin/python
'''
Description: This script processes a directory of Internet Archive-formatted closed captioning files, extracts metadata, constructs a flattened text string of a entire program, and inserts into a MongoDB collection.
Author: Rebecca Weiss
'''
import os, sys, re, csv, argparse, multiprocessing, hashlib
from datetime           import datetime
from pymongo            import MongoClient
from pymongo            import errors
from multiprocessing    import Process, JoinableQueue
from hashlib            import md5
import logging

def MongoConnect(db_name, collection_name):
	try: 
		connection = MongoClient()
		database = connection[db_name]
		collection = database[collection_name]
		index_exists = index_check(collection)
		if index_exists:
			return (connection, database, collection)
		else:
			return False
	except pymongo.errors.OperationFailure:
		return False        

def index_check(collection):
	collection.ensure_index("md5", unique=True)
	indices = collection.index_information()
	if indices['md5_1']:
		return True
	else:
		return False

def list_files(data_dir):
	for f in os.listdir(data_dir):
		pathname = os.path.join(data_dir, f)
		yield pathname

def files_to_process(data_dir, cc_type):
    #filecount = 0
	for f in list_files(data_dir):
		if f.endswith(cc_type):
			yield f
    # TODO: Return filecount?

def extract_and_insert(current_filename, mongo_collection, archiver, process_logger):
	m = hashlib.md5()
	m.update(current_filename)
	try:
		cursor = mongo_collection.find({"md5": m.hexdigest()}).limit(1)
	except AttributeError as e: # TODO: This is the wrong error.
		process_logger.error('Error ({error}), exiting.'.format(error=e))
		sys.exit()

	document_exists = cursor.count()

	if document_exists > 0:
		process_logger.debug('{fname} has already been processed'.format(fname=current_filename))
		return
	else:
		extracted_document = archiver.extract(current_filename, mongo_collection)
		shortened_filename = ''.join(current_filename.split('/')[-1])

		if extracted_document:
			process_logger.debug('Processed {filename}'.format(filename=shortened_filename))
		else:
			process_logger.error('Extraction failure, skipping {filename}'.format(filename=shortened_filename))
		return    
    
def extract_and_insert_process(queue, mongo_collection, process_logger, extractor_logger):
	arc = InternetArchiver(extractor_logger=extractor_logger)
	while True:
		current_file = queue.get()
		if current_file is None:
			break
		extract_and_insert(current_file, mongo_collection, arc, process_logger)
		queue.task_done()        
	queue.task_done()
    
class InternetArchiver(object):

	def __init__(self, extractor_logger):
		self.logger = extractor_logger

    # Main extraction function
	def extract(self, infile, mongo_collection):
		processed = False
		m = hashlib.md5()

		try:
			mongo_document = self._process_cc(infile)
			processed = True
		except AttributeError as e:
			self.logger('Processing error {file}'.format(file=infile))

		if processed:
			m.update(mongo_document['filename'])
			mongo_document['md5'] = m.hexdigest()
			mongo_collection.save(mongo_document)
            # TODO Fix this to catch errors
			# try:
			# 	mongo_collection.save(mongo_document)
			# except pymongo.errors.OperationFailure:
			# 	processed = False
		return processed
        
	# This function takes an Internet Archive cc input file and extracts metadata and a flattened string of cc text
	def _process_cc(self, infile):
		try:
			with open(infile, 'rb') as ccfile:
				ccdata = ccfile.readlines()
		except IOError as e:
			self.logger.error('IOError: {error}'.format(error=e))
		
		metadata = self._get_metadata(infile)

		return {
			'filename':    infile,
			'channel':     metadata['channel'],
			'date':        metadata['date'].isoformat(),
			'time':        metadata['time'].isoformat(),
			'show':        metadata['show'],
			'cc_text':     self._get_cc_data(ccdata)
		}
        
	# Takes in a filename string from the IArchive and returns metadata (Channel, Date, Show)
	def _get_metadata(self, filename_string):
		file_pattern = re.compile(r"rawdata\/([A-Z0-9]+)_(\d+)_(\d+)_(.+)\.cc5\.txt$")
		match = re.search(file_pattern, filename_string)
		date = datetime.strptime(match.group(2), "%Y%m%d") #date
		time = datetime.strptime(match.group(3), "%H%M%S") #time, NOT ACCURATE?

		return {
			'channel':  match.group(1),
			'date':     date.date(),
			'time':     time.time(),
			'date':     date,
			'time':     time,
			'show':     str(match.group(4))		
		}

	# Takes in the list of closed caption data and returns a single flattened string
	def _get_cc_data(self, ccdata_list):
		return ''.join(self._get_caption_text(line.lower()) for line in ccdata_list)

	# Splits a line in the cc text into timestamp and string, returns the string value, minus the timestamp
	def _get_caption_text(self, inputline):
		cc_pattern = re.compile(r"^\[(\d+):(\d+):(\d+);(\d+)\][>]*([\w|/.|/|/,|\"| ']+)")
		match = re.search(cc_pattern, inputline)

		try:
			cc_line = match.group(5).replace('"', '``')
		except AttributeError as e:
			self.logger.error('regex processing error: {inputline} ({error})'.format(inputline=inputline, error=e))
#			sys.stderr.write('regex processing error: +' + inputline + str(e))
		
		return cc_line

def main(data_dir, mdb, mc, logfile):
	logging.basicConfig(level 		= logging.DEBUG,
	                	format 		= '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
	                	datefmt 	= '%m-%d %H:%M',
	                	filename 	= logfile,
	                	filemode 	= 'w')
	
	console = logging.StreamHandler()
	console.setLevel(logging.INFO)
	formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
	console.setFormatter(formatter)
	logging.getLogger('').addHandler(console)
	archiver_process_logger = logging.getLogger('archiver.process')
	archiver_extractor_logger = logging.getLogger('archiver.extractor')

	logging.info('Connecting to Mongo database {mdb}, collection {mc}...'.format(mdb=mdb, mc=mc))
	connection, db, collection = MongoConnect(mdb, mc)

	if connection:
		logging.info('Successfully connected to Mongo.')
	else:
		logging.info('Failed to connect to Mongo.')
		sys.exit()

	if not os.path.exists(data_dir):
		logging.error('Data directory does not exist.')
		sys.exit()
	
	num_procs = multiprocessing.cpu_count()
	queue = JoinableQueue()
	files = files_to_process(data_dir, '.cc5.txt')
	#sys.stdout.write("{filecount} files to process".format(filecount=len(files)))
    
	for i in range(0, num_procs):
		logging.info('Creating process {num}'.format(num=i + 1))
		p = Process(target=extract_and_insert_process, args=(queue, collection, archiver_process_logger, archiver_extractor_logger))
		p.start()
            
	for f in files:
		queue.put(f)        
        
	for i in range(0, num_procs):
		queue.put(None)
		queue.join()

	logging.info('Processing complete.')

if __name__ == "__main__":	
	parser = argparse.ArgumentParser(description='Process Internet Archive files')
	parser.add_argument('-d',   '--datadir',    help='data directory',      default='data/',    	required=True)
	parser.add_argument('-mdb', '--mongodb',    help='mongo database',      default='test',     	required=False)
	parser.add_argument('-mc',  '--mongocoll',  help='mongo collection',    default='test',     	required=False)
	parser.add_argument('-log', '--logfile',	help='logfile path', 		default='archiver.log', required=True)
	args = vars(parser.parse_args())
    
	if args['mongodb'] and args['mongocoll']:
		main(data_dir=args['datadir'], mdb=args['mongodb'], mc=args['mongocoll'], logfile=args['logfile'])
	else:
		main(data_dir=args['datadir'], mdb=None, mc=None, logfile=args['logfile'])

