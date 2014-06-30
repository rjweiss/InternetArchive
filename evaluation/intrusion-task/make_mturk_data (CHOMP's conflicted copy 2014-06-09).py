'''
python make_mturk_data.py  /Users/rweiss/Dropbox/research/projects/InternetArchive/data/gensim-update1x5k-thresh0.2-subset20k-k30.model-topic-terms.txt 
'''
import os, sys, csv
from random import randint, shuffle

import pprint

def write_to_file(data_as_list, output_file):
	# XXX check to see if output_file already exists
	# check that data is valid
	return None

def process_file(input_file, num_topics):
	master_data = dict()
	csv_reader = csv.reader(open(input_file, 'r'))

	for line in csv_reader:
		master_data[line[0]] = [item.strip() for item in line[1:len(line)]]
	
	data = create_data(master_data, num_topics)
	return master_data

def create_data(master_data_dict, num_topics):
	datum = dict()
	topics = range(0, num_topics)
	shuffle(topics)
	origin_topic = str(topics[0])
	intruder_topic = str(topics[1])

	intruder_index = randint(0, len(intruder_topic))
	intruder_token = master_data_dict[intruder_topic][intruder_index]

	origin_topic_list = master_data_dict[origin_topic][:5]
	origin_topic_list.append(intruder_token)
	shuffle(origin_topic_list)
	print 'words = ' + ', '.join(origin_topic_list)
	print 'intruder token = ' + intruder_token
	print 'intruder index = ' + str(origin_topic_list.index(intruder_token))
	print 'origin topic # = ' + origin_topic
	print 'intruder topic # = ' + intruder_topic
	print 'intruder index in source list = ' + str(intruder_index)
	
	#return data_as_list

def main():
	if (sys.argv < 2):
		sys.exit("not enough arguments")

	cleaned_data = process_file(sys.argv[1], num_topics=30)


#	write_to_file(cleaned_data, sys.argv[2]

if __name__ == "__main__":
	main()
