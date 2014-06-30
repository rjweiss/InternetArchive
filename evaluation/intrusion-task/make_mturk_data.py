'''
This script generates the datafile required by the Amazon Mechanical Turk intrusion word LDA model verification task.
Author: Rebecca Weiss
'''
import os, sys, csv, argparse
from random import randint, shuffle

import pprint

def write_datum_to_file(datum, output_file):
	# check that data is valid
	# XXX Needs exception handling

	if not os.path.exists('upload'):
		os.makedirs('upload')

	output_path = os.path.join('upload', output_file)

	if os.path.isfile(output_path):
		with open(output_path, 'a') as outfile:		
			csv_writer = csv.DictWriter(outfile, fieldnames = datum.keys())
			csv_writer.writerow(datum)		
	else:
		with open(output_path, 'w') as outfile:
			csv_writer = csv.DictWriter(outfile, fieldnames = datum.keys())
			csv_writer.writeheader()
			csv_writer.writerow(datum)

def generate_datum(topic_dictionary):
	# XXX Needs exception handling
	data = create_datum(topic_dictionary)
	yield data

def process_file(input_file, num_obs, num_instances):
	topic_dictionary = create_topic_dictionary(input_file)
	new_obs = dict()

	for obs in xrange(0, num_obs):
		for i in xrange(0, num_instances):
			datum = generate_datum(topic_dictionary)
			old_obs = datum.next()
			for key in iter(old_obs):
				new_obs['{val}_q{index}'.format(val=key, index=i+1)] = old_obs[key]

		write_datum_to_file(new_obs, 'k{num_topics}_{num_obs}hits_{num_instances}Qs_AMT.csv'.format(
			num_topics=len(topic_dictionary.keys()), num_obs=num_obs, num_instances=num_instances))

	return True
	
def create_topic_dictionary(input_file):
	topic_dictionary = dict()
	csv_reader = csv.reader(open(input_file, 'r'))

	for line in csv_reader:
		topic_dictionary[line[0]] = [item.strip() for item in line[1:len(line)]]
	
	return topic_dictionary

def create_datum(topic_dictionary):
	datum = dict()
	topics = range(0, len(topic_dictionary.keys()))
	shuffle(topics)
	origin_topic = str(topics[0])
	intruder_topic = str(topics[1])
	intruder_index = randint(0, 19)
	intruder_token = topic_dictionary[intruder_topic][intruder_index]
	origin_topic_list = topic_dictionary[origin_topic][:5]
	intruder_topic_list = topic_dictionary[intruder_topic]
	origin_topic_list.append(intruder_token)
	shuffle(origin_topic_list)

	datum = {
		"origin_topic": origin_topic,
		"intruder_topic": intruder_topic,
		"intruder_token": intruder_token,
		"intruder_task_index": origin_topic_list.index(intruder_token),
		"intruder_source_index": intruder_index
	}

	for word in origin_topic_list:
		datum['word{i}'.format(i=origin_topic_list.index(word))] = word

	return datum
	
def main(args):
	# XXX Convert to logfile
	num_topics = int(args.input.split('-')[4].split('.')[0].strip('k')) # XXX This is gross.
	sys.stdout.write('Creating data with {topics} topics, {obs} total observations for {instances} questions per AMT task...\n'.format(
		topics=num_topics, obs=args.num_obs, instances=args.num_instances))

	finished = process_file(args.input, num_obs=args.num_obs, num_instances=args.num_instances)

	if finished:
		sys.exit("Created data.")


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='Create intrusion task data file.')
	parser.add_argument('--input', 			required=True, 					help='Input file')
	parser.add_argument('--num_obs', 		type=int, 		default=1000, 	help='Number of MTurk HITs')
	parser.add_argument('--num_instances', 	type=int, 		default=5, 		help='Number of questions per HIT')
	
	args = parser.parse_args()
	main(args)
