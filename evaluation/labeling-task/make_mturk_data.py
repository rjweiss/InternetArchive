'''
python make_mturk_data.py test.txt 
'''
import os, sys, csv
from random import randint, shuffle
import pprint, argparse

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

def create_datum(topic_dictionary):
	datum = dict()
	topics = range(0, len(topic_dictionary.keys()))
	shuffle(topics)
	origin_topic = str(topics[0])
	words = topic_dictionary[origin_topic][:8]
	shuffle(words)
	origin_topic_word_list = words[:8]

#	for i in range(0, len(origin_topic_word_list)):
#		datum['word{i}'.format(i=i)] = origin_topic_word_list[i]
	datum['words'] = ', '.join(origin_topic_word_list)
	datum['origin_topic'] = origin_topic 

	return datum

def generate_datum(topic_dictionary):
	# XXX Needs exception handling
	data = create_datum(topic_dictionary)	
	yield data

def create_topic_dictionary(input_file):
	topic_dictionary = dict()
	csv_reader = csv.reader(open(input_file, 'r'))

	for line in csv_reader:
		topic_dictionary[line[0]] = [item.strip() for item in line[1:len(line)]]
	
	return topic_dictionary

def process_file(input_file, num_obs, num_instances):
	topic_dictionary = create_topic_dictionary(input_file)
	new_obs = dict()

	for obs in xrange(0, num_obs):
		for i in xrange(1, num_instances + 1):
			datum = generate_datum(topic_dictionary)
			old_obs = datum.next()

			for key in iter(old_obs):
				new_obs['{val}_q{index}'.format(val=key, index=i)] = old_obs[key]
			
		if new_obs:
			write_datum_to_file(new_obs,  'k{num_topics}_{num_obs}hits_{num_instances}Qs_AMT.csv'.format(
				num_topics=len(topic_dictionary.keys()), num_obs=num_obs, num_instances=num_instances))

	return True	

def main(args):
	num_topics = int(args.input.split('-')[4].split('.')[0].strip('k')) # XXX Gross.
	sys.stdout.write('Creating data with {topics} topics, {obs} total observations for {instances} questions per AMT task...\n'.format(
		topics=num_topics, obs=args.num_obs, instances=args.num_instances))

	finished = process_file(args.input, num_obs=args.num_obs, num_instances=args.num_instances)

	if finished:
		sys.exit("Created data.")


#	write_to_file(cleaned_data, sys.argv[2]

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Create labeling task data file.')
	parser.add_argument('--input', 			required=True, 					help='Input file')
	parser.add_argument('--num_obs', 		type=int, 		default=1000,	help='Number of MTurk HITs')
	parser.add_argument('--num_instances', 	type=int, 		default=3, 		help='Number of questions per HIT')
	
	args = parser.parse_args()
	main(args)
