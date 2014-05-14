'''
Script to create a trained LDA model
Author: Rebecca Weiss
TODO: Needs a lot of work; setting various model parameters and hyperparameters from command line
'''
import os, logging, sys, csv
import gensim
from gensim import corpora, models

def load_data(dict_path, corpus_path):
	dictionary = corpora.Dictionary.load(dict_path)
	corpus = corpora.MmCorpus(corpus_path)
	return dictionary, corpus

def create_model(dictionary, corpus, logger, model_path, num_topics):
	# assume corpus is a bag of words for now...need to try against tfidf
	model = models.LdaModel(corpus, id2word=dictionary, alpha='auto', eval_every=10, chunksize=5000, num_topics=num_topics)
	logger.info('Saving model to disk: {}'.format(model_path))
	model.save(model_path)
	return True

def get_topic_mixtures(model, corpus, model_path, num_topics):
	lda_corpus = model[corpus]
	outfile = open(model_path + '-doc-topics.txt', 'wb')
	writer = csv.DictWriter(outfile, delimiter=',', fieldnames = [i for i in xrange(num_topics)])
	docs = (doc for doc in lda_corpus)
	for doc in docs:
		row = dict.fromkeys([i for i in xrange(num_topics)])
		for tup in doc:
			row[tup[0]] = tup[1]
		for k, v in row.iteritems():
			if v is None:
				row[k] = 0
		writer.writerow(row)
	outfile.close()

def main(dict_path, corpus_path, model_path, num_topics):
	logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
	logger = logging.getLogger('Archive.LDA')
	dictionary, corpus = load_data(dict_path, corpus_path)
	logger.info('Initiate training')
	model_created = create_model(dictionary, corpus, logger, model_path, num_topics)
	#logger.info('Saving model to disk: {}'.format(model_path))
	#model.save(model_path)
	get_topic_mixtures(model, corpus, model_path, num_topics)

if __name__ == '__main__':
	if sys.argv < 5:
		sys.exit('Provide path to training data file (1) and dictionary path (2), model output path (3), number of topics (4)')

	dict_path = os.path.join(sys.argv[1])
	corp_path = os.path.join(sys.argv[2])
	model_path = os.path.join(sys.argv[3])
	num_topics = int(sys.argv[4])

	try:
		os.path.isfile(dict_path) and os.access(dict_path, os.R_OK)	
	except IOError as e:
		sys.exit('({})'.format(e))

	try:
		os.path.isfile(corp_path) and os.access(corp_path, os.R_OK)	
	except IOError as e:
		sys.exit('({})'.format(e))

	main(dict_path = dict_path, corpus_path = corp_path, model_path = model_path, num_topics = num_topics)


