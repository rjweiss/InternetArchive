'''
This is a very simple script that creates a dictionary and VSM of data for gensim.
Very rudimentary.  Expects a file with a text document per line.
Author: Rebecca Weiss
TODO: Pretty janky...
'''
import os, logging, sys
import gensim
from gensim import corpora, models, utils
from gensim.parsing.preprocessing import STOPWORDS as gensim_stopwords
#from nltk.corpus import stopwords as nltk_stopwords

class Corpus(object):

	def __init__(self, datafile):
		self.datafile = datafile
		#self.STOPWORDS = frozenset(gensim_stopwords.union(nltk_stopwords.words('english')))
		self.STOPWORDS = gensim_stopwords	
		self.dictionary = corpora.Dictionary(line.lower().split() for line in utils.smart_open(self.datafile))
		stop_ids = [self.dictionary.token2id[stopword] for stopword in self.STOPWORDS if stopword in self.dictionary.token2id]
		once_ids = [tokenid for tokenid, docfreq in self.dictionary.dfs.iteritems() if docfreq == 1]
		self.dictionary.filter_extremes( no_below=5, no_above=0.5, keep_n=1000000) # Tweak this.
		self.dictionary.filter_tokens(stop_ids + once_ids)
		self.dictionary.compactify()

	def __iter__(self):
		# TODO: Modify this to read from MongoDB?
		for line in utils.smart_open(self.datafile):			
			yield self.dictionary.doc2bow(line.lower().split())

def main(training_datafile, output_path):	
	logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

	corpus = Corpus(datafile=training_datafile)
	filename = ''.join(training_datafile.split('/')[-1])

	if not os.path.exists(os.path.join(output_path)):
		os.makedirs(output_path)

	corpora.MmCorpus.serialize(output_path + '/{filename}.mm'.format(filename=filename), corpus)
	corpus.dictionary.save(output_path + '/{filename}.dict'.format(filename=filename))


if __name__ == '__main__':
	if sys.argv < 3:
		sys.exit('Provide path to training data file (1) and output path (2)')
		
	filename_path = sys.argv[1]
	output_path = sys.argv[2]

	try:
		os.path.isfile(filename_path) and os.access(filename_path, os.R_OK)
		main(training_datafile = sys.argv[1], output_path = sys.argv[2])
	except IOError as e:
		sys.exit('({})'.format(e))
