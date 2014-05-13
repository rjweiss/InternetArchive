'''
This is a very simple script that creates a dictionary and VSM of data for gensim.
Very rudimentary.  Expects a file with a text document per line.
Author: Rebecca Weiss
TODO: Pretty janky...
'''
import os, logging, sys
import gensim
from gensim import corpora, models, utils
from gensim.corpora.dictionary import Dictionary
from gensim.corpora.mmcorpus import MmCorpus 
from gensim.parsing.preprocessing import preprocess_string, strip_punctuation, strip_multiple_whitespaces, strip_numeric, remove_stopwords, strip_short, STOPWORDS
#from nltk.corpus import stopwords as nltk_stopwords

class ArchiveCorpus(corpora.TextCorpus):

	def __init__(self, datafile, preprocess=[], dictionary=None):
		self.datafile = datafile
		self.preprocess = preprocess
		self.metadata = None

		if dictionary:
				self.dictionary = dictionary
		else:
				self.dictionary = Dictionary()
				if datafile is not None:
					self.dictionary.add_documents(self.get_texts())
					self.dictionary.filter_extremes(no_below=5, no_above=0.5, keep_n=500000)


	def get_texts(self):
		with utils.smart_open(self.datafile) as inputfile:
			for line in inputfile:
				for f in self.preprocess:
					line = f(line)
				text = list(utils.tokenize(line, deacc=True, lowercase=True))
				yield text

def main(training_datafile, output_path):	
	logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
	logger = logging.getLogger('Archive.gensim')
	filters = [strip_punctuation, strip_multiple_whitespaces, strip_numeric, remove_stopwords, strip_short]
	logger.info('Creating Corpus object...')
	corpus = ArchiveCorpus(datafile=training_datafile, preprocess=filters)
	filename = ''.join(training_datafile.split('/')[-1])

	if not os.path.exists(output_path):
		os.makedirs(output_path)

	outfile_path = os.path.join(output_path, filename)
	logger.info('Saving corpus to disk: {}.mm'.format(filename))
	MmCorpus.serialize('{}.mm'.format(outfile_path), corpus, progress_cnt=1000)
	logger.info('Saving dictionary to disk: {}.dict'.format(filename))
	corpus.dictionary.save('{}.dict'.format(outfile_path))

if __name__ == '__main__':

	if sys.argv < 3:
		sys.exit('Provide path to training data file (1) and output path (2)')
		
	fn_path = os.path.join(sys.argv[1])
	out_path = os.path.join(sys.argv[2])

	try:
		os.path.isfile(fn_path) and os.access(fn_path, os.R_OK)
		main(training_datafile = fn_path, output_path = out_path) 
	except IOError as e:
		sys.exit('({})'.format(e))
