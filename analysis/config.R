# This is the main configuration file for the analysis of the Internet Archive data
# It defines all the global variables for the analysis and configures paths for input and output files
# There are also variables that require setting for establishing which model will be run.
# Author: Rebecca Weiss

# Clear workspace
rm(list=ls(all=T))
gc()

# Define global constants
COVERAGE_START    = 20110511 # so that we get Colbert and Jon Stewart
COVERAGE_END      = 20140325
NUM_TOPICS        = 40

WORKING_DIR       = "/Users/rweiss/Dropbox/research/projects/InternetArchive/"
OUTPUT_DIR        = paste(WORKING_DIR, "output/", sep='')
DATA_DIR          = paste(WORKING_DIR, "data/", sep='')
TOPICS_DATA_DIR   = paste(WORKING_DIR, "data/", NUM_TOPICS, 'topics/', sep='')
TOPICS_OUTPUT_DIR = paste(WORKING_DIR, "output/", NUM_TOPICS, 'topics/', sep='')

DOC_TOPICS_FILE   = paste(DATA_DIR, 'gensim-update1x5k-thresh0.2-subset20k-k40.model-doc-topics.txt', sep='')
LABELS_FILE       = paste(DATA_DIR, 'labels40.csv', sep='')
METADATA_FILE     = paste(DATA_DIR, 'subset_meta.csv', sep='')

BYCHANNEL_RDATA             = paste(TOPICS_DATA_DIR, 'bychannel.RData', sep='')
BYSHOW_RDATA                = paste(TOPICS_DATA_DIR, 'byshow.RData', sep='')
BYCHANNEL_DAILY_RDATA       = paste(TOPICS_DATA_DIR, 'bychannel_daily.RData', sep='')
BYSHOW_DAILY_RDATA          = paste(TOPICS_DATA_DIR, 'byshow_daily.RData', sep='')
BYCHANNEL_WEEKLY_RDATA      = paste(TOPICS_DATA_DIR, 'bychannel_weekly.RData', sep='')
BYSHOW_WEEKLY_RDATA         = paste(TOPICS_DATA_DIR, 'byshow_weekly.RData', sep='')
BYCHANNEL_MONTHLY_RDATA     = paste(TOPICS_DATA_DIR, 'bychannel_monthly.RData', sep='')
BYSHOW_MONTHLY_RDATA        = paste(TOPICS_DATA_DIR, 'byshow_monthly.RData', sep='')
BYCHANNEL_DAY_OF_WEEK_RDATA = paste(TOPICS_DATA_DIR, 'bychannel_wday.RData', sep='')
BYSHOW_DAY_OF_WEEK_RDATA    = paste(TOPICS_DATA_DIR, 'byshow_wday.RData', sep='')

# Initialize project and load required libraries
setwd(WORKING_DIR)
dir.create(file.path(OUTPUT_DIR), showWarnings = FALSE)
dir.create(file.path(TOPICS_OUTPUT_DIR), showWarnings = FALSE)
dir.create(file.path(TOPICS_DATA_DIR), showWarnings = FALSE)
packages = c('dplyr','plyr','ggplot2','lubridate', 'reshape2', 'stringr')
sapply(packages, require, character.only=T)
MEDIA_NAMES = read.csv(paste(WORKING_DIR, "docs/source_data.csv", sep='')) # XXX This should be taken care of in config.R
rm(packages)