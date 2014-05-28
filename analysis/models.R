####
# This script loads the RData files created by create_dataset.R and creates some models.
####

# LOAD DATA
load(BYCHANNEL_RDATA)
load(BYSHOW_RDATA)
load(BYCHANNEL_DAILY_RDATA)
load(BYSHOW_DAILY_RDATA)
load(BYCHANNEL_WEEKLY_RDATA)
load(BYSHOW_WEEKLY_RDATA)
load(BYCHANNEL_DAY_OF_WEEK_RDATA)
load(BYSHOW_DAY_OF_WEEK_RDATA)
#load(BYCHANNEL_MONTHLY_RDATA)
#load(BYSHOW_MONTHLY_RDATA)


# PERFORM FACTOR ANALYSIS
source(factor_analysis.R)