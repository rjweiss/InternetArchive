####
# This script loads the files created by the preprocessing python scripts and creates datasets suitable for analysis
# 1. Load in raw data indicated as follows:
#     - DOC_TOPICS_FILE = Document-Topic probability mixtures, nrows = documents, ncol = number of topics
#     - LABELS_FILE     = Labels per topic number, created through manual inspection (topic labels chosen to correspond to News Coverage Index)
#     - METADATA_FILE   = Contains document ID, channel, show, date, and (eventually) time data 
# 2. Create .RData files for each time interval
# Author: Rebecca Weiss

# Read in data
raw_data = read.csv(DOC_TOPICS_FILE, header=T)
raw_data = raw_data[-1,] # XXX Remove the first document, because it's actually a mistake
labels = read.csv(LABELS_FILE, header=T)
meta = read.csv(METADATA_FILE, header=T)
meta$date = ymd_hms(as.character(meta$date), tz='UTC')
names(raw_data) = labels$label # XXX This is sloppy
full_data = cbind(meta, raw_data)

# What are the coverage dates for each channel and show?
coverage_dates_channel = full_data %.% 
  dplyr::group_by(channel) %.%
  dplyr::summarise(start = min(date), end = max(date))

coverage_dates_shows = full_data %.% 
  dplyr::group_by(show) %.%
  dplyr::summarise(start = min(date), end = max(date))

# Filter out the channels/shows that don't have complete coverage over the time period COVERAGE_START - COVERAGE_END
# If the earliest broadcast is after COVERAGE_START or if the latest broadcast is before COVERAGE_END, don't include the channel/show
start = ymd(COVERAGE_START)
end = ymd(COVERAGE_END)
valid_dates = start %--% end
valid_shows = filter(coverage_dates_shows, !(start %within% valid_dates))
valid_shows = filter(valid_shows, end %within% valid_dates)
filtered_data = filter(full_data, show %in% valid_shows$show)

# Filter down to news shows in News Coverage Index
# XXX This is pretty not great
show_keys = c('ABC', 'NBC', 'CBS', 'PBS', 'Fox', 'OReilly', 'Maddow', 'Stewart', 'Colbert', 'Hannity')
shows = as.character(factor(unique(filtered_data$show)))
valid_shownames = NULL
for (show in show_keys) {
  valid_shownames = c(valid_shownames, shows[str_detect(shows, show)])
}

filtered_data = filter(filtered_data, show %in% valid_shownames)

# Make sure the levels now reflect the filtered data
filtered_data$show = factor(filtered_data$show)
filtered_data$channel = factor(filtered_data$channel)


# DAILY DATA
melted_data_daily = melt(filtered_data, id.vars=c('X_id', 'channel', 'date', 'show'))
melted_data_daily$value = as.numeric(melted_data_daily$value)
save(melted_data_daily, file=DAILY_RDATA)

# AGGREGATE DATA
melted_data_bychannel = melted_data_daily %.% dplyr::group_by(channel, variable) %.% dplyr::summarise(mean = mean(value, na.rm = T))
melted_data_byshow = melted_data_daily %.% dplyr::group_by(show, variable) %.% dplyr::summarise(mean = mean(value, na.rm = T))
save(melted_data_bychannel, file=BYCHANNEL_RDATA)
save(melted_data_byshow, file=BYSHOW_RDATA)

# WEEKLY DATA
filtered_data$week = round((filtered_data$date - min(filtered_data$date)) / eweeks(1))
melted_data_weekly = melt(filtered_data, id.vars=c('X_id', 'channel', 'week', 'show'))
melted_data_weekly = filter(melted_data_weekly, variable == 'date')
melted_data_weekly$value = as.numeric(melted_data_weekly$value)
save(melted_data_weekly, file=WEEKLY_RDATA)

# DAY OF WEEK DATA
filtered_data$wday = wday(filtered_data$date)
melted_data_wday = melt(filtered_data, id.vars=c('X_id', 'channel', 'wday', 'show'))
melted_data_wday = filter(melted_data_weekly, variable %in% c('week','date'))
save(melted_data_wday, file=DAY_OF_WEEK_RDATA)

# TIME OF DAY DATA
# XXX Need to include time in metadata

# Cleaning up workspace
rm('coverage_dates_channel', 'coverage_dates_shows','end','filtered_data','full_data',
   'labels','melted_data_bychannel', 'melted_data_byshow', 'melted_data_daily','melted_data_wday',
   'melted_data_weekly','meta','raw_data', 'show','show_keys','shows','start','valid_dates',
   'valid_shownames','valid_shows')
gc()