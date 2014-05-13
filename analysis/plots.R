####
# This script loads the RData files created by create_dataset.R and generates plots.
# Author: Rebecca Weiss
####

# Load data
load(BYCHANNEL_RDATA)
load(BYSHOW_RDATA)
load(DAILY_RDATA)
load(WEEKLY_RDATA)
load(DAY_OF_WEEK_RDATA)
media_name = read.csv('docs/source_data.csv') # XXX This should be taken care of in config.R

# AGGREGATE PLOTS
topics = as.character(unique(melted_data_byshow$variable))

# By Channel
melted_data_bychannel = join(melted_data_bychannel, media_name)

for (topic in topics) {
  topic_subset = filter(melted_data_bychannel, variable %in% topic)
  fig = ggplot(topic_subset, aes(
    x=reorder(fulltitle, mean),
    y=mean,
    fill=fulltitle)) + 
    geom_histogram(stat="identity", position="dodge") + 
    theme(legend.position="none") +
    coord_flip() +
    theme_bw() +
    theme(legend.position="none") +
    theme(axis.text.x = element_text(angle = 90))
  filename = file.path(paste(TOPICS_OUTPUT_DIR, topic, '_bychannel.pdf', sep=''))
  ggsave(file=filename, fig, height=11) 
}

# By Show
for (topic in topics) {
  topic_subset = filter(melted_data_byshow, variable %in% topic)
  fig = ggplot(topic_subset, aes(
    x=reorder(show, mean),
    y=mean,
    fill=show)) + 
    geom_histogram(stat="identity", position="dodge") + 
    theme(legend.position="none") +
    coord_flip() +
    theme_bw() +
    theme(legend.position="none") +
    theme(axis.text.x = element_text(angle = 90))
  filename = file.path(paste(TOPICS_OUTPUT_DIR, topic, '_byshow.pdf', sep=''))
  ggsave(file=filename, fig, height=11) 
}

# # DAILY DATA
# 
# 
# # Reshape filtered data: compute weekly average per topic and channel
# daily_plot_data = melted_data_daily %.% 
#   dplyr::group_by(show, variable, date) %.%
#   dplyr::summarise(value=mean(value, na.rm=T)) 
# 
# # Let's investigate by day
# #politics = dplyr::filter(melted_data_dplyr, variable == 'politics')
# maddow_fig = ggplot(daily_plot_data[str_detect(daily_plot_data$show, 'Maddow'),], aes(
#   x=date,
#   y=value)) + 
#   geom_histogram(stat="identity") + 
#   theme(legend.position="none") +
#   facet_wrap(~variable) +
#   theme_bw()
# #ggsave(file="output/politics_by_channel_and_week.pdf", plot=politics_fig)
# 
# hannity_fig = ggplot(plot_data[str_detect(plot_data$show, 'Hannity'),], aes(
#   x=date,
#   y=value)) + 
#   geom_histogram(stat="identity") + 
#   theme(legend.position="none") +
#   facet_wrap(~variable) +
#   theme_bw()
# 
# courtcase = filter(melted_data_dplyr, variable == 'courtcase')
# courtcase_fig = ggplot(courtcase, aes(
#   x=date,
#   y=value)) + 
#   geom_histogram(stat="identity") + 
#   theme(legend.position="none") +
#   facet_wrap(~channel)
# ggsave(file="output/courtcase_by_channel_and_week.pdf", plot=courtcase_fig)
# 
# scandal = filter(melted_data_dplyr, variable == 'scandal')
# scandal_fig = ggplot(scandal, aes(
#   x=date,
#   y=value)) + 
#   geom_histogram(stat="identity") + 
#   theme(legend.position="none") +
#   facet_wrap(~channel)
# ggsave(file="output/scandal_by_channel_and_week.pdf", plot=scandal_fig)
# 
# # WEEKLY DATA

# 
# # DAY OF WEEK DATA


