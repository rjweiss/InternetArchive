####
# This script loads the RData files created by create_dataset.R and generates plots.
# Author: Rebecca Weiss
####

##
# DAILY DATA
##

daily_data = load(DAILY_RDATA)

# Reshape filtered data: compute weekly average per topic and channel
plot_data = filtered_data %.% 
  dplyr::group_by(show, variable, date) %.%
  dplyr::summarise(value=mean(value, na.rm=T)) 

# Let's investigate by channel
#politics = dplyr::filter(melted_data_dplyr, variable == 'politics')
maddow_fig = ggplot(plot_data[str_detect(plot_data$show, 'Maddow'),], aes(
  x=date,
  y=value)) + 
  geom_histogram(stat="identity") + 
  theme(legend.position="none") +
  facet_wrap(~variable) +
  theme_bw()
#ggsave(file="output/politics_by_channel_and_week.pdf", plot=politics_fig)

shows_fig = ggplot(plot_data[str_detect(plot_data$show, shows),], aes(
  x=date,
  y=value, 
  color=show)) + 
  geom_histogram(stat="identity", position="dodge") + 
  theme(legend.position="none") +
  facet_wrap(~variable) +
  theme_bw()


hannity_fig = ggplot(plot_data[str_detect(plot_data$show, 'Hannity'),], aes(
  x=date,
  y=value)) + 
  geom_histogram(stat="identity") + 
  theme(legend.position="none") +
  facet_wrap(~variable) +
  theme_bw()

courtcase = filter(melted_data_dplyr, variable == 'courtcase')
courtcase_fig = ggplot(courtcase, aes(
  x=date,
  y=value)) + 
  geom_histogram(stat="identity") + 
  theme(legend.position="none") +
  facet_wrap(~channel)
ggsave(file="output/courtcase_by_channel_and_week.pdf", plot=courtcase_fig)

scandal = filter(melted_data_dplyr, variable == 'scandal')
scandal_fig = ggplot(scandal, aes(
  x=date,
  y=value)) + 
  geom_histogram(stat="identity") + 
  theme(legend.position="none") +
  facet_wrap(~channel)
ggsave(file="output/scandal_by_channel_and_week.pdf", plot=scandal_fig)


