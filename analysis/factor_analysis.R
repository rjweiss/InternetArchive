##
# DIMENSIONALITY REDUCTION
##

##
# DAY OF WEEK

# By Show
# cast_data_wday_byshow = dcast.data.table(as.data.table(melted_data_wday_byshow), formula=show + wday ~ variable, value.var = 'mean')
# cast_data_wday_byshow = as.data.frame(cast_data_wday_byshow)
# 
# ecb = function(x, y){
#   fig = ggplot(data=as.data.frame(x), aes(x=V1, y=V2, color=cast_data_wday_byshow$show, label=cast_data_wday_byshow$show)) + geom_text() +
#     theme(legend.position="none") # guides(color=guide_legend(ncol=3)) 
#   print(fig)
# }
# tsne_cast_data_wday_byshow = tsne(cast_data_wday_byshow[,3:ncol(cast_data_wday_byshow)], epoch_callback = ecb, perplexity=50)

# By Channel
cast_data_wday_bychannel = dcast.data.table(as.data.table(melted_data_wday_bychannel), formula=channel + wday ~ variable, value.var = 'mean')
cast_data_wday_bychannel = as.data.frame(cast_data_wday_bychannel)
cast_data_wday_bychannel = join(cast_data_wday_bychannel, MEDIA_NAMES)

ecb = function(x, y){
  fig = ggplot(data=as.data.frame(x), aes(x=V1, y=V2, color=cast_data_wday_bychannel$fulltitle, label=cast_data_wday_bychannel$fulltitle)) + geom_text() + theme(legend.position="none")
  print(fig)
}
tsne_cast_data_wday_bychannel = tsne(cast_data_wday_bychannel[,3:(ncol(cast_data_wday_bychannel)-4)], epoch_callback = ecb, perplexity=50)

# Use most predictive dimensions determined by mean centered data, random forests
subset_dims = c('politics','religion','commercials','local','international', 'us_foreign_affairs')
subset_data = subset(cast_data_wday_bychannel, select = subset_dims)

center_scale <- function(x) {
  scale(x, scale = FALSE)
}

subset_data = sapply(subset_data, center_scale)
tsne_cast_data_wday_bychannel = tsne(test, epoch_callback = ecb, perplexity=50)
plotmatrix = ggpairs(test)

#som_data = cast_data_wday_bychannel[,3:(ncol(cast_data_wday_bychannel)-4)]
#som_data = filtering(som_data)
#som_data = normalize(som_data)
#foo = som(som_data, xdim=100, ydim=100)

# Using PCA
# pca_cast_data_wday = princomp(cast_data_wday[,2:ncol(cast_data_wday)])
# pca_cast_data_wday_2dim = pca_cast_data_wday$scores[,1:2]
# plot(pca_cast_data_wday_2dim, t='n')
# text(pca_cast_data_wday_2dim, labels=cast_data_wday$show, col=colors[cast_data_wday$show])
# 
# pca_cast_data_byshow = princomp(cast_data_byshow[,2:ncol(cast_data_byshow)])
# pca_cast_data_byshow_2dim = pca_cast_data_byshow$scores[,1:2]
# plot(pca_cast_data_byshow_2dim, t='n')
# text(pca_cast_data_byshow_2dim, labels=cast_data_byshow$show, col=colors[cast_data_byshow$show])
# 
# pca_cast_data_wday_bychannel = princomp(cast_data_wday_bychannel[,2:(ncol(cast_data_wday_bychannel)-4)])
# pca_cast_data_wday_bychannel_2dim = pca_cast_data_wday_bychannel$scores[,1:2]
# plot(pca_cast_data_byshow_2dim, t='n')
# text(pca_cast_data_byshow_2dim, labels=cast_data_byshow$show, col=colors[cast_data_byshow$show])
