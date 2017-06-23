#!/usr/bin/env Rscript
library(optparse)
library(ggplot2)

# Setup options
option_list = list(
  make_option(c("-f", "--file"), type="character", default=NULL, 
              help="dataset file name", metavar="character"),
  make_option(c("--out_all"), type="character", default="out_all.pdf", 
              help="output file name [default= %default]", metavar="character"),
  make_option(c("--out_mean"), type="character", default="out_mean.pdf", 
              help="output file name [default= %default]", metavar="character")
); 

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

# Error checking
if (is.null(opt$file)){
  print_help(opt_parser)
  stop("At least one argument must be supplied (input file).csv", call.=FALSE)
}

# read input file and derive variable
df = read.csv(opt$file, header=TRUE)
df$site <- paste(df$site_x,df$site_y,df$control,sep='_')
df$controlwell <- paste(df$control,df$well,sep='_')

# plot results
output <- ggplot(data=df,aes(x=threshold,y=spot_count,col=control,group=site)) + 
  geom_point(alpha=0.4,size=.2) + geom_line(alpha=0.4) + 
  scale_x_continuous(name = "IdentifySpots2D threshold",limits=c(0,NA)) +
  scale_y_continuous(name = "Spots per acquisition site",limits=c(0,NA))
ggsave(filename=opt$out_all, output,width=12,height=8, units="cm")

# aggregate by control
mean <- aggregate(x=df$spot_count,by=list(df$controlwell,df$threshold),FUN=mean)
names(mean) <- c("controlwell","threshold","mean_spot_count")

output <- ggplot(data=mean,aes(x=threshold,y=mean_spot_count,col=controlwell)) + 
  geom_point() + geom_line() + 
  scale_x_continuous(name = "IdentifySpots2D threshold",limits=c(0,NA)) +
  scale_y_continuous(name = "Spots per acquisition site",limits=c(0,NA))
ggsave(filename=opt$out_mean, output,width=12,height=8, units="cm")

