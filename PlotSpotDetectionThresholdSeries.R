#!/usr/bin/env Rscript
library(optparse)
library(ggplot2)

# Setup options
option_list = list(
  make_option(c("-f", "--file"), type="character", default=NULL, 
              help="dataset file name", metavar="character"),
  make_option(c("-o", "--out"), type="character", default="out.pdf", 
              help="output file name [default= %default]", metavar="character")
); 

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

# Error checking
if (is.null(opt$file)){
  print_help(opt_parser)
  stop("At least one argument must be supplied (input file).csv", call.=FALSE)
}

# read input file
df = read.csv(opt$file, header=TRUE)
df$site <- paste(df$site_x,df$site_y,sep='_')

# plot results
output <- ggplot(data=df,aes(x=threshold,y=spot_count,col=control,group=site)) + 
  geom_point() + geom_line() + 
  scale_x_continuous(name = "IdentifySpots2D threshold",limits=c(0,NA)) +
  scale_y_continuous(name = "Spots per acquisition site",limits=c(0,NA))
ggsave(filename=opt$out, output,width=12,height=8, units="cm")