library(tidyverse)

#Import and merge data
# ---------------------------------------------------------------------------------------
LOF = read.csv("/home/pascal/code/masters/bigdata/Local-Outlier-Factor-LOF-in-Spark/data/results/lof.txt/part-00000", header = F)
names(LOF)[1] = "ID"
names(LOF)[2] = "LOF"


ABOD = read.csv("/home/pascal/code/masters/bigdata/Local-Outlier-Factor-LOF-in-Spark/withindex.csv", header= F)
names(ABOD)[1] = "ID"
names(ABOD)[2] = "X"
names(ABOD)[3] = "Y"

data = merge(LOF,ABOD,by="ID")

#Calculate and graph data
# ---------------------------------------------------------------------------------------

lambda = 0.005
data = data %>% mutate(LOF_R = dense_rank(desc(LOF)))
data = data %>% mutate(ABOD_R = dense_rank(desc(ABOD)))
data$LOF_Out = ifelse(data$LOF_R < lambda*nrow(data),1,0)
data$ABOD_Out = ifelse(data$ABOD_R < (lambda)*nrow(data),1,0)

ggplot(data, aes(X, Y)) + 
  geom_point(aes(colour = factor(LOF_Out)))

ggplot(data, aes(X, Y)) + 
  geom_point(aes(colour = factor(ABOD_Out)))



