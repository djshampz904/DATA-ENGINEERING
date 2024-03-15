#De-comment to install the packages below
install.packages(c("tidyverse","dendextend","cluster","factoextra","gplots",
                  "mvabund","vegan","RColorBrewer"))
library(tidyverse)  
library(cluster)  #For AGNES and DIANA
library(dendextend)  #For dendrogram and for calculating the cophenetic correlation
library(factoextra)  #For visualisation of clusters
library(gplots)  #For the heatmaps
library(mvabund)  #For spider dataset
library(vegan)  #For vegdist(.) function
library(RColorBrewer)  #For more colour palettes


## ---------------------------------------------------------------------------------------
dat <- read.csv("Public Utility.csv",header=TRUE,stringsAsFactors=TRUE); 
rownames(dat) <- dat[,ncol(dat)]; #Name the rows by the company names
dat <- dat[,1:(ncol(dat)-1)]; #Remove the "Company" variable as it's not required anymore.
View(dat)

dat <- na.omit(dat)  #Omit missing values although there isn't any here.
dat.z <- scale(dat);  #Standardise the variables


## ---------------------------------------------------------------------------------------
#Create the distance matrix with Euclidean distance.
dist.pu <- dist(dat.z,method="euclidean");

#heat map for distance matrix. 
fviz_dist(dist.pu,gradient = list(low = "#00AFBB", mid = "white", high = "#FC4E07"))


## ---------------------------------------------------------------------------------------
#Divisive hierarchical clustering
dhc.pu <- diana(dat.z,
                #Standardise data? Not require here since data are already standardised
                stand=FALSE); 

#Plot the dendrogram
fviz_dend(dhc.pu,  #Clustering model
          cex = 0.5,  #Font size of texts
          k = 5,  #Number of clusters. To be defined by user
          color_labels_by_k = TRUE,  #Differentiate cluster by different colours
          rect = TRUE)  #Draw a rectangle to encompass each cluster



## ---------------------------------------------------------------------------------------
#Assign group membership to objects
Group <- cutree(as.hclust(dhc.pu), k = 5); 

#PCA plot where the observations are colour-coded by their respective clusters
fviz_cluster(list(data=dat.z,cluster=Group),ggtheme=theme_minimal())


## ---------------------------------------------------------------------------------------
divcoef.pu <- dhc.pu$dc; divcoef.pu   #Divisive coefficient
coph.dhc.pu <- cor_cophenetic(dhc.pu,dist.pu); coph.dhc.pu #Cophenetic correlation


## ---------------------------------------------------------------------------------------
#Elbow Method
fviz_nbclust(dat.z, FUN = hcut, method = "wss")

#Average Silhouette Method
fviz_nbclust(dat.z, FUN = hcut, method = "silhouette")

#Gap Statistic Method
gap_stat <- clusGap(dat.z, FUN = hcut, nstart = 25, K.max = 10, B = 500)
fviz_gap_stat(gap_stat)


## ---------------------------------------------------------------------------------------
#Agglomerative hierarchical clustering using Average Linkage
ahc.pu.ave <- agnes(dat.z,method="average"); 

#Plot the dendrogram
fviz_dend(ahc.pu.ave, cex = 0.5, k = 5,
 color_labels_by_k = TRUE, rect = TRUE)


## ---------------------------------------------------------------------------------------
Group <- cutree(as.hclust(ahc.pu.ave), k = 5); #Assign group membership to objects.
fviz_cluster(list(data=dat.z,cluster=Group),ggtheme=theme_minimal())


## ---------------------------------------------------------------------------------------
aggcoef.ave <- ahc.pu.ave$ac; aggcoef.ave  #Agglomerative coefficient
coph.cor.ave <- cor_cophenetic(ahc.pu.ave,dist.pu); coph.cor.ave  #Cophenetic corrrelation


## ---------------------------------------------------------------------------------------

#Convert clustering information to dendrograms 
dend1 <- as.dendrogram(ahc.pu.ave);  
dend2 <- as.dendrogram(dhc.pu); 

entanglement(dend1,dend2)  #Entanglement measure

tanglegram(dend1,dend2,
           highlight_distinct_edges = FALSE, # Turn-off dashed lines
           common_subtrees_color_lines = FALSE, # Turn-off line colors
           common_subtrees_color_branches = TRUE) # Colour common branches
           


## ---------------------------------------------------------------------------------------
#Agglomerative hierarchical clustering using Average Linkage
dat.z.trp <- t(dat.z); 
dist.pu.trp <- dist(dat.z.trp,method="euclidean");
ahc.pu.ave2 <- agnes(dist.pu.trp,method="average"); 
aggcoef.ave2 <- ahc.pu.ave2$ac; aggcoef.ave2   #Agglomerative coefficient
coph.cor.ave2 <- cor_cophenetic(ahc.pu.ave2,dist.pu.trp); coph.cor.ave2

#Plot the dendrogram
fviz_dend(ahc.pu.ave2, cex = 0.5, k = 5,
 color_labels_by_k = TRUE, rect = TRUE, labels_track_height=2.8)


## ---------------------------------------------------------------------------------------
colfunc <- colorRampPalette(c("gray", "white", "red"))  #Custom colour palette
heatmap.2(dat.z,  #Input data
          Rowv=as.dendrogram(ahc.pu.ave),
          Colv=as.dendrogram(ahc.pu.ave2),
          cexRow=0.8,  #Font size of row labels
          cexCol=0.8,  #Font size of column labels
          #Relabel the column text
          labCol=c("FCCR","Return\non capital","% nuclear","Cost\nper KW",
                   "Peak KWH\ngrowth","Sales","Annual foad\nfactor","Total fuel\ncost"),
          key.title=NA,
          col=colfunc(15)) 



## ---------------------------------------------------------------------------------------
data(spider)  #Load the spider data
dat.abund <- spider$abund  #Extract the abundance data

#Clustering of sites
dist.site <- vegdist(dat.abund,method="bray");
ahc.site <- agnes(dist.site,method="average"); 
aggcoef.site <- ahc.site$ac; aggcoef.site 
coph.cor.site <- cor_cophenetic(ahc.site,dist.site); coph.cor.site

#Plot the dendrogram
fviz_dend(ahc.site, cex = 0.8, k = 4,
 color_labels_by_k = TRUE, rect = TRUE,labels_track_height=-0.4)


## ---------------------------------------------------------------------------------------
#Clustering of species
dist.species <- vegdist(t(dat.abund),method="bray");
ahc.species <- agnes(dist.species,method="average"); 
aggcoef.species <- ahc.species$ac; aggcoef.species  
coph.cor.species <- cor_cophenetic(ahc.species,dist.species); coph.cor.species

#Plot the dendrogram
fviz_dend(ahc.species, cex = 0.8, k =5,
 color_labels_by_k = TRUE, rect = TRUE,labels_track_height=-0.1)

## ---------------------------------------------------------------------------------------
heatmap.2(as.matrix(dat.abund),  #Input data. Must be a numeric matrix.
          Rowv=as.dendrogram(ahc.site),
          Colv=as.dendrogram(ahc.species),
          cexRow=0.8,  #Font size of row labels
          cexCol=0.8,  #Font size of column labels
          key.title=NA,col=colfunc(15)) 


## ---------------------------------------------------------------------------------------
dat.abund.4root <- (dat.abund)^(1/4); #Fourth root transformation

#AGNES by sites on the transformed data
dist.site.4root <- vegdist(dat.abund.4root,method="bray");
ahc.site.4root <- agnes(dist.site.4root,method="average"); 

#AGNES by species on the transformed data
dist.species.4root <- vegdist(t(dat.abund.4root),method="bray");
ahc.species.4root <- agnes(dist.species.4root,method="average"); 

heatmap.2(as.matrix(dat.abund.4root),  #Input data. Must be a numeric matrix.
          Rowv=as.dendrogram(ahc.site.4root),
          Colv=as.dendrogram(ahc.species.4root),
          cexRow=0.8,  #Font size of row labels
          cexCol=0.8,  #Font size of column labels
          key.title=NA,col=colfunc(15)) 


