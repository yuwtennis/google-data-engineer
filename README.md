
### Notice

On 2025.02.02 I went through with the book all the way to the end.  
However, some topics were not implemented since the 1st edition of the book was out-dated and   
there was no point to put cost into it. This repos will be archived.

I will start new repository with 2nd edition and if you are interested , follow the new one.

Thank you.
@yuwtennis

# google-data-engineer

- [Overview](#overview)  
- [Instructions](#instructions)  
  - [Chapter6](#chapter-6)

## Overview

Sets of code I have created while studying below book.

Data Science on the Google Cloud Platform  
http://shop.oreilly.com/product/0636920057628.do

1. Ch02. Ingesting Data into the Cloud
2. Ch03. Creating Compelling Dashboard
3. Ch04. Streaming Data' Publiation and Ingest
4. Ch05. Interactive Data Exploration
5. Ch06. Bayes Classifier on Cloud Dataproc

## Instructions
### Chapter 6

1. Activate environment variables
```
cd Ch06/dataproc
source env.sh
```

2. Copy bootstrap script to google storage
```
./copy_to_gs.sh
```

3. Start dataproc
```
./init_cluster.sh
```

4. Remove cluster
```
./delete_cluster.sh
```
