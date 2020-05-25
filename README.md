# Movies ETL

## Description
The ETL application extracts movie data from Wikipedia and Kaggle data files transforms the data and load it into the movies_data PosgreSQL database

## Analysis
This project has shown that programming skills are not enought for an developer to successfully develop a ETL programm to process and itegrate various data sources. The programmer must develop strong analytical skills to investigate the the quality of the data, to develop a strategy to transform the data into the formated needed b, and to decide on a data merging strategy of the various data sources to generate the most accurate and highest quality result set.

The methodology used in developing this ETL process involves evaluating the data elemennts of a data source and assigning the data element into one of three catigories
* Beyond repair
* Badly damaged
* Wrong form

Data elements that are beyond repair are data elements that have to many missing value. In this situation the data element must be dropped during the transformation process. In this project the assumption was made to drop data columns from the Wikipedia move data file if had less then 10% of its values not null. Also the assumption was make to remove any entry in the wiKIpedia movie data set on the following critera
* Had 'Director' or 'Directed by'
* Had 'imdb_link'
* Did Not Have 'No. of episodes'

Using this process to eliminate bad data was essential to make sure only accurate quality data would be produced by the ETL process


