# Movies ETL

## Description
The ETL application extracts movie data from Wikipedia and Kaggle data files, cleans and transforms the data, and load it into the movies_data PostgreSQL database

## Analysis
Programming skills alone are not enough for an ELT programmer to successfully develop a ETL program. The programmer must be able to apply a general ETL data source investigation methodology and utilize programmatic data investigation techniques, The methodology and techniques enable the developer to determine the data elements needed to fulfill the ETL requirements and to determine the quality of data provided by the selected data elements. 

This project applied the following interactive ETL data source investigation methodology to determine the data elements needed to meet the ETL requirements, to determine the quality of the data elements, and to define transformation process needed to clean and format these data elements to meet the ETL requirements.

Methodology
* Determine if a data element in a data source fulfills an ETL requirement
* Inspect the data element to discover data quality problems
* Determine if the data quality problem can be corrected
* Determine an algorithm to correct the problem
* Determine the effort required to implement the corrective algorithm and if it is work implementing
* Developing the final code to implement the corrective algorithm

## Source Data Investigation and Analysis

Based on the investigation and analysis of the Wikipedia data file, it was determined that the file contained additional data besides movie data. The ELT requirements only specified extracting movie data. From investigation, movie data was identified if the data row met the following criteria
* the data row contained a 'Director' or 'Directed by' field
* The data row contained an 'imdb_link' field
* The data row did not contain a 'No. of episodes' field

A Movie data entry contained over 100 columns. Each data column was analyzed to determine the count of null values it contained. During the transformation process all columns that had null values for 90% or more of its data were removed. This was based on the assumption that the data in these columns were of limited or no value because they were too sparsely populated.

In the cleaning process, the 'imdb_id' was extracted from the 'imdb_link' field and stored into a new DataFrame field. Based on the assumption that rows with the same 'imdb_id’ values are duplicate rows, these duplicate rows were dropped using the DataFrame method
*  df.drop_duplicates(subset='imdb_id', inplace=True)

Analysis of the box office and the budget fields showed that the data was either stored as strings or as a list of strings. A lambda function was created to convert box office data and budget data stored as a list of strings to a string
* lambda x: ' '.join(x) if type(x) == list else x

After converting box office and budget data to a string value, the string representation of box office and budget field values were analyzed using regular expressions. Based on assumptions made during the analysis, two regular expression were developed to match the majority of string values for these fields. Using these regular expressions, the string values of the box office and budget field were extracted and converted to numeric values and were stored in new DataFrame field.

## Merging Movie Metadata
The analysis of common columns in the movie metadata from Wikipedia and Kaggle showed that the Kaggle common columns had higher quality data. Therefore, the Kaggle fields were retained in the merge DataFrame and the Wikipedia fields were dropped. However, some of the rows in the Kaggle fields were missing data. A function was created to fill in these empty Kaggle field rows with data from the the Wikipedia field before being dropped. This hybrid merge approach resulted in the final merged data set having more complete data.
