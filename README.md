# Movies ETL

## Description
The ETL application extracts movie data from Wikipedia and Kaggle data files, cleans and transforms the data, and load it into the movies_data PostgreSQL database

## Analysis
Programming skills alone are not enough for an ELT programmer to successfully develop a ETL program. The programmer must be able apply a general ETL data source investigation methodology and utilize programmatic data investigation techniques to determine the data elements needed to fulfill the ETL requirements and to determine quality of data provided by the selected data elements. 

This project applied the following interactive ETL data source investigation methodology to determine the data elements needed to meet the ETL requirements and to determine that transformation process needed to clean and format these data elements.

Methodology
* Determine if a data source data element fulfills an ETL requirement
* Inspect the data elements to discover data quality problems
* Determine if the data quality problem can be corrected
* Determine an algorithm to correct the problems
* Determine the effort required to implement the corrective algorithm
* Determine if the effort to correct the data element make it work implementing
* Developing the final code to implement the corrective algorithm

## Source Data Investigation and Analysis

Bases on the investigation and analysis of the Wikipedia data file it was determined that the file contained other data besides movie data. The ELT requirements only specified extracting movie data It It was determined that if a data row met the following criterial the row contained movie data 
* Data row contained a 'Director' or 'Directed by' field
* The row contained an 'imdb_link' field
* The row did not contain a 'No. of episodes' field

Movie data contained over 100 columns. Each data columns were analyzed to determine the count of null values it contained. During the transformation process all columns that had null values for 90% or more of its data were removed based on the assumption that the data in these columns were of limited or no value because they were too sparsely populated.

In the cleaning process, the 'imdb_id' was extracted from the 'imdb_link' field and stored into a new DataFrame field. Based on the assumption that rows with the same 'imdb_id’ values are duplicate rows, these duplicate rows were dropped using the  DataFrame method
*  df.drop_duplicates(subset='imdb_id', inplace=True)

Analysis of the box office and the budget fields showed that the data was either stored as strings or as a list of strings. A lambda function was created to convert box office data and budget data stored as a list of strings to a string
* lambda x: ' '.join(x) if type(x) == list else x

After converting box office and budget data to a string value, the string representation of box office and movie budgets was analyzed using regular expressions. Based on assumptions made during the analysis, two regular expression were developed to match the majority of string values for these field. Using these regular expressions, the string values of the box office and budget field were extracted and converted to  numeric values to meet the ETL requirements and  were stored in new DataFrame field.

## Merging Movie Metadata
The analysis of common columns in the movie metadata from Wikipedia and Kaggle showed that the Kaggle columns had higher quality data. Therefore, the Kaggle fields were retained in the merge DataFrame and the Wikipedia fields were dropped. However, some of the rows in the Kaggle fields were missing data. A function was created to fill in these empty Kaggle field rows with data from the the Wikipedia field before dropping the Wikipedia fields. This hybrid merge approach resulted in the final merged data set having more complete data.
