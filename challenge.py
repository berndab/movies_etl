import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import time
from config import db_password

def movie_data_etl(wiki_movie_meta_data, kaggle_movie_metadata, kaggle_movie_ratings):

    ###############################################
    #               Inner functions               #
    ###############################################
 
    def clean_movie(movie):
    
        # Create a non-destructive dictonary copy
        movie = dict(movie)
        
        # Create dictionary to store the alternate titles of the movie
        alt_titles = {}
        
        # Alternate title keys
        for key in ['Also known as','Arabic','Cantonese','Chinese','Fre nch',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            
            # If the movie has an alternate title for the key
            # store the title inthe alt_titles dict
            # remove the key containing the alternate title
            # from the movie dict
            if key in movie:
                alt_titles[key] = movie[key] 
                movie.pop(key)
        
        # If the movie has alternate titles
        # store the alternate titles in the movie dict
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # Standardize movie dictionary keys
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
                
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

    
    def parse_dollars(s):
    
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub(r'\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan
    
    # Replace missing kaggle metadata column values with 
    # wiki movie metadata column values  
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column],axis=1)
        df.drop(columns=wiki_column, inplace=True)
   

    print("Movie data ETL process - start")


    ###############################################
    #  Processing the wiki movie metadata file   #
    ###############################################

    try:

        print(f"Processing file:{wiki_movie_meta_data} - start")

        # Importing data
        with open(wiki_movie_meta_data, mode='r') as file:
            wiki_movies_raw = json.load(file) 

        # Filter for the required movie types
        wiki_movies = [movie for movie in wiki_movies_raw if ('Director' in movie or 'Directed by' in movie) and 'imdb_link' in movie and 'No. of episodes' not in movie]

        # Clean movies data
        clean_movies = [clean_movie(movie) for movie in wiki_movies]

        # Create a datafrom from the clean_movies json object
        wiki_movies_df = pd.DataFrame(clean_movies)

        # Extract the imdb id from the imdb link
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')

        # Remove duplicate movies with the same imdb id
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

        # Generate list of DataFrame columns that have 
        # not null values in more that 10% of their rows
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]

        # Only select DataFrame columns that have
        # where more than 10% of their rows contain data
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]    
        
        # regular expressions for dollar string value
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        
        #
        # Parse wiki movie metadata box office column
        #

        # Get data from the orginal column and drop rows with null data
        box_office = wiki_movies_df['Box office'].dropna()

        # Convert data stored in the form of a list to a string
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Reformat data expressed in a range
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        # Extract numeric string value
        # convert it to a float value and
        # store it into a new column in the DataFrame
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        # Drop original DataFrame column
        wiki_movies_df.drop('Box office', axis=1, inplace=True)

        #
        # Parse wiki movie metadata budget column
        #

        # Get data from the orginal column and drop rows with null data
        budget = wiki_movies_df['Budget'].dropna()

        # Convert data stored in the form of a list to a string
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)

        # Reformat data expressed in a range
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        # Extract numeric string value
        # convert it to a float value and
        # and store it into a new column in the DataFrame
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        # Drop original DataFrame column
        wiki_movies_df.drop('Budget', axis=1, inplace=True)

        #
        # Parse wiki movie metadata release date column
        #

        # Get data from the orginal column and drop rows with null data
        # Covert data stored in the form of a list to a string
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # regular expressions for date strings
        date_form_one   = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two   = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four  = r'\d{4}'

        # Extract date string value
        # convert it to a datetime value
        # and store it into a new column in the DataFrame
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

        # Drop original DataFrame column
        wiki_movies_df.drop('Release date', axis=1, inplace=True)
    
        #
        # Parse wiki movie metadata running time column
        #
    
        # Get data from the orginal column and drop rows with null data
        # Covert data stored in the form of a list to a string
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        # Extract time string values
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

        # Convert the time values to numeric
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

        # Convert numeric values to minutes
        # and store it into a new column in the DataFrame
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

        # Drop original DataFrame column
        wiki_movies_df.drop('Running time', axis=1, inplace=True)

        print(f"Processing file:{wiki_movie_meta_data} - complete")

    except:

        print(f"An exception occured while processing file:{wiki_movie_meta_data}")

        raise

    ###############################################
    #  Processing the Kaggel movie metadata file  #
    ###############################################

    try:

        print(f"Processing file:{kaggle_movie_metadata} - start")

        # Importing data
        kaggle_metadata = pd.read_csv(kaggle_movie_metadata)

        # Filter out all adult movie rows in the metadata
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

        # Convert the column value to boolean
        kaggle_metadata['video']        = kaggle_metadata['video'] == 'True'

        # Convert the column value to integer
        kaggle_metadata['budget']       = kaggle_metadata['budget'].astype(int)

        # Convert the column value to numeric
        kaggle_metadata['id']           = pd.to_numeric(kaggle_metadata['id'], errors='raise')

        #Convert the column value to numeric
        kaggle_metadata['popularity']   = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')

        #Convert the column value to datetime
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

        print(f"Processing file:{kaggle_movie_metadata} - complete")

    except:

        print(f"An exeption occurred while processing file:{kaggle_movie_metadata}")

        raise

    ###############################################
    #  Processing the Kaggel movie ratings file   #
    ###############################################

    try:

        print(f"Processing file:{kaggle_movie_ratings} - start")

        # Importing data
        ratings = pd.read_csv(kaggle_movie_ratings)

        # Convert ratings unix epoch time to datetime formate
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

        print(f"Processing file:{kaggle_movie_ratings} - complete")
        
    except:

        print(f"An exception occured while processing file:{kaggle_movie_ratings}")

        raise
 
    ##################################################################
    #  Merging wiki metadata DataFram and kaggle metadata DataFrame  #
    ##################################################################
    
    try:

        print("Merging wiki and kaggle metadata DataFrames - start")

        # Merging DataFrames
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

        # drop redundant columns in the merged DataFrame
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

        # Replace missing kaggle metadata column values with 
        # wiki movie metadata column values
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')


        # Select columns and column order for the final version of the merged data frame 
        movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                            'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                            'genres','original_language','overview','spoken_languages','Country',
                            'production_companies','production_countries','Distributor',
                            'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                            ]]


        # Rename columns to use standard column names
        movies_df.rename({'id':'kaggle_id',
                        'title_kaggle':'title',
                        'url':'wikipedia_url',
                        'budget_kaggle':'budget',
                        'release_date_kaggle':'release_date',
                        'Country':'country',
                        'Distributor':'distributor',
                        'Producer(s)':'producers',
                        'Director':'director',
                        'Starring':'starring',
                        'Cinematography':'cinematography',
                        'Editor(s)':'editors',
                        'Writer(s)':'writers',
                        'Composer(s)':'composers',
                        'Based on':'based_on'
                        }, axis='columns', inplace=True)

        print("Merging wiki and kaggle metadata DataFrames - complete")

    except:

        print("An error occurred while merging wiki and kaggle metadata DataFrames")

        raise

    ##################################################################
    #  Merging wiki metadata DataFram and kaggle metadata DataFrame  #
    ##################################################################

    try:

        print("Merging ratings DataFrame with metadata DataFrame - start")

        # Get movie ratings value counts pivot table
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count().rename({'userId':'count'}, axis=1).pivot(index='movieId',columns='rating', values='count')

        # Rename ratings columns
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

        # Merge DataDrames
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

        # Not all movies with have ratings for each ratings value 
        # Fill in movie ratings columns that do not have values with 0
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

        print("Merging ratings DataFrame with metadata DataFrame - complete")

    except:
 
        print("An error occurred while merging ratings DataFrame with metadata DataFrame")

        raise

    ##################################################################
    #      Importing merged movie metadata into the database         #
    ##################################################################

    try:

        print("Connecting to the database")

       # Create a database engine
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)

        print("Connection to the database - SUCCESSFUL")

    except:

        print("An exception occurred connecting to the database")

        raise

    try:

        print("Importing movie metadata into the movie_data database movies table- start")
 
        # Store merged movie metadata to the database
        #
        # NOTE: Module section "8.5.1 Connect Pandas and SQL" subsection "Import the Movie Data" 
        # saves the movie merged metadata DataFrame  movies_df to the database. 
        # However, in section "8.4.2 Transform and Merge Rating Data", the merged movie metadata 
        # DataFrame movies_df is merged with movie ratings DataFrame rating_counts to create
        # the movies_with_ratings_df DataFrane, but it is never stored in the database
        #
        # I think that the code in  8.5.1 "Import the Movie Data" erroneously
        # stores movies_df to the database instead of movies_with_ratings_df
        #
        # I followed the code in 8.5.1 as is and stored movies_df to the database
        movies_df.to_sql(name='movies', con=engine, if_exists="replace")

        print("Importing movie metadata into the movie_data database movies table - complete - " + str(len(movies_df)) + " rows imported")

    except:

        print("An error occurred importing movie metadata into the movie_data database movies table")        

        raise
    
 
    ##################################################################
    #       Importing movie ratings data  into the database          #
    ##################################################################

 
    try:
        
        print("Importing movie ratings data into the movie_data database ratings table - start")

        # Clear all data from the ratings table
        engine.connect().execute("TRUNCATE TABLE ratings")

        rows_imported = 0
        # get the start_time from time.time()
        start_time = time.time()
        for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)

            # add elapsed time to final print out
            print(f'Done. {time.time() - start_time} total seconds elapsed')
        
        print("Importing movie ratings data into the movie_data database ratings table - complete")

    except:

        print("An exception occurred importing ratings data into the movie_data database ratings table")

        raise

    print("Movie data ETL process - Complete")



# Execute function

file_dir = 'C:/Users/deanb/OneDrive/Documents/Git/UTBootcamp/movies_etl/data/'
wiki_movie_meta_data   = f'{file_dir}wikipedia.movies.json'
kaggle_movie_metadata  = f'{file_dir}movies_metadata.csv'
kaggle_movie_ratings   = f'{file_dir}ratings.csv'

movie_data_etl(wiki_movie_meta_data, kaggle_movie_metadata, kaggle_movie_ratings)