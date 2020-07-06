#Import dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import time
from datetime import datetime
import random

#Create a variable for the file directory
file_dir = 'C:/Users/freds/Desktop/Data Analytics Projects/Movies-ETL'

# Open the file and load it into the "wiki_movies_raw" variable
with open(f'{file_dir}/wikipedia.movies.json', mode='r', encoding='utf-8') as file:
    wiki_movies_raw = json.load(file)

#Load the movies_metadata.csv and ratings.csv files into variables.
kaggle_metadata = pd.read_csv(f'{file_dir}/movies_metadata.csv')
ratings = pd.read_csv(f'{file_dir}/ratings.csv')


def Movie_ETL(wiki_movies_raw, kaggle_metadata, ratings):
#Transform wikipedia data Step 
    #Establish criteria for the movies we are interested in using (has a director, imdb link, and 0 episodes (not a TV show!))
    try:
        wiki_movies = [movie for movie in wiki_movies_raw
                  if ('Director' in movie or 'Directed by' in movie) and 'imdb_link' in movie and 'No. of episodes' not in movie]
        #Function to clean the wikipedia data
        def clean_movie(movie):
            movie = dict(movie) #create a non-destructive copy
            alt_titles = {}
            for key in ['Also known as','Arabic','Cantonese','Chinese','French','Hangul','Hebrew','Hepburn','Japanese','Literally','Mandarin','McCune–Reischauer','Original title','Polish','Revised Romanization','Romanized','Russian','Simplified','Traditional','Yiddish']:
                if key in movie:
                    alt_titles[key] = movie[key]
                    movie.pop(key)
            if len(alt_titles) > 0:
                movie['alt_titles'] = alt_titles

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
        #Apply the function to all rows of wikipedia data. Convert into a DataFrame. 
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        wiki_movies_df = pd.DataFrame(clean_movies)

        #Find the movies with an imdb_id fitting the format required. Then remove any duplicate rows.
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id',inplace=True)

        #Establish columns worth keeping and only keep those in the dataframe. 
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df)*0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

        #Establish box_office variable to represent movies that have box office value listed.
        box_office = wiki_movies_df['Box office'].dropna()

        #Join list items in the box_office column into a string.
        box_office = box_office.apply(lambda x: ''.join(x) if type(x) == list else x)

        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        #Extract box office values that match either form_one or form_two
        box_office.str.extract(f'({form_one}|{form_two})')

        #Create a function to parse the dollars values and create the correct format for them.
        def parse_dollars(s):
            # if s is not a string, return NaN
            if type(s) != str:
                return np.nan

            # if input is of the form $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

                # remove dollar sign and " million"
                s = re.sub('\$|\s|[a-zA-Z]','', s)

                # convert to float and multiply by a million
                value = float(s) * 10**6

                # return value
                return value

            # if input is of the form $###.# billion
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

                # remove dollar sign and " billion"
                s = re.sub('\$|\s|[a-zA-Z]','', s)

                # convert to float and multiply by a billion
                value = float(s) * 10**9

                # return value
                return value

            # if input is of the form $###,###,###
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

                # remove dollar sign and commas
                s = re.sub('\$|,','', s)

                # convert to float
                value = float(s)

                # return value
                return value

            # otherwise, return NaN
            else:
                return np.nan

        #Apply the parse_dollars function to the box_office column.
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        #Drop the old "Box office" column from our DataFrame.
        wiki_movies_df.drop('Box office', axis=1, inplace=True)

        #Create the budget variable to drop all movies that don't have a budget listed.
        budget = wiki_movies_df['Budget'].dropna()

        #Join all lists into strings for the budget variable.
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

        #Remove citation references.
        budget = budget.str.replace(r'\[\d+\]\s*', '')

        #Extract budgets that fit the desired formatting.
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

        #Remove the redundant 'Budget' column from the dataframe.
        wiki_movies_df.drop('Budget', axis=1, inplace=True)

        #Create the release data variable to hold delete movies with no release date available and join the lists into a single string.
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        #Create the date forms that we are looking to use with regular expressions.
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'

        #set the release date column in the dataframe to match our format.
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

        #Set the running time variable to hold movies that have a running time listed. Join lists into a single string.
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

        #have the running_time_extract variable hold the running times fitting our format requirements.
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

        #Convert running times to numeric format from string.
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

        #Remove the redundant column.
        wiki_movies_df.drop('Running time', axis=1, inplace=True)

        #Merge the wikipedia and kaggle dfs 
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    except:
        print("There is an issue with the parameter 'wiki_movies_raw' check the file path.")

#Transform Kaggle Data Step
    try:
        #Keep the kaggle data where the adult column is equal to "False"
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'FALSE'].drop('adult',axis='columns')

        #keep the kaggle rows where the 'video' column is true
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'

        #Convert budget, id, and popularity to the correct data types.
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')

        #Convert 'release_date' to datetime format.
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

        #Drop row that doesn't make sense
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

        #Drop the specified columns
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

        #Fills in columns with missing data, then removes the redundant column
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(
                lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
            df.drop(columns=wiki_column, inplace=True)

        #Fill selected columns with missing data, then remove the redundant columns
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

        #Have the DataFrame contain only the desired columns.
        movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link','runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count','genres','original_language','overview','spoken_languages','Country','production_companies','production_countries','Distributor','Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on']]

        #Rename the DataFrame columns to more usable titles.
        movies_df.rename({'id':'kaggle_id','title_kaggle':'title','url':'wikipedia_url','budget_kaggle':'budget','release_date_kaggle':'release_date','Country':'country','Distributor':'distributor','Producer(s)':'producers','Director':'director','Starring':'starring','Cinematography':'cinematography','Editor(s)':'editors','Writer(s)':'writers','Composer(s)':'composers','Based on':'based_on'}, axis='columns', inplace=True)
    except:
        print("The 'kaggle_metadata' parameter has an error, check the file path.")
    #convert the timestamp column of the ratings data to datetime
    try: 
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

        # movieID is the index, the columns will be all the rating values, and the rows will be the counts for each rating value.
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                        .rename({'userId':'count'}, axis=1) \
                        .pivot(index='movieId',columns='rating', values='count')
        #Rename the columns
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

        #convert "kaggle_id" datatype so as to be able to merge DataFrames.
        movies_df["kaggle_id"]=movies_df["kaggle_id"].astype(int)

        #Merge the ratings DataFrame with the movies dataframe
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

        #fill NaN values for rating counts with 0.
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    except: 
        print("The 'ratings' parameter has an error, check the file path.")

#Load the data into a SQL database
    #SQL Connection String
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

    #Create database engine
    engine = create_engine(db_string)

    #Export the ratings data to a SQL table.
    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    try:
        for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)
            # add elapsed time to final print out
            print(f'Done. {time.time() - start_time} total seconds elapsed')
    except:
        #If there is an error, they probably did not create a 'movie_data' database.
        print("You must create the 'movie_data' SQL database before upload!") 

    #Move the DF to a SQL table. If unable to load to the SQL table, load to another table.
    try:
        movies_df.to_sql(name='movies', con=engine)
    #If there is an error, divert the data to a new table.
    except:
        random_suffix = random.random()
        movies_df.to_sql(name=f'movies_diverted_{random_suffix}',con=engine)
        print("Next time, be sure to clear the 'movies' table before trying to run the function.")
        print(f'The "movies" DataFrame is stored in the movies_diverted_{random_suffix} table')
        

    
    
    print("Data Upload Complete!")


