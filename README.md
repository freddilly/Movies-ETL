# Movies-ETL
## Challenge Overview:
 - Creating a function to automate the ETL process for movie data from Wikipedia and Kaggle.
 - The function takes in the data from the JSON and CSV  files, transforms it so that it is usable, and then uploads it to SQL tables.
## Resources:
 - Data Source: ratings.csv, movies_metadata.csv, wikipedia.movies.json (pulled from Wikipedia and Kaggle)
 - Software: Python 3.7, Visual Studio Code 1.45.1, Jupyter Notebook 6.0.3, PostgreSQL 11.8, pgAdmin 4.21
 
## Assumptions Made
For this function, there were several major assumptions that I made. Each is listed below, along with each solution.
  1. I assumed that the "wiki_movies_raw" variable would be correctly defined as the filepath to the wikipedia.movies.json file before being inputted as a function parameter. If this is not the case, then the specific transformations would not work, thus leading to an error message. I used a try-except block to tell the user to fix the "wiki_data_raw" variable if an error occurs.
  2. I assumed that the "kaggle_metadata" variable would be correctly defined as the filepath to the movies.metadata.csv file before being inputted as a function parameter. If this is not the case, then the specific transformations would not work. I used a try-except block to tell the user to fix the "kaggle_metadata" variable if an error occurs.
  3. I assumed the "ratings" variable would be correctly defined as the filepath to the ratings.csv file prior to being inputted as a function parameter. If this is not the case, then the specific transformation would not work, thus leading to an error message. I used a try-except block to tell the user to fix the "ratings" variable if an error occurs.
  4. I assumed that the user had already created a "movie_data" database on PostgresSQL. If this is not the case, then the data would not be able to be uploaded. Therefore, I added a try-except block that prints a message telling the user to create the "movie_data" database in the event of an error.
  5. I assumed that the user had cleared the "movies" table in PostgresSQL. If this is not the case, they will be unable to upload the data into the table. In order to circumvent this issue, I included a try-except block that sends the data to a new table in the event that the "movies" table has not already been cleared.
