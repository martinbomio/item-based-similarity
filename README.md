# item-based-similarity
Hadoop example for Montevideo BigData & DataScience Meetup.
Implementation of item based similarity for MovieLens recommendations using hadoop to create the similarity matrix.

## First Hadoop example
Simple hadoop job that creates a histogram of user ratings per movie. (`edu.mapreduce.histogram.job.CountMovieUsersRatingsJob`)
To running the job:
```
hadoop jar item-based-similarity-jar-with-dependencies.jar edu.mapreduce.histogram.job.CountMovieUsersRatingsJob <input> <output>
```
Provides Job unit tests using MRunit.

## Custom Hadoop formats
Job that creates the history of movie ratings per user. Uses custom input and output formats to read and write the data. (`edu.mapreduce.formats.job.UserMoviesRatingJob`)
To run the job:
```
hadoop jar item-based-similarity-jar-with-dependencies.jar edu.mapreduce.formats.job.UserMoviesRatingJob <input> <output>
```

