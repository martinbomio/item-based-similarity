# item-based-similarity
Hadoop example for Montevideo BigData & DataScience Meetup.
Implementation of item based similarity for MovieLens recommendations using hadoop to create the similarity matrix.

## First Hadoop example
Simple hadoop job that creates a histogram of user ratings per movie.
For running the job:
```
hadoop jar item-based-similarity-jar-with-dependencies.jar edu.mapreduce.histogram.job.CountMovieUsersRatingsJob <input> <output>
```
Provides Job unit tests using MRunit.

