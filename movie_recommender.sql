USE movie_lens ;

-- Create the table structure ;
DROP TABLE IF EXISTS movies_ratings ;

CREATE TABLE movies_ratings (
  user_id		INT ,
  movie_id		INT ,
  rating		INT ,
  timestamp		INT ,
  INDEX(user_id)
) ;

-- Actually load the data ;
LOAD DATA 
  LOCAL INFILE '/Users/thomas/Documents/data/datasets/movielens/ml-100k/u.data'
  INTO TABLE movies_ratings
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n' ;
  
-- Create coratings and base data;  
CREATE TABLE coratings AS
SELECT
  a.movie_id ,
  b.movie_id AS movie_id_2 ,
  COUNT(*) AS N ,
  SUM(a.rating) AS ratingSum ,
  SUM(b.rating) AS rating2Sum ,
  SUM(a.rating * b.rating) AS dotProductSum ,
  SUM(a.rating * a.rating) AS ratingSqSum ,
  SUM(b.rating * b.rating) AS rating2SqSum  
FROM
  movies_ratings a
  JOIN movies_ratings b ON a.user_id = b.user_id
WHERE
  a.movie_id < b.movie_id 
GROUP BY 
  1, 2 
HAVING
  N >= 30 ;
-- 238 sec. ;
  
-- Calculate the Pearson correlation coeff for each pair ;
CREATE TABLE recommendations AS
SELECT
  movie_id ,
  movie_id_2 ,
  (N * dotProductSum - ratingSum * rating2Sum) / (SQRT(N * ratingSqSum - ratingSum * ratingSum) * SQRT(N * rating2SqSum - rating2Sum * rating2Sum)) AS correlation
FROM
  coratings
GROUP BY 
  1, 2 
ORDER BY
  1, 3 DESC ;
  
-- Show Top 5 rec per movies ;
-- From http://www.xaprb.com/blog/2006/12/07/how-to-select-the-firstleastmax-row-per-group-in-sql/
ALTER TABLE recommendations ADD KEY(movie_id, correlation);

SET @num := 0, @movie_id := 0;

CREATE TABLE top_n_recommendations AS
SELECT
  movie_id , 
  movie_id_2 , 
  correlation ,
  @num := IF(@movie_id = movie_id, @num + 1, 1) AS row_number ,
  @movie_id := movie_id AS dummy
FROM
  recommendations FORCE INDEX(movie_id)
GROUP BY
  movie_id , 
  movie_id_2 , 
  correlation
HAVING 
  row_number <= 5 
ORDER BY
  1, 3 DESC ;
