-- Loading base data
movies_ratings = LOAD '/Users/thomas/Documents/data/datasets/movielens/ml-100k/u.data' USING PigStorage('\t') AS (user_id:int, movie_id:int, rating:int) ;

-- Starting by limiting the dataset to movies with at least 30 ratings ;
B = GROUP movies_ratings BY movie_id ;
C = FOREACH B GENERATE group AS movie_id, COUNT($1) AS count ;
D = FILTER C BY count >= 30 ;
E = FOREACH D GENERATE movie_id AS movie_ok ;
F = JOIN movies_ratings BY movie_id, E BY movie_ok ;
filtered = FOREACH F GENERATE user_id, movie_id, rating ;

-- Creating coratings with a self join ;
filtered_2 = FOREACH F GENERATE user_id AS user_id_2, movie_id AS movie_id_2, rating AS rating_2 ;
pairs = JOIN filtered BY user_id, filtered_2 BY user_id_2 ;

-- Eliminate dupes ;
J = FILTER pairs BY movie_id < movie_id_2 ;

-- Core data ;
K = FOREACH J GENERATE 
		movie_id ,
		movie_id_2 ,
		rating ,
		rating_2 ,
		rating * rating AS ratingSq ,
		rating_2 * rating_2 AS rating2Sq ,
		rating * rating_2 AS dotProduct ;
		
L = GROUP K BY (movie_id, movie_id_2) ;

co = FOREACH L GENERATE
		group ,
		COUNT(K.movie_id) AS N ,
		SUM(K.rating) AS ratingSum ,
		SUM(K.rating_2) AS rating2Sum ,
		SUM(K.ratingSq) AS ratingSqSum ,
		SUM(K.rating2Sq) AS rating2SqSum ,
		SUM(K.dotProduct) AS dotProductSum ;

coratings = FILTER co BY N >= 30 ;
 
recommendations = FOREACH coratings GENERATE
		group.movie_id ,
		group.movie_id_2 ,
		(double)(N * dotProductSum - ratingSum * rating2Sum) / ( SQRT((double)(N * ratingSqSum - ratingSum * ratingSum)) * SQRT((double)(N * rating2SqSum - rating2Sum * rating2Sum)) ) AS correlation ;

O = ORDER recommendations BY movie_id, correlation DESC ;
P = LIMIT O 50 ;
DUMP P ;



