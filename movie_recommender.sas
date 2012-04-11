** Loading ratings dataset ;
filename source "Z:\data\datasets\movielens\ml-100k\u.data" ;

data Movies_Ratings ;
  attrib
	user_id		informat=best8.
	movie_id	informat=best8.
	rating		informat=best8. ;
  infile
    source dlm='09'x dsd missover ;
  input
	user_id
	movie_id
	rating ;
run ;

filename source clear ;

** Create coratings ;  
proc sql ;
  create table Pairs as
  select
    a.movie_id ,
	b.movie_id as movie_id_2 ,
	a.rating ,	
	b.rating as rating_2,
	a.user_id 
  from
    Movies_Ratings a
	join Movies_Ratings b on a.user_id = b.user_id
  where
    a.movie_id < b.movie_id
  group by
    1, 2
  having
    count(*) >= 30
  order by
    1, 2 ;
quit ;

** Get correlation ;
proc corr data=Pairs noPrint out=Recommendations (where=(_TYPE_ in ('CORR'))) spearman;
  by movie_id movie_id_2 ;
  var rating  ;
  with rating_2 ;
run ;

** Get top n recommendations ;
proc sort data=Recommendations ;
  by movie_id descending rating ;
run ;

data Recommendations ;
  set Recommendations ;
  retain counter ;
  by movie_id ;
  if first.movie_id then counter = 0 ;
  counter + 1 ;
  if counter <= 5 ;
run ;