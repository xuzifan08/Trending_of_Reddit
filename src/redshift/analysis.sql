--drop duplicates by creating and deleting temp tables
delete from authors using authors_temp where authors.author_id = authors_temp.author_id;
insert into authors select * from authors_temp;
Drop table authors_temp;
	

delete from subreddit using subreddit_temp 
where subreddit.subreddit_id = subreddit_temp.subreddit_id;
Insert into subreddit select * from subreddit_temp;
Drop table subreddit_temp


--query score density for each subreddit over hours
create table subm_stage
sortkey(subreddit_id)
as 
select extract(hour from to_timestamp("time",'DD/MM/YYYY HH:MI:SS')) as hour,
score, submission_id, subreddit_id
from submission;


Create table subreddit_stage
Sortkey(subreddit_id)
As
Select * from subreddit;


create table subreddit_density
as
select hour,sum(score)/count(submission_id) as score_density, subreddit_stage.subreddit_id, subreddit
from subm_stage
left join subreddit_stage on subm_stage.subreddit_id = subreddit_stage.subreddit_id
group by subreddit_stage.subreddit_id, subreddit,hour;


--query the number of active authors for each subreddit
create table Topx_subreddit
    sortkey(subreddit_id)
	as
	SELECT
	    subreddit_id,
	    date,
	    unique_authors   
	FROM
	    (SELECT
	        subreddit_id,
	        date,
	        unique_authors,
	        ROW_NUMBER() OVER (PARTITION           
	    BY
	        date           
	    ORDER BY
	        unique_authors DESC) rank           
	    FROM
	        (SELECT
	            subreddit_id,
	        LEFT(DATE(to_timestamp("time",'DD/MM/YYYY HH:MI:SS')),7) as date,
	        COUNT(distinct author_id) as unique_authors 
	        FROM
	            comment                   
	        GROUP BY
	            subreddit_id,
	            date ))                    
	    ORDER BY
	        date ASC,
	        unique_authors DESC;
     
     
create table subreddit_stage
sortkey(subreddit_id)
as 
select distinct * from 
subreddit;


create table active_subreddit as
select subreddit_stage.subreddit_id, subreddit,date,unique_authors
from subreddit_stage
right join Topx_subreddit
on subreddit_stage.subreddit_id = Topx_subreddit.subreddit_id;
