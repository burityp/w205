# Project 1: Query Project

- In the Query Project, you will get practice with SQL while learning about
  Google Cloud Platform (GCP) and BiqQuery. You'll answer business-driven
  questions using public datasets housed in GCP. To give you experience with
  different ways to use those datasets, you will use the web UI (BiqQuery) and
  the command-line tools, and work with them in Jupyter Notebooks.

#### Problem Statement

- You're a data scientist at Lyft Bay Wheels (https://www.lyft.com/bikes/bay-wheels), formerly known as Ford GoBike, the
  company running Bay Area Bikeshare. You are trying to increase ridership, and
  you want to offer deals through the mobile app to do so. 
  
- What deals do you offer though? Currently, your company has several options which can change over time.  Please visit the website to see the current offers and other marketing information. Frequent offers include: 
  * Single Ride 
  * Monthly Membership
  * Annual Membership
  * Bike Share for All
  * Access Pass
  * Corporate Membership
  * etc.

- Through this project, you will answer these questions: 

  * What are the 5 most popular trips that you would call "commuter trips"? 
  
  * What are your recommendations for offers (justify based on your findings)?

- Please note that there are no exact answers to the above questions, just like in the proverbial real world.  This is not a simple exercise where each question above will have a simple SQL query. It is an exercise in analytics over inexact and dirty data. 

- You won't find a column in a table labeled "commuter trip".  You will find you need to do quite a bit of data exploration using SQL queries to determine your own definition of a communter trip.  In data exploration process, you will find a lot of dirty data, that you will need to either clean or filter out. You will then write SQL queries to find the communter trips.

- Likewise to make your recommendations, you will need to do data exploration, cleaning or filtering dirty data, etc. to come up with the final queries that will give you the supporting data for your recommendations. You can make any recommendations regarding the offers, including, but not limited to: 
  * market offers differently to generate more revenue 
  * remove offers that are not working 
  * modify exising offers to generate more revenue
  * create new offers for hidden business opportunities you have found
  * etc. 

#### All Work MUST be done in the Google Cloud Platform (GCP) / The Majority of Work MUST be done using BigQuery SQL / Usage of Temporary Tables, Views, Pandas, Data Visualizations

A couple of the goals of w205 are for students to learn how to work in a cloud environment (such as GCP) and how to use SQL against a big data data platform (such as Google BigQuery).  In keeping with these goals, please do all of your work in GCP, and the majority of your analytics work using BigQuery SQL queries.

You can make intermediate temporary tables or views in your own dataset in BigQuery as you like.  Actually, this is a great way to work!  These make data exploration much easier.  It's much easier when you have made temporary tables or views with only clean data, filtered rows, filtered columns, new columns, summary data, etc.  If you use intermediate temporary tables or views, you should include the SQL used to create these, along with a brief note mentioning that you used the temporary table or view.

In the final Jupyter Notebook, the results of your BigQuery SQL will be read into Pandas, where you will use the skills you learned in the Python class to print formatted Pandas tables, simple data visualizations using Seaborn / Matplotlib, etc.  You can use Pandas for simple transformations, but please remember the bulk of work should be done using Google BigQuery SQL.

#### GitHub Procedures

In your Python class you used GitHub, with a single repo for all assignments, where you committed without doing a pull request.  In this class, we will try to mimic the real world more closely, so our procedures will be enhanced. 

Each project, including this one, will have it's own repo.

Important:  In w205, please never merge your assignment branch to the master branch. 

Using the git command line: clone down the repo, leave the master branch untouched, create an assignment branch, and move to that branch:
- Open a linux command line to your virtual machine and be sure you are logged in as jupyter.
- Create a ~/w205 directory if it does not already exist `mkdir ~/w205`
- Change directory into the ~/w205 directory `cd ~/w205`
- Clone down your repo `git clone <https url for your repo>`
- Change directory into the repo `cd <repo name>`
- Create an assignment branch `git branch assignment`
- Checkout the assignment branch `git checkout assignment`

The previous steps only need to be done once.  Once you your clone is on the assignment branch it will remain on that branch unless you checkout another branch.

The project workflow follows this pattern, which may be repeated as many times as needed.  In fact it's best to do this frequently as it saves your work into GitHub in case your virtual machine becomes corrupt:
- Make changes to existing files as needed.
- Add new files as needed
- Stage modified files `git add <filename>`
- Commit staged files `git commit -m "<meaningful comment about your changes>"`
- Push the commit on your assignment branch from your clone to GitHub `git push origin assignment`

Once you are done, go to the GitHub web interface and create a pull request comparing the assignment branch to the master branch.  Add your instructor, and only your instructor, as the reviewer.  The date and time stamp of the pull request is considered the submission time for late penalties. 

If you decide to make more changes after you have created a pull request, you can simply close the pull request (without merge!), make more changes, stage, commit, push, and create a final pull request when you are done.  Note that the last data and time stamp of the last pull request will be considered the submission time for late penalties.

---

## Parts 1, 2, 3

We have broken down this project into 3 parts, about 1 week's work each to help you stay on track.

**You will only turn in the project once  at the end of part 3!**

- In Part 1, we will query using the Google BigQuery GUI interface in the cloud.

- In Part 2, we will query using the Linux command line from our virtual machine in the cloud.

- In Part 3, we will query from a Jupyter Notebook in our virtual machine in the cloud, save the results into Pandas, and present a report enhanced by Pandas output tables and simple data visualizations using Seaborn / Matplotlib.

---

## Part 1 - Querying Data with BigQuery

### SQL Tutorial

Please go through this SQL tutorial to help you learn the basics of SQL to help you complete this project.

SQL tutorial: https://www.w3schools.com/sql/default.asp

### Google Cloud Helpful Links

Read: https://cloud.google.com/docs/overview/

BigQuery: https://cloud.google.com/bigquery/

Public Datasets: Bring up your Google BigQuery console, open the menu for the public datasets, and navigate to the the dataset san_francisco.

- The Bay Bike Share has two datasets: a static one and a dynamic one.  The static one covers an historic period of about 3 years.  The dynamic one updates every 10 minutes or so.  THE STATIC ONE IS THE ONE WE WILL USE IN CLASS AND IN THE PROJECT. The reason is that is much easier to learn SQL against a static target instead of a moving target.

- (USE THESE TABLES!) The static tables we will be using in this class are in the dataset **san_francisco** :

  * bikeshare_stations

  * bikeshare_status

  * bikeshare_trips

- The dynamic tables are found in the dataset **san_francisco_bikeshare**

### Some initial queries

Paste your SQL query and answer the question in a sentence.  Be sure you properly format your queries and results using markdown. 

- What's the size of this dataset? (i.e., how many trips)

  * Answer: 983,648
  * SQL query: `SELECT count(*) as total_trips FROM `bigquery-public-data.san_francisco.bikeshare_trips` `

- What is the earliest start date and time and latest end date and time for a trip?

  * Answer: The earliest start date and time is 2013-08-29 09:08:00 UTC and latest end date and time is 2016-08-31 23:48:00 UTC
  * SQL query: `SELECT min(start_date) as earliest_start_date, max(end_date) as latest_end_date FROM `bigquery-public-data.san_francisco.bikeshare_trips` `

- How many bikes are there? 

  * Answer: 700 
  * SQL query: `SELECT COUNT (DISTINCT bike_number) AS total_bikes FROM `bigquery-public-data.san_francisco.bikeshare_trips` `

### Questions of your own
- Make up 3 questions and answer them using the Bay Area Bike Share Trips Data.  These questions MUST be different than any of the questions and queries you ran above.

- Question 1: What is the most popular start station?
  * Answer: San Francisco Caltrain (Townsend at 4th) - 72683 started there
  * SQL query: 
  ```
    SELECT  start_station_name ,
    COUNT( start_station_name ) AS `trips_per_station` 
    FROM     `bigquery-public-data.san_francisco.bikeshare_trips`
    GROUP BY start_station_name
    ORDER BY `trips_per_station` DESC 
    LIMIT 1
    ```

- Question 2: What is the most popular end station?
  * Answer: San Francisco Caltrain (Townsend at 4th) - 92014 trips ended there
  * SQL query:
    ```
    SELECT  start_station_name ,
    COUNT( start_station_name ) AS `trips_per_station` 
    FROM     `bigquery-public-data.san_francisco.bikeshare_trips`
    GROUP BY start_station_name
    ORDER BY `trips_per_station` DESC 
    LIMIT 1
    ```


- Question 3: How trips are divided between subscribers and non-subscribers?
  * Answer: Out of the 983648 trips, 86.1% are by subscribers and 13.9% by non-subscribers.  
  * SQL query:

```
Select subscriber_type, (Count(subscriber_type)* 100 / (Select Count(*) From `bigquery-public-data.san_francisco.bikeshare_trips`)) as subscriber_type_share
From `bigquery-public-data.san_francisco.bikeshare_trips`
Group By subscriber_type
```

### Bonus activity queries (optional - not graded - just this section is optional, all other sections are required)

The bike share dynamic dataset offers multiple tables that can be joined to learn more interesting facts about the bike share business across all regions. These advanced queries are designed to challenge you to explore the other tables, using only the available metadata to create views that give you a broader understanding of the overall volumes across the regions(each region has multiple stations)

We can create a temporary table or view against the dynamic dataset to join to our static dataset.

Here is some SQL to pull the region_id and station_id from the dynamic dataset.  You can save the results of this query to a temporary table or view.  You can then join the static tables to this table or view to find the region:
```sql
#standardSQL
select distinct region_id, station_id
from `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info`
```

- Top 5 popular station pairs in each region

- Top 3 most popular regions(stations belong within 1 region)

- Total trips for each short station name in each region

- What are the top 10 used bikes in each of the top 3 region. these bikes could be in need of more frequent maintenance.

---

## Part 2 - Querying data from the BigQuery CLI 

- Use BQ from the Linux command line:

  * General query structure

    ```sql
    bq query --use_legacy_sql=false '
        SELECT count(*)
        FROM
           `bigquery-public-data.san_francisco.bikeshare_trips`'
    ```

### Queries

1. Rerun the first 3 queries from Part 1 using bq command line tool (Paste your bq
   queries and results here, using properly formatted markdown):

  * What's the size of this dataset? (i.e., how many trips): There are 983,648 entries in the dataset. 
  
    ```sql
    bq query --use_legacy_sql=false '
        SELECT count(*) as total_trips FROM `bigquery-public-data.san_francisco.bikeshare_trips`'
    ```

  * What is the earliest start time and latest end time for a trip? The earliest start date and time is 2013-08-29 09:08:00 UTC and latest end date and time is 2016-08-31 23:48:00 UTC
  
    ```sql
    bq query --use_legacy_sql=false '
    SELECT min(start_date) as earliest_start_date, max(end_date) as latest_end_date FROM `bigquery-public-data.san_francisco.bikeshare_trips`  '
    ```

  * How many bikes are there? There are 700 bikes.
  
    ```sql
    bq query --use_legacy_sql=false '
        SELECT COUNT (DISTINCT bike_number) AS total_bikes FROM `bigquery-public-data.san_francisco.bikeshare_trips` '
    ```


2. New Query (Run using bq and paste your SQL query and answer the question in a sentence, using properly formatted markdown):

  * How many trips are in the morning vs in the afternoon? There are 412,339 trips in the morning and 391,199 trips in the afternoon. This answer assumes that the morning ends at 12pm and the afternoon ends at 6pm, and that the time of the trip is when the trip starts.  
  
    ```sql
    bq query --use_legacy_sql=false '
        select count(*) as total,
       sum(case when EXTRACT(hour FROM start_date) < 12 then 1 else 0 end) as Morning,
       sum(case when EXTRACT(hour FROM start_date) >= 12 and EXTRACT(hour FROM start_date) < 18 then 1 else 0 end) as Afternoon,
       sum(case when EXTRACT(hour FROM start_date) >= 18 then 1 else 0 end) as Evening
       from `bigquery-public-data.san_francisco.bikeshare_trips` 
    ``` 


### Project Questions
Identify the main questions you'll need to answer to make recommendations (list
below, add as many questions as you need).

- Question 1: What is the share of total trips that occur in typical commuter hours?

- Question 2: What is the share of total trips that occur in typical commuter hours AND depart from train/ferry boat stations in the morning and end in these places at the end of the day?

- Question 3: What are the 5 most popular trips overall? 

- Question 4: What are the 3 most popular trips early in the morning (when commuters are heading to office/school)? 

- Question 5: What are the 3 most popular trips in the late afternoon and early evening (when commuters are returning home)? 

- ...

- Question n: 

### Answers

Answer at least 4 of the questions you identified above. You can use either
BigQuery or the bq command line tool.  Paste your questions, queries and
answers below.

- Question 1: 
  * Answer: 67% of total trips that occur in typical commuter hours. This answer assumes that commuters typically go to their workplace/school from 5AM to 10AM and return from 4PM to 8PM. 
  * SQL query: 
  
```sql
    select count(*) as total_trips,
       sum(case when (EXTRACT(hour FROM start_date) >= 5 and EXTRACT(hour FROM start_date) < 10) then 1 else 0 end)/(select count(*) from `bigquery-public-data.san_francisco.bikeshare_trips` ) as share_morning,
       sum(case when (EXTRACT(hour FROM start_date) >= 16 and EXTRACT(hour FROM start_date) < 20) then 1 else 0 end)/(select count(*) from `bigquery-public-data.san_francisco.bikeshare_trips` ) as share_evening
from `bigquery-public-data.san_francisco.bikeshare_trips` 

```

- Question 2: 
  * Answer: Rougthly 20%. This answer assumes that commuters typically get at train and ferry boat stations in the morning (from 5AM to 10AM) and take a bike to go to their workplace/school, and do the opposite at the end of the workday (from 4PM to 8PM). 
  * SQL query: 
  
```sql
    select count(*) as total,
       sum(case when (EXTRACT(hour FROM start_date) >= 5 and EXTRACT(hour FROM start_date) < 10) and (start_station_name = "San Francisco Caltrain (Townsend at 4th)" or start_station_name = "San Francisco Caltrain 2 (330 Townsend)" or start_station_name = "Harry Bridges Plaza (Ferry Building)") then 1 else 0 end)/(select count(*) from `bigquery-public-data.san_francisco.bikeshare_trips` ) as share_morning_start_trainstat,
       sum(case when (EXTRACT(hour FROM start_date) >= 16 and EXTRACT(hour FROM start_date) < 20) and (end_station_name = "San Francisco Caltrain (Townsend at 4th)" or end_station_name = "San Francisco Caltrain 2 (330 Townsend)" or end_station_name = "Harry Bridges Plaza (Ferry Building)") then 1 else 0 end)/(select count(*) from `bigquery-public-data.san_francisco.bikeshare_trips` ) as share_evening_end_trainstat
from `bigquery-public-data.san_francisco.bikeshare_trips` 
```


- Question 3: What are the 5 most popular trips? The most popular trips are: Harry Bridges Plaza (Ferry Building)
to Embarcadero at Sansome (0.93% of total trips), San Francisco Caltrain 2 (330 Townsend) to Townsend at 7th (0.86% of total trips), 2nd at Townsend to Harry Bridges Plaza (Ferry Building) (0.77% of total trips), Harry Bridges Plaza (Ferry Building) to 2nd at Townsend (0.93% of total trips) (0.70% of total trips), Embarcadero at Sansome
to Steuart at Market (0.93% of total trips) (0.70% of total trips). 

  * SQL query:
  
```sql
 SELECT COUNT(*) AS `num_of_trips`, COUNT(*)/(select count(*) from `bigquery-public-data.san_francisco.bikeshare_trips` )*100 AS `share`, start_station_name, end_station_name
   FROM `bigquery-public-data.san_francisco.bikeshare_trips`
   GROUP BY start_station_name, end_station_name
   ORDER BY `share` DESC
   LIMIT 5
```

- Question 4: What are the 3 most popular trips early in the morning (when commuters are heading to office/school)? 
  * Answer: The 3 most popular trips early in the morning are Harry Bridges Plaza (Ferry Building)
to 2nd at Townsend, San Francisco Caltrain (Townsend at 4th) to Temporary Transbay Terminal (Howard at Beale) and Steuart at Market to 2nd at Townsend
  * SQL query:
  
```sql
   SELECT start_station_name, end_station_name,
       sum(case when EXTRACT(hour FROM start_date) >= 5 and EXTRACT(hour FROM start_date) < 10 then 1 else 0 end)as going_trip,
       FROM `bigquery-public-data.san_francisco.bikeshare_trips`
       GROUP BY start_station_name, end_station_name
       ORDER BY `going_trip` DESC
       LIMIT 3  
```
  
- Question 5: What are the 3 most popular trips in the late afternoon and early evening (when commuters are returning home)? 
  * Answer: The 3 most popular trips in the late afternoon and early evening are 2nd at Townsend to 
Harry Bridges Plaza (Ferry Building), Embarcadero at Sansome to Steuart at Market and Embarcadero at Folsom to 
San Francisco Caltrain (Townsend at 4th). 

  * SQL query:
  
```sql
   SELECT start_station_name, end_station_name,
       sum(case when EXTRACT(hour FROM start_date) >= 16 and EXTRACT(hour FROM start_date) < 20 then 1 else 0 end) as returning_trip,
       FROM `bigquery-public-data.san_francisco.bikeshare_trips`
       GROUP BY start_station_name, end_station_name
       ORDER BY `returning_trip` DESC
       LIMIT 3  
```
  
- ...

- Question n:
  * Answer:
  * SQL query:

---

## Part 3 - Employ notebooks to synthesize query project results

### Get Going

Create a Jupyter Notebook against a Python 3 kernel named Project_1.ipynb in the assignment branch of your repo.

#### Run queries in the notebook 

At the end of this document is an example Jupyter Notebook you can take a look at and run.  

You can run queries using the "bang" command to shell out, such as this:

```
! bq query --use_legacy_sql=FALSE '<your-query-here>'
```

- NOTE: 
- Queries that return over 16K rows will not run this way, 
- Run groupbys etc in the bq web interface and save that as a table in BQ. 
- Max rows is defaulted to 100, use the command line parameter `--max_rows=1000000` to make it larger
- Query those tables the same way as in `example.ipynb`

Or you can use the magic commands, such as this:

```sql
%%bigquery my_panda_data_frame

select start_station_name, end_station_name
from `bigquery-public-data.san_francisco.bikeshare_trips`
where start_station_name <> end_station_name
limit 10
```

```python
my_panda_data_frame
```

#### Report in the form of the Jupter Notebook named Project_1.ipynb

- Using markdown cells, MUST definitively state and answer the two project questions:

  * What are the 5 most popular trips that you would call "commuter trips"? 
  
  * What are your recommendations for offers (justify based on your findings)?

- For any temporary tables (or views) that you created, include the SQL in markdown cells

- Use code cells for SQL you ran to load into Pandas, either using the !bq or the magic commands

- Use code cells to create Pandas formatted output tables (at least 3) to present or support your findings

- Use code cells to create simple data visualizations using Seaborn / Matplotlib (at least 2) to present or support your findings

### Resource: see example .ipynb file 

[Example Notebook](example.ipynb)

