CREATE SCHEMA Bike_share_dataset ;
SET search_path TO Bike_share_dataset ;

DROP TABLE bike_share
CREATE TABLE bike_share( ride_id             VARCHAR(25)
						,rideable_type       VARCHAR(20)
						,started_at          timestamp 
						,ended_at            timestamp 
						,start_station_name  VARCHAR(60)
						,start_station_id    VARCHAR(50)
						,end_station_name    VARCHAR(60) 
						,end_station_id      VARCHAR(50)
						,start_lat           VARCHAR(30)
						,start_lng           VARCHAR(30)  
						,end_lat             VARCHAR(30)
						,end_lng             VARCHAR(30) 
						,member_casual        VARCHAR(30)) ;

ALTER TABLE bike_share ALTER COLUMN start_station_name TYPE VARCHAR(100)
ALTER TABLE bike_share ALTER COLUMN end_station_name TYPE VARCHAR(100)

SELECT min(started_at) FROM bike_share






----------------------------------------------------------------------- Cleaning the data-------------------------------------------------------------------------------------- 

		--UPLOADING DATA TO DATABASE : Cycle ride data from 2022 April till 2023 March has been uploaded which comprises of 13 columns and 5803720 rows of data 
		SELECT * FROM bike_share


		--Dealing with null values 
		SELECT * 
		FROM bike_share
		WHERE start_station_name IS NULL OR end_station_name IS NULL ;
		/*There are 1321112 rows which doesnt contain either start_station_name or end_station_name or both .As this data is incomplete and will effect the final analysis because 
		it will consider all the trips for which we wont have any record of starting and final destination . So we will drop all the rows which contain any kind of null value .*/

		DELETE FROM bike_share WHERE start_station_name IS NULL OR end_station_name IS NULL;


		--adding weekday column to the table and updating it 
		ALTER TABLE bike_share ADD start_week_day VARCHAR(10) ;
		UPDATE bike_share
		SET start_week_day = To_Char("started_at", 'DY')

		--Adding trip_start hour column and updating it 
		ALTER TABLE bike_share ADD started_at_hour VARCHAR(10) ;
		UPDATE bike_share 
		SET started_at_hour=EXTRACT(HOUR FROM  started_at)


		--Adding trip_duration column and updating it 
		ALTER TABLE bike_share ADD trip_time interval ;
		UPDATE bike_share 
		SET trip_time=(ended_at-started_at)

		ALTER TABLE bike_share DROP column trip_time_seconds;
		ALTER TABLE bike_share ADD trip_time_seconds numeric ;

		UPDATE bike_share 
		SET trip_time_seconds= EXTRACT ( epoch FROM (trip_time));

		UPDATE bike_share 
		SET trip_time_seconds= ROUND(trip_time_seconds,0);



------------------------------------------------------------------------ANALYSIS OF DATASET--------------------------------------------------------------------------------------

-------------------------------------------------------------Riding pattern of Casual and member customers-------------------------------------------------------------------
		--Question What is the Share of member rides and casual rides .
		SELECT  member_casual ,ROUND((count(ride_id)*100.0/(SELECT COUNT(ride_id) FROM bike_share )),2) as percent_share
		FROM bike_share
		GROUP BY member_casual
		--Question No of trips grouped on week_day and wether it was member or casual ride.
		SELECT start_week_day , member_casual ,count(ride_id)
		FROM bike_share
		GROUP BY start_week_day , member_casual

		--Question On each day at what time most of the trips werre started
		SELECT start_week_day,CAST(started_at_hour AS int),count(ride_id)
		FROM bike_share
		GROUP BY start_week_day,CAST(started_at_hour AS int)
		ORDER BY start_week_day,CAST(started_at_hour AS int)
		--Question n each day at what time most of the trips were started for casual user
		SELECT start_week_day,CAST(started_at_hour AS int),count(ride_id)
		FROM bike_share
		WHERE member_casual='casual'
		GROUP BY start_week_day,CAST(started_at_hour AS int)
		ORDER BY start_week_day,CAST(started_at_hour AS int)

		--Question On each day at what time most of the trips werre started for members users
		SELECT start_week_day,CAST(started_at_hour AS int),count(ride_id)
		FROM bike_share
		WHERE member_casual='member'
		GROUP BY start_week_day,CAST(started_at_hour AS int)
		ORDER BY start_week_day,CAST(started_at_hour AS int)

		--Question Find the hourly usage of bike by casual and member customers
		SELECT CAST(started_at_hour AS int),member_casual,count(ride_id) 
		FROM bike_share
		GROUP BY started_at_hour,member_casual
		ORDER BY CAST(started_at_hour AS int)


		--Question No of monthly trips by each type of rider.
		SELECT TO_CHAR(started_at , 'Month') AS start_at_month,member_casual,count(ride_id)
		FROM bike_share
		GROUP BY TO_CHAR(started_at , 'Month'),member_casual

		--Question start_of_trip pattern for trips on weekdays and weekends .

		WITH CTE AS 
				(SELECT * , 
				CASE WHEN start_week_day='SAT' OR start_week_day='SUN' THEN 'Weekend'
					 ELSE 'Weekday' END AS day
				FROM bike_share)	
		SELECT day,CAST(started_at_hour AS int),count(ride_id)
		FROM CTE 
		GROUP BY day,CAST(started_at_hour AS int)

		--Question Rides by hour of the day on weekends and weekdays with different type of customer
		WITH CTE AS 
				(SELECT * , 
				CASE WHEN start_week_day='SAT' OR start_week_day='SUN' THEN 'Weekend'
					 ELSE 'Weekday' END AS day
				FROM bike_share)
		SELECT member_casual,day,CAST(started_at_hour AS int),count(ride_id)
		FROM CTE 
		GROUP BY member_casual,day,CAST(started_at_hour AS int)







-------------------------------------------------------------Preference of rideables by customers-------------------------------------------------------------------

		--Question Check how different kind of bikes are being used 
		SELECT rideable_type ,count(ride_id) as no_of_rides
		FROM bike_share
		GROUP BY rideable_type
		--Question Which customer is using which kind of bikes more

		SELECT rideable_type, member_casual ,count(ride_id) as no_of_rides
		FROM bike_share
		GROUP BY rideable_type ,member_casual

		SELECT start_week_day , rideable_type ,count(ride_id) as no_of_rides
		FROM bike_share
		where member_casual='member'
		GROUP BY start_week_day , rideable_type

		SELECT start_week_day , rideable_type ,count(ride_id) as no_of_rides
		FROM bike_share
		where member_casual='casual'
		GROUP BY start_week_day , rideable_type


		SELECT rideable_type, TO_CHAR(started_at , 'Mon') AS start_at_month ,count(ride_id) as no_of_rides
		FROM bike_share
		where member_casual='member'
		GROUP BY rideable_type ,TO_CHAR(started_at , 'Mon')

		SELECT rideable_type,TO_CHAR(started_at , 'Mon') AS start_at_month ,count(ride_id) as no_of_rides
		FROM bike_share
		where member_casual='casual'
		GROUP BY rideable_type ,TO_CHAR(started_at , 'Mon')











----------------------------------------------------------------Analysing the trip durations of different customers ---------------------------------------------------------

		--Question Find the average time for trips .
		SELECT  ROUND((avg(trip_time_seconds)/60),2) mean_trip_duration_mins
		FROM bike_share
		--Question Find the average time for trips for members and casual
		SELECT member_casual , ROUND((avg(trip_time_seconds)/60),2) mean_trip_duration_mins
		FROM bike_share
		GROUP BY member_casual
		--Question Find the average time for trips for members and casual month wise 
		SELECT member_casual,TO_CHAR(started_at , 'Mon') AS start_at_month , ROUND((avg(trip_time_seconds)/60),2) mean_trip_duration_mins
		FROM bike_share
		GROUP BY member_casual , TO_CHAR(started_at , 'Mon')
		--Question Find the average time for trips for members and casual day wise
		SELECT member_casual,start_week_day , ROUND((avg(trip_time_seconds)/60),2) mean_trip_duration_mins
		FROM bike_share
		GROUP BY member_casual , start_week_day

		--Question Find the average time for trips for members and casual month wise
		SELECT member_casual,start_week_day , ROUND((avg(trip_time_seconds)/60),2) mean_trip_duration_mins
		FROM bike_share
		GROUP BY member_casual , start_week_day
		
		--Max trip durations member and casual customers .
        SELECT  member_casual,ROUND((MAX(trip_time_seconds)/3600),2) max_trip_duration_hours
		FROM bike_share
		GROUP BY member_casual






--------------------------------------------------------------------Analysing the most active stations and routes -----------------------------------------------------------
		--Question Most active route 
			SELECT start_station_name ,end_station_name ,count(ride_id) as no_of_trips
			FROM bike_share
			GROUP BY  start_station_name ,end_station_name
			ORDER BY count(ride_id) desc
			--we can see there are pairs of stations with trips both way and trips whose start and end location were same 

			--total no of trips between any two stations bothway for both kind of customer 
			WITH total_trips AS
					(SELECT start_station_name ,end_station_name ,count(ride_id) as no_of_trips
					FROM bike_share
					GROUP BY  start_station_name ,end_station_name
					ORDER BY count(ride_id) desc),
				trips_with_same_start_end AS
					(SELECT * 
					FROM total_trips
					WHERE start_station_name=end_station_name),
				trips_with_diff_start_end AS
					(SELECT * , row_number()over()as rn
					FROM total_trips
					WHERE start_station_name<>end_station_name),	
				trips_with_diff_start_end_final AS
					(SELECT start_station_name_t1,end_station_name_t1, 
					CASE WHEN no_of_trips_t2 IS NOT NULL THEN (no_of_trips_t1+no_of_trips_t2)
						 ELSE no_of_trips_t1 END AS total_trips_between_two_point
					FROM(SELECT t1.start_station_name as start_station_name_t1  ,t1.end_station_name as end_station_name_t1,t1.no_of_trips no_of_trips_t1,t1.rn as rn_t1,
						 t2.start_station_name,t2.end_station_name,t2.no_of_trips as  no_of_trips_t2,t2.rn as rn_t2 
					FROM trips_with_diff_start_end t1 
					LEFT JOIN trips_with_diff_start_end t2 ON t1.start_station_name=t2.end_station_name AND t2.start_station_name=t1.end_station_name
					WHERE t1.rn>t2.rn OR t2.rn IS NULL)x)
			(SELECT * FROM trips_with_same_start_end)
			UNION ALL
			(SELECT *FROM trips_with_diff_start_end_final )
			ORDER BY no_of_trips desc

			--total no of trips between any two stations bothway for member customer 
			WITH total_trips AS
					(SELECT start_station_name ,end_station_name,count(ride_id) as no_of_trips 
					FROM bike_share
					 WHERE member_casual='member'
					GROUP BY  start_station_name ,end_station_name
					ORDER BY count(ride_id) desc),
				trips_with_same_start_end AS
					(SELECT * 
					FROM total_trips
					WHERE start_station_name=end_station_name)  ,
				trips_with_diff_start_end AS
					(SELECT * , row_number()over()as rn
					FROM total_trips
					WHERE start_station_name<>end_station_name),	
				trips_with_diff_start_end_final AS
					(SELECT start_station_name_t1,end_station_name_t1,
					CASE WHEN no_of_trips_t2 IS NOT NULL THEN (no_of_trips_t1+no_of_trips_t2)
						 ELSE no_of_trips_t1 END AS total_trips_between_two_point
					FROM(SELECT t1.start_station_name as start_station_name_t1,t1.end_station_name as end_station_name_t1,t1.no_of_trips no_of_trips_t1,t1.rn as rn_t1,
						 t2.start_station_name,t2.end_station_name,t2.no_of_trips as  no_of_trips_t2,t2.rn as rn_t2 
					FROM trips_with_diff_start_end t1 
					LEFT JOIN trips_with_diff_start_end t2 ON t1.start_station_name=t2.end_station_name AND t2.start_station_name=t1.end_station_name
					WHERE t1.rn>t2.rn OR t2.rn IS NULL)x)
			(SELECT * FROM trips_with_same_start_end)
			UNION 
			(SELECT * FROM trips_with_diff_start_end_final )
			ORDER BY no_of_trips desc



			--total no of trips between any two stations bothway for casual customer 
			WITH total_trips AS
					(SELECT start_station_name ,end_station_name ,count(ride_id) as no_of_trips
					FROM bike_share
					 WHERE member_casual='casual'
					GROUP BY  start_station_name ,end_station_name
					ORDER BY count(ride_id) desc),
				trips_with_same_start_end AS
					(SELECT * 
					FROM total_trips
					WHERE start_station_name=end_station_name),
				trips_with_diff_start_end AS
					(SELECT * , row_number()over()as rn
					FROM total_trips
					WHERE start_station_name<>end_station_name),	
				trips_with_diff_start_end_final AS
					(SELECT start_station_name_t1,end_station_name_t1, 
					CASE WHEN no_of_trips_t2 IS NOT NULL THEN (no_of_trips_t1+no_of_trips_t2)
						 ELSE no_of_trips_t1 END AS total_trips_between_two_point
					FROM(SELECT t1.start_station_name as start_station_name_t1  ,t1.end_station_name as end_station_name_t1,t1.no_of_trips no_of_trips_t1,t1.rn as rn_t1,
						 t2.start_station_name,t2.end_station_name,t2.no_of_trips as  no_of_trips_t2,t2.rn as rn_t2 
					FROM trips_with_diff_start_end t1 
					LEFT JOIN trips_with_diff_start_end t2 ON t1.start_station_name=t2.end_station_name AND t2.start_station_name=t1.end_station_name
					WHERE t1.rn>t2.rn OR t2.rn IS NULL)x)
			(SELECT * FROM trips_with_same_start_end)
			UNION ALL
			(SELECT *FROM trips_with_diff_start_end_final )
			ORDER BY no_of_trips desc


			--Most active starting point  
			SELECT start_station_name ,count(ride_id) as no_of_trips
			FROM bike_share
			GROUP BY  start_station_name 
			ORDER BY count(ride_id) desc

			--Most active ending point
			SELECT end_station_name ,count(ride_id) as no_of_trips
			FROM bike_share
			GROUP BY  end_station_name
			ORDER BY count(ride_id) desc

			--Data for Tableau
			SELECT start_station_name,start_lat,start_lng ,end_station_name,end_lat,end_lng ,count(ride_id) as no_of_trips
			FROM bike_share
			WHERE member_casual='member'
			GROUP BY  start_station_name,start_lat,start_lng ,end_station_name,end_lat,end_lng
			having count(ride_id)>1
			ORDER BY count(ride_id) desc

			SELECT start_station_name,start_lat,start_lng ,end_station_name,end_lat,end_lng ,count(ride_id) as no_of_trips
			FROM bike_share
			WHERE member_casual='casual'
			GROUP BY  start_station_name,start_lat,start_lng ,end_station_name,end_lat,end_lng
			having count(ride_id)>1
			ORDER BY count(ride_id) desc







