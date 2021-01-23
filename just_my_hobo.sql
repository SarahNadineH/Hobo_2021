--Temperature indices for my hobo with device_id='10347367'
DROP VIEW IF EXISTS temperature;
CREATE VIEW temperature AS(
WITH term_11 AS (
	SELECT 
		* 
	FROM data AS d
	JOIN metadata AS m ON m.id=d.meta_id 
	WHERE m.term_id=11
	AND m.device_id='10347367')
,
disc_device AS (
	SELECT DISTINCT device_id 
	FROM term_11
),
t_means AS (
	SELECT
		device_id,
		(
			SELECT 
			avg(value) AS mean_temp 
			FROM term_11
			WHERE dd.device_id=term_11.device_id),
		(
			SELECT 
			avg(value) AS day_mean 
			FROM term_11
			WHERE date_part('hour', tstamp)>= 6 AND date_part('hour', tstamp) < 18
			AND dd.device_id=term_11.device_id),
		(
			SELECT avg(value) AS night_mean 
			FROM term_11
			WHERE NOT (date_part('hour', tstamp)>= 6 AND date_part('hour', tstamp) < 18)
			AND dd.device_id=term_11.device_id)
	FROM disc_device dd
)
SELECT 
	* , 
	abs(day_mean - night_mean) AS t_ND
FROM t_means)


--View for correlation indices for my hobo with device_id='10347367'
DROP VIEW IF EXISTS correaltion;
CREATE VIEW correaltion AS (
WITH ids AS (
	SELECT 
		id AS my_hobo_id ,
		(
			SELECT id 
			FROM metadata AS m20 
			WHERE term_id= 9 
			ORDER BY st_distance(m21.location, m20.location) 
			LIMIT 1 
		) AS close_hobo_2020_id ,
		(
			SELECT id 
			FROM metadata AS m19 
			WHERE term_id= 7 
			ORDER BY st_distance(m21.location, m19.location) 
			LIMIT 1 
		) AS close_hobo_2019_id
	FROM metadata AS m21
	WHERE term_id=11
	AND device_id='10347367'
),
data_2019 AS (
	SELECT 
		tstamp, 
		value
	FROM data
	WHERE meta_id=(SELECT close_hobo_2019_id FROM ids)
),
norm_2019 AS(
	SELECT 
		value - (SELECT avg(value) FROM data_2019) AS norm_value,
		row_number() OVER (ORDER BY (SELECT NULL))-1 AS norm_hr
	FROM data_2019
),
data_2020 AS (
	SELECT 
		tstamp, 
		value
	FROM data
	WHERE meta_id=(SELECT close_hobo_2020_id FROM ids)
),
norm_2020 AS(
	SELECT 
		value - (SELECT avg(value) FROM data_2020) AS norm_value,
		row_number() OVER (ORDER BY (SELECT NULL))-1 AS norm_hr
	FROM data_2020
),
data_2021 AS (
	SELECT 
		tstamp, 
		value
	FROM data
	WHERE meta_id=(SELECT my_hobo_id FROM ids)
),
norm_2021 AS(
	SELECT 
		value - (SELECT avg(value) FROM data_2021) AS norm_value,
		row_number() OVER (ORDER BY (SELECT NULL))-1 AS norm_hr
	FROM data_2021
), 
values_20_21 AS(
	SELECT 
		n20.norm_value AS value_20,
		n21.norm_value AS value_21
	FROM norm_2020 AS n20
	JOIN norm_2021 AS n21 ON n21.norm_hr=n20.norm_hr
),
values_19_21 AS(
	SELECT 
		n19.norm_value AS value_19,
		n21.norm_value AS value_21
	FROM norm_2019 AS n19
	JOIN norm_2021 AS n21 ON n21.norm_hr=n19.norm_hr
),
corr_20_21 AS(
	SELECT 
		CORR(value_20,value_21) AS Tcorr1Y
	FROM values_20_21
),
corr_19_21 AS(
	SELECT 
		CORR(value_19,value_21) AS Tcorr2Y
	FROM values_19_21
)
SELECT 
	 *
	FROM  corr_20_21
CROSS JOIN corr_19_21
)

--Final view of all indices
DROP VIEW IF EXISTS temperature_indices_1;
CREATE VIEW temperature_indices_1 AS (
SELECT 
*
FROM temperature
CROSS JOIN correaltion
)
