--Temperature 
DROP VIEW IF EXISTS temp1;
CREATE VIEW temp1 AS (
WITH term_11 AS (
	SELECT 
	* 
	FROM data AS d
	JOIN metadata m ON m.id=d.meta_id 
	WHERE m.term_id=11
),
disc_device AS (
	SELECT DISTINCT device_id FROM term_11
),
t_means AS (
	SELECT
		device_id,
		(
			SELECT avg(value) AS "Tavg" 
			FROM term_11
			WHERE dd.device_id=term_11.device_id
		),
		(
			SELECT avg(value) AS "TD" 
			FROM term_11
		 	WHERE date_part('hour', tstamp)>= 6 AND date_part('hour', tstamp) < 18
		 	AND dd.device_id=term_11.device_id
		),
		 (
			 SELECT avg(value) AS "TN" 
			 FROM term_11
			 WHERE NOT (date_part('hour', tstamp)>= 6 AND date_part('hour', tstamp) < 18)
		 	 AND dd.device_id=term_11.device_id)
	FROM disc_device dd
)
SELECT 
	* , 
	abs(TD - TN) AS "T_ND"
FROM t_means
)

--Correaltion 
DROP VIEW IF EXISTS correlation_2;
CREATE VIEW correlation_2 AS (
WITH meta21 AS (
	SELECT 
		*, 
		(SELECT id FROM metadata ly WHERE term_id=9 ORDER BY st_distance(m.location, ly.location) ASC LIMIT 1) as close_meta20_id,
		(SELECT id FROM metadata ly WHERE term_id=7 ORDER BY st_distance(m.location, ly.location) ASC LIMIT 1) as close_meta19_id
	FROM metadata AS m
	WHERE term_id=11 AND sensor_id=1
),
data_norm AS (	
	SELECT
		row_number() OVER (PARTITION BY meta_id, variable_id ORDER BY tstamp ASC) 
		AS measurement_index,
		*,
		value - avg(value) OVER (PARTITION BY meta_id, variable_id) AS norm,
		avg(value) OVER (PARTITION BY meta_id, variable_id) AS group_avg	
	FROM data
),
indices AS (  
	SELECT 
		meta21.id, 								
		avg(d.value) AS "mean",					
		corr(d.norm, d20.norm) AS "Tcorr1Y",
		corr(d.norm, d19.norm) AS "Tcorr2Y"	
	FROM data_norm AS d													
	JOIN meta21 on meta21.id = d.meta_id		
	JOIN metadata AS m20 on meta21.close_meta20_id=m20.id
	JOIN metadata AS m19 on meta21.close_meta19_id=m19.id
	JOIN data_norm AS d20 on m20.id=d20.meta_id AND d.measurement_index=d20.measurement_index
	JOIN data_norm AS d19 on m19.id=d19.meta_id AND d.measurement_index=d19.measurement_index
	GROUP BY meta21.id
)
SELECT 
	m.id,
	m.device_id,
	mean, 
	"Tcorr1Y", 
	"Tcorr2Y"
FROM indices AS ind
JOIN metadata AS m ON ind.id=m.id
)

--END VIEW
DROP VIEW IF EXISTS temperature_indices;
CREATE VIEW temperature_indices AS (
SELECT 
	temp1.device_id,
	"Tavg", 
	"TD",
	"TN",
	"T_ND",
	cor2."Tcorr1Y",
	cor2."Tcorr2Y"
FROM temp1
FULL JOIN correlation_2 AS cor2 ON cor2.device_id=temp1.device_id
)
