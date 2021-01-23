--Creation of the correaltion VIEW for all Hobos
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
		corr(d.norm, d20.norm) AS "Tcorr1Y"	,
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
