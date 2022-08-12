-- General notes to keep in mind when using the guide
-- 1) Scripts provided are only samples that might include criteria from different segments. Double-check the dataest paths and make sure the columns and values are in the correct spelling (case-sensitive)
-- 2) Use the audience explorer function on the CIE tool as a reference point on how the dataset columns are spelled and categorized
-- 3) Substitute "database_name" with your own personal database created on Athena

------------- Creating, Extracting and Analysing Segments -------------

-- Creating a segment with lifestage datasets

CREATE OR REPLACE VIEW "database_name"."xxx" AS
SELECT DISTINCT (ifa) --SELECT COUNT DISTINCT (d.ifa) to find IFA count
FROM "da_datamart"."dse_lifestage"
WHERE "lifestage_name" = 'lifestage'
AND "country" = 'country'
AND dt IN ('date')


-- Creating a segment with affluence/age datasets

CREATE OR REPLACE VIEW "database_name"."xxx" AS
SELECT DISTINCT (ifa) --SELECT COUNT DISTINCT (d.ifa) to find IFA count
FROM "da_datamart"."da_datamart_static_full_(country)"
WHERE "age" = 'age_group'
AND/OR "final_affluence" = 'affluence'
AND dt IN ('date')


-- Creating a segment with app datasets

CREATE OR REPLACE VIEW "database_name"."xxx" AS
SELECT DISTINCT (ifa) --SELECT COUNT DISTINCT (d.ifa) to find IFA count
FROM "da_datamart"."da_datamart_app_(country)"
WHERE "l1_name" = 'l1_name'
AND dt IN ('date')
-- AND brq_count > xxx (Filters brq counts over/under a certain amount to filter frequent users of apps or visitors of POIs)


-- Creating a segment with POI datasets

CREATE OR REPLACE VIEW "database_name"."xxx" AS
SELECT DISTINCT (ifa) --SELECT COUNT DISTINCT (d.ifa) to find IFA count
FROM "da_datamart"."da_datamart_poi_(country)"
WHERE "l1_name" = 'l1_name'
AND month IN ('date')
-- AND brq_count > 200 (Filters brq counts over/under a certain amount to filter frequent users of apps or visitors of POIs)


-- Creating a segment with home location

CREATE OR REPLACE VIEW "database_name"."xxx" AS
SELECT DISTINCT (ifa) --SELECT COUNT DISTINCT (d.ifa) to find IFA count
FROM "ada_data"."home_location_country"
WHERE "home_state/etc" = 'state_name'
AND partition_0 IN ('date')

-- Note: Notice that the scripts above are conducting the same function, but the difference is in the datasets pulled, the names of the columns and what is filtered

-- Creating a segment with INNER JOINS (age, poi, lifestage & static datasets) 
-- Note: Remove the datasets you dont want and edit the filter subjects to those that would fit your segment criteria

--Step 1: Create a table/view to save your results on 
CREATE OR REPLACE VIEW "database_name"."xxx" AS

--Step 2: Select the relevant columns or results youd like to pull
SELECT DISTINCT (d.ifa) -- to pull list of unique IFAs. 'SELECT COUNT DISTINCT (d.ifa)' to find IFA count

--Step 3: Call upon the datasets youd like to use, join it with other datasets if needed
FROM "da_datamart"."da_datamart_app_(country)" as a
INNER JOIN "da_datamart"."da_datamart_poi_(country)" as b on a.ifa = b.ifa
INNER JOIN "da_datamart"."da_datamart_static_full_(country)" as c on b.ifa = c.ifa
INNER JOIN "da_datamart"."dse_lifestage" as d on c.ifa = d.ifa

--Step 4: Apply filters where needed
WHERE a.l1_name IN ('l1_name')
AND b.l1_name IN ('l1_name')
AND c.age IN ('age')
AND c.final_affluence IN ('affluence')
AND d.lifestage_name IN ('lifestage_name')
AND d.country IN ('MY')
AND a.dt IN ('date')
AND b.month IN ('date')
AND c.dt IN ('date')
AND d.dt IN ('date')


-- The script with all the correct values and filters should look like this. This script would pull high affluence, mid-aged IFAS with young kids
-- Note: Remove the datasets you dont want and edit the filter subjects to those that would fit your segment criteria

CREATE OR REPLACE VIEW "da_aqlif_2022"."sample_db" AS
SELECT DISTINCT (d.ifa)
FROM "da_datamart"."da_datamart_app_my" as a
INNER JOIN "da_datamart"."da_datamart_poi_my" as b on a.ifa = b.ifa
INNER JOIN "da_datamart"."da_datamart_static_full_my" as c on b.ifa = c.ifa
INNER JOIN "da_datamart"."dse_lifestage" as d on c.ifa = d.ifa
WHERE a.l1_name IN ('Call and Chat')
AND b.l1_name IN ('Place of Work')
AND c.age IN ('25-34')
AND c.final_affluence IN ('High','Ultra High')
AND d.lifestage_name IN ('Parents with Kids (0-6)')
AND d.country IN ('MY')
AND a.dt IN ('202207')
AND b.month IN ('202207')
AND c.dt IN ('202207')
AND d.dt IN ('202207')


-- FOR IFA EXTRACTIONS: click 'Download Results' for the list of IFAs in csv


-- FOR PERSONA BUILDS AND CI: GROUP BY function to analyze data

SELECT column_name, COUNT (DISTINCT ifa) AS ifa_count
FROM "dataset_name"."view_name"
GROUP BY column_name
ORDER BY ifa_count DESC

-- An example of how it would look like:
SELECT l1_name, COUNT (DISTINCT ifa) AS ifa_count
FROM "da_datamart"."da_datamart_app_sg"
GROUP BY l1_name
ORDER BY ifa_count DESC

# | l1_name | ifa_count
1	Games	2242613
2	Personal Productivity	1194073
3	Photo Video	1132451
4	Social App Accessories	1104985
5	Music	962414


------------- Geofencing -------------

--Step 1: Create a reference csv detailing the POI name, latitude, longitude and radius

--Step 2: Upload the csv onto Athena 

--Step 3: To see how many IFAs are within a certain radius of the POI, use the script below.

--Note: Check the 'from' functions and see if the script is pulling from the right database/file. Check also the filters to see if those are right

CREATE TABLE 
database_name.table_name AS
SELECT
    ifa,
    name, 
    local_datetime,
    distance,
    dt
    --partition_0,
    --count(distinct ifa)
    --distance,
    --CASE WHEN distance <= 0.5 THEN '500m'
         --when distance > 0.5 and distance <= 0.7 then '500m - 700m' ELSE '1000m' END AS distance_group,
FROM
(
    SELECT
    ST_DISTANCE(ST_POINT(a.longitude,a.latitude), ST_POINT(b.longitude,b.latitude))*111.139 AS distance,
    *
FROM
(
    SELECT
    DISTINCT
    'X' AS join_key,
    ifa,
    dt,
    cast(loc.latitude as double) as latitude,
    cast(loc.longitude as double) as longitude,
    date_add('hour', 7, cast(loc.first_seen as timestamp)) as local_datetime
    -- change the number to interval utc depending on the market u're geofencing
    FROM ada_data.monthly_agg_(country)
    cross join unnest(gps) as t(loc)
    where dt in ('xxx')
    --and loc.latitude <> 13.805399894714355 and loc.longitude <> 100.67510223388672
) A
INNER JOIN
(
SELECT
    'X' AS join_key,
    name,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude
    FROM database_name.geofence_reference_file --Taken from your POI csv. Table should contain at least 3 columns - Latitude, Logitude, POI name + any additional columns of info you'd like to add
) B
ON A.join_key = B.join_key
)
WHERE distance <= xxx;

--Step 4: With the output table from the above script, filter according to POI of choice and conduct analysis

--Step 4(a) - Extracting IFA from a particular poi
--create or replace view database_name.poi_1 as <- use this if a new view is needed
select distinct ifa
from database_name.table_name
where name = 'poi 1'

--Step 4(b) - Finding IFA counts from a particular POI
select count (distinct ifa)
from database_name.table_name
where name = 'poi 1'

--Step 4(c) - Filtering according to capture radius
select distinct ifa
from database_name.table_name
where distance < xxx

--Step 4(d) - Analyzing the POIs by joining it with xact datasets
--Note: Check the 'from' functions and see if the script is pulling from the right database/file. Check also the filters to see if those are right
select l1_name, count (distinct b.ifa) as ifa_count
from database_name.table_name as A
inner join da_datamart.da_datamart_app_(country) as b on a.ifa = b.ifa
where a.name = 'poi_1'
group by l1_name
order by ifa_count desc


------------- Device platform analysis -------------

--Creating a table unnesting the aggregate monthly dataset
--Note: if the request requires more columns for analysis, refer to the monthly_agg_(country) dataset to see which nested datapoints are required and add to the 'select' funciton using the format below

CREATE OR REPLACE VIEW "database_name"."device_dataset_xxx" AS 
SELECT DISTINCT
  ifa
, brq_count
, dt
, CAST(a.req_carrier_name AS varchar) carrier_name
, CAST(a.mm_con_type_desc AS varchar) mm_con_type
, CAST(a.req_con_type_desc AS varchar) req_con_type
, device.device_manufacturer
, device.device_name
, device.device_year_of_release
, device.major_os
FROM
  (("ada_data"."monthly_agg_(country)"
CROSS JOIN UNNEST(connection) t (a))
CROSS JOIN UNNEST(gps) t (b))
WHERE ((b.country = 'XX') AND (dt IN ('XXX')))

--From the output above, we can begin to look into various points of analysis ie. carrier brand share, connection type breakdown, phone brand share, popular phone models within a certian brand, popular operating systems etc.

--SAMPLE ANALYSIS:
-- Finding popular phone brands
select device_manufacturer, count (distinct ifa) as ifa_count
from "database_name"."device_dataset_xxx"
group by device_manufacturer
order by ifa_count desc

-- Finding popular operating systems
select device_manufacturer, count (distinct ifa) as ifa_count
from "database_name"."device_dataset_xxx"
group by device_manufacturer
order by ifa_count desc

--Finding popular phone brands in a certain state
select device_manufacturer, count (distinct ifa) as ifa_count
from "database_name"."device_dataset_xxx" as a
inner join "ada_data"."home_location_(country)"
where home_state = 'XXX'
group by device_manufacturer
order by ifa_count desc








------------- Sample Scripts -------------

-- Sample script to copy & edit

CREATE OR REPLACE VIEW "database_name"."view_name" AS
SELECT DISTINCT (d.ifa) -- to pull list of unique IFAs. 'SELECT COUNT DISTINCT (d.ifa)' to find IFA count
FROM "da_datamart"."da_datamart_app_(country)" as a
INNER JOIN "da_datamart"."da_datamart_poi_(country)" as b on a.ifa = b.ifa
INNER JOIN "da_datamart"."da_datamart_static_full_(country)" as c on b.ifa = c.ifa
INNER JOIN "da_datamart"."dse_lifestage" as d on c.ifa = d.ifa
WHERE a.l1_name IN ('Call and Chat')
AND b.l1_name IN ('Place of Work')
AND c.age IN ('25-34')
AND c.final_affluence IN ('High','Ultra High')
AND d.lifestage_name IN ('Parents with Kids (0-6)')
AND d.country IN ('MY')
AND a.dt IN ('date')
AND b.month IN ('date')
AND c.dt IN ('date')
AND d.dt IN ('date')

-- Datasets to copy & edit

FROM "da_datamart"."da_datamart_app_(country)" -- Replace (country) with country code ie. MY
FROM "da_datamart"."da_datamart_poi_(country)" -- Replace (country) with country code ie. MY
FROM "da_datamart"."da_datamart_static_full_(country)" -- Replace (country) with country code ie. MY
FROM "da_datamart"."dse_lifestage"
FROM "ada_data"."home_location_(country)" -- Replace (country) with country code ie. MY

-- Filters to copy & edit

WHERE l1_name = 'Education' --Draws upon the level 1 categorization from our app/poi datasets
WHERE l2_name = 'Private Insitiutions' --Draws upon the level 2 categorization from our app/poi datasets
WHERE l3_name = 'High School' --Draws upon the level 3 categorization from our app/poi datasets
WHERE l4_name = 'Cempaka High School' --Draws upon the level 4 categorization from our app/poi datasets
WHERE brq_count > xxx -- Filters brq counts over/under a certain amount to filter frequent users of apps or visitors of POIs
