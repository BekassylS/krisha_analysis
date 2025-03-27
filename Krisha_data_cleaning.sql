SELECT column_name, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'krisha'

CREATE TABLE krisha_copy 
(LIKE public.krisha INCLUDING ALL);

SELECT *
FROM krisha_copy

INSERT INTO krisha_copy
SELECT *
FROM public.krisha

SELECT *
FROM krisha

SELECT *
FROM krisha_copy

SELECT rooms, COUNT(*) count
FROM krisha_copy
GROUP BY rooms
ORDER BY count DESC

SELECT DISTINCT *
FROM krisha_copy

/* DISTINCT * and lower query provide the same table, with 10021 rows */

WITH duplicate_CTE AS (SELECT *, 
ROW_NUMBER() OVER(PARTITION BY listing_id, url, 
price, rooms, district, floor, building_type, 
complex_name, area, bathrooms, ceiling_height, year_built, parking) AS row_num
FROM krisha_copy)

SELECT *
FROM duplicate_CTE
WHERE row_num > 1

CREATE TABLE IF NOT EXISTS public.krisha_copy2
(listing_id INT,
url VARCHAR(50),
price INT,
rooms SMALLINT,
district VARCHAR(30),
floor VARCHAR(10),
building_type VARCHAR(30),
complex_name VARCHAR(50),
area NUMERIC,
bathrooms VARCHAR(20),
ceiling_height NUMERIC,
year_built NUMERIC,
parking VARCHAR(50)
)

INSERT INTO public.krisha_copy2
SELECT DISTINCT *
FROM krisha_copy

SELECT *
FROM krisha_copy2

SELECT COUNT(DISTINCT url)
FROM krisha_copy2

WITH duplicate_CTE AS (SELECT *, 
ROW_NUMBER() OVER(PARTITION BY listing_id, url) AS row_num
FROM krisha_copy2)

SELECT *
FROM duplicate_CTE
WHERE row_num > 1

WITH check_for_duplicates AS (SELECT *, LAG(rooms) OVER (PARTITION BY listing_id) AS rooms_prev
FROM krisha_copy2
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
GROUP BY listing_id
HAVING COUNT(*) > 2) 
ORDER BY listing_id)

SELECT *
FROM check_for_duplicates
WHERE rooms != rooms_prev

SELECT *
FROM krisha_copy2
WHERE listing_id IN (697094717, 762224409)
ORDER BY listing_id

/* After hand-checking the url, I decided to drop the Aruna City flat with 2 rooms, 
because in the original listing it says 1 room. */
DELETE FROM krisha_copy2 
WHERE rooms = 2 AND listing_id = 762224409

DELETE FROM krisha_copy2
WHERE listing_id = 762199431 AND rooms = 2 /* Deleted two rows with everything the same 
but rooms, 1 room is more realistic for 44 sqm than 1. */


WITH check_for_duplicates AS (SELECT *, LAG(price) OVER (PARTITION BY listing_id) AS price_prev
FROM krisha_copy2
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
GROUP BY listing_id
HAVING COUNT(*) > 1) 
ORDER BY listing_id)

SELECT listing_id, price, price_prev
FROM check_for_duplicates
WHERE price != price_prev  


SELECT listing_id, MIN(price) 
FROM krisha_copy2
WHERE listing_id != 697094717
GROUP BY listing_id
HAVING COUNT(*) > 1
ORDER BY MIN(price)


WITH ranked AS (SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY listing_id ORDER BY price) AS rank
FROM krisha_copy2)
WHERE rank > 1 AND listing_id != 697094717
ORDER BY price)

DELETE FROM krisha_copy2 
WHERE listing_id IN (
    SELECT listing_id FROM ranked WHERE rank > 1
)
 AND price IN (
    SELECT price FROM ranked WHERE rank > 1
);
/* Here, the rows with the higher prices for the same listing were deleted, 
the logic is that the lower price is the most likely the actual one 
since people relist their property if they can't sell it with lower price. */


/* Now, what is remaining are the same apartments but with just a little different info 
in some of the columns like area (51 and 50.6) and etc. Better to delete them by hand. */
SELECT *
FROM krisha_copy2
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
WHERE listing_id != 697094717
GROUP BY listing_id
HAVING COUNT(*) > 1)
ORDER BY listing_id

SELECT *
FROM krisha_copy2

SELECT DISTINCT *
FROM krisha_copy2

DELETE FROM krisha_copy2
WHERE listing_id = 762222081 AND area = 68.8

DELETE FROM krisha_copy2
WHERE listing_id = 690566614 AND floor = '7 из 8'

DELETE FROM krisha_copy2
WHERE listing_id = 760689843 AND floor = '5 из 9'

DELETE FROM krisha_copy2
WHERE listing_id = 761381601 AND area = 51.0

DELETE FROM krisha_copy2
WHERE listing_id = 761896524 AND parking IS NULL

DELETE FROM krisha_copy2
WHERE listing_id = 761902543 AND floor = '5 из 12'

DELETE FROM krisha_copy2
WHERE listing_id = 762200723 AND year_built = 2020
/**/

/* Usually flats with 2 rooms does not need 2 bathrooms but I know that 
in expensive complexes it is not always the case, they also tend to have bigger flats overall.
That is why i use following conditions in WHERE clause, basically i try 
to choose 2 bedroom flats from non-expensive complexes. And yes, if 1 bedroom apartment 
has more than 70 sqm in it, i am sure that it would have 2 bathrooms. */
SELECT *, price / area AS tenge_per_sqm
FROM krisha_copy2
WHERE bathrooms = 'совмещенный' AND rooms < 3 AND area < 70 AND price / area < 600000
ORDER BY area DESC

UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
WHERE bathrooms = 'совмещенный' AND rooms < 3 AND area < 70 AND price / area < 600000) 

UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
WHERE bathrooms = 'совмещенный' AND rooms < 3 AND area > 70 AND price / area > 600000) 

SELECT *
FROM krisha_copy2
WHERE rooms = 1 AND area >= 70
ORDER BY area DESC

/* As i mentioned before, 1 bedroom apartments with sqm > 70 tend to have 2 bathrooms. */
UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
WHERE rooms = 1 AND area >= 70) 


SELECT *
FROM krisha_copy2
WHERE rooms = 1 AND area < 70 AND bathrooms = 'раздельный' 
ORDER BY area DESC

UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
WHERE rooms = 1 AND area < 70 AND bathrooms = 'раздельный') 

SELECT *
FROM krisha_copy2
WHERE rooms = 1 AND area < 70 AND bathrooms = 'совмещенный' 
ORDER BY area DESC

UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
WHERE rooms = 1 AND area < 70 AND bathrooms = 'совмещенный') 

SELECT *
FROM krisha_copy2
WHERE rooms = 1 AND bathrooms IS NULL
ORDER BY area DESC

/* I dont sort here by area since all of them are less than 70 sqm. */
UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
WHERE rooms = 1 AND bathrooms IS NULL) 

/* I use LIKE here since I have saved the bathrooms column as string. */
SELECT *
FROM krisha_copy2
WHERE rooms = 1 AND bathrooms NOT LIKE '1' AND bathrooms NOT LIKE '2'

UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (SELECT listing_id
FROM krisha_copy2
WHERE rooms = 1 AND bathrooms NOT LIKE '1' AND bathrooms NOT LIKE '2')

/* To check for 'c/у и более' */
SELECT *
FROM krisha_copy2
WHERE rooms = 1 AND bathrooms NOT LIKE '1' AND bathrooms NOT LIKE '2'

SELECT *
FROM krisha_copy2
WHERE (bathrooms = 'раздельный' OR bathrooms = 'совмещенный') AND area < 60
ORDER BY area

SELECT *
FROM krisha_copy2
WHERE rooms = 2 AND bathrooms LIKE '2'

SELECT *
FROM krisha_copy2
WHERE rooms = 2 AND area >= 77 AND (bathrooms = 'раздельный' OR bathrooms = 'совмещенный')
ORDER BY area DESC

/* 77 sqm is a good split point, both checked by hand and by the assumption that more than 
this area for 2 bedrooms would be business+ complexes. */
UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 2 AND area < 77
AND (bathrooms = 'раздельный' OR bathrooms = 'совмещенный')
)

UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 2 AND area >= 77
AND (bathrooms = 'раздельный' OR bathrooms = 'совмещенный')
)

UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 2 AND bathrooms IS NULL AND area < 77
)

UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 2 AND bathrooms IS NULL AND area >= 77
)

SELECT *
FROM krisha_copy2
WHERE rooms = 2 AND bathrooms NOT LIKE '1' AND bathrooms NOT LIKE '2' AND area < 77

UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 2 AND bathrooms NOT LIKE '1' AND bathrooms NOT LIKE '2' AND area < 77
)

UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 2 AND bathrooms NOT LIKE '1' AND bathrooms NOT LIKE '2' AND area >= 77
)

SELECT *
FROM krisha_copy2
WHERE rooms = 2 AND bathrooms NOT IN ('1', '2')

/* Now, let's deal with 3 bedroom apartments. */

SELECT *
FROM krisha_copy2
WHERE rooms = 3 AND area > 70 AND bathrooms LIKE '1'
ORDER BY area

SELECT DISTINCT bathrooms
FROM krisha_copy2
WHERE rooms = 3

SELECT *
FROM krisha_copy2
WHERE rooms = 3 AND bathrooms LIKE '2 с/у%'
ORDER BY area DESC

SELECT *
FROM krisha_copy2
WHERE rooms = 3 AND bathrooms IN ('совмещенный', 'раздельный', NULL) AND area > 75
ORDER BY area DESC

UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 3 AND bathrooms LIKE '2 с/у%'
)

UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 3 AND bathrooms IN ('совмещенный', 'раздельный', NULL) AND area > 75
)

UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 3 AND bathrooms IN ('совмещенный', 'раздельный', NULL) AND area <= 75
)

UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 3 AND bathrooms IS NULL AND area > 75
)

UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 3 AND bathrooms IS NULL AND area <= 75
)

SELECT DISTINCT bathrooms
FROM krisha_copy2
WHERE rooms = 3

/* Now, let's deal with 4+ bedroom apartments. */

SELECT *
FROM krisha_copy2
WHERE rooms = 4 AND area < 130 AND area >= 80
ORDER BY area DESC

UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2 
WHERE rooms = 4 AND area < 80 AND bathrooms LIKE '2 с/у%'
)

UPDATE krisha_copy2
SET bathrooms = 1
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2 
WHERE rooms = 4 AND area < 80 AND (bathrooms NOT LIKE '2' OR bathrooms IS NULL)
)

UPDATE krisha_copy2
SET bathrooms = 2
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE rooms = 4 AND area < 130 AND area >= 80
)

/* Now I decided to stop cleaning the bathroom column since starting from 130 sqm it becomes
hard to understand whether the flat has 2 or more bathrooms since we have huge apartments 
with bathroom in each room, so I would leave everything as it is. */

SELECT *
FROM krisha_copy2
WHERE bathrooms NOT IN ('1', '2')
ORDER BY area DESC

/* It is also better to make them NULL since I want to change the column data type from string
to integer. */

UPDATE krisha_copy2
SET bathrooms = NULL
WHERE listing_id IN (
SELECT listing_id
FROM krisha_copy2
WHERE bathrooms NOT IN ('1', '2')
)

SELECT *
FROM krisha_copy2
WHERE bathrooms IS NULL
ORDER BY area DESC

ALTER TABLE krisha_copy2
ALTER COLUMN bathrooms TYPE INT USING bathrooms::INTEGER

SELECT AVG(bathrooms)
FROM krisha_copy2

/* Let's also fix the year_built column, since we dont need .0 in the values. */

SELECT LEFT(year_built::VARCHAR(7), 4)
FROM krisha_copy2

ALTER TABLE krisha_copy2
ALTER COLUMN year_built TYPE INT USING LEFT(year_built::VARCHAR(7), 4)::INTEGER

SELECT *
FROM krisha_copy2

SELECT COUNT(*)
FROM krisha_copy2
WHERE parking LIKE 'рядом%'

