-- 1. Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
SELECT
	PERCENTILE_DISC(0.99) WITHIN GROUP (
ORDER BY
	total_area) AS total_area_limit,
	PERCENTILE_DISC(0.99) WITHIN GROUP (
ORDER BY
	rooms) AS rooms_limit,
	PERCENTILE_DISC(0.99) WITHIN GROUP (
ORDER BY
	balcony) AS balcony_limit,
	PERCENTILE_DISC(0.99) WITHIN GROUP (
ORDER BY
	ceiling_height) AS ceiling_height_limit_h,
	PERCENTILE_DISC(0.01) WITHIN GROUP (
ORDER BY
	ceiling_height) AS ceiling_height_limit_l
FROM
	real_estate.flats     
),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    ),
-- Выведем объявления без выбросов и добавим категории:
adding_category AS (
SELECT
	*,
	CASE
		WHEN days_exposition <= 30 THEN 'до 1 месяца'
		WHEN days_exposition <= 90 THEN 'до 3 месяцев'
		WHEN days_exposition <= 180 THEN 'до полугода'
		WHEN days_exposition IS NULL THEN 'незакрытые объявления'
		ELSE 'дольше полугода'
	END AS ads_exposition,
	CASE
		WHEN city_id IN (
		SELECT
			city_id
		FROM
			real_estate.city
		WHERE
			city = 'Санкт-Петербург') THEN 'Санкт-Петербург'
		ELSE 'Города ЛО'
	END AS region
FROM
	real_estate.flats
LEFT JOIN real_estate.advertisement
		USING(id)
LEFT JOIN real_estate.type
		USING (type_id) INNER JOIN filtered_id USING(id)
WHERE TYPE = 'город'),
-- Посчитаем общее кол-во объявлений в регионах:
total_ads_in_region AS(
SELECT region, count(id) AS total_ads
FROM adding_category
GROUP BY region)
-- Посчитаем средние показатели для сегментов:
SELECT
	region,
	ads_exposition,
	count(id) AS ads_number,
	round(count(*) / sum(count(*)) over(),3) AS share_total,
    round(count(*) / sum(count(*)) over(PARTITION BY region),3) AS share_region,
	round(avg(last_price / total_area)::NUMERIC, 3) AS avg_price_p_m2,
	round(avg(total_area::NUMERIC), 3) AS avg_tot_area,
	round(avg(kitchen_area::NUMERIC), 3) AS avg_kitchen_area,
	percentile_disc(0.5) WITHIN GROUP (
ORDER BY
	rooms) AS mid_rooms_number,
	percentile_disc(0.5) WITHIN GROUP (
ORDER BY
	balcony) AS mid_balcony_number,
	percentile_disc(0.5) WITHIN GROUP (
ORDER BY
	floor) AS mid_floor_number,
	percentile_disc(0.5) WITHIN GROUP (
ORDER BY
	parks_around3000) AS mid_parks_number
FROM
	adding_category
LEFT JOIN total_ads_in_region
		USING(region)
GROUP BY
	region,
	ads_exposition,
	total_ads
ORDER BY
	region DESC,
	ads_exposition;
-- 2. Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
SELECT
	PERCENTILE_DISC(0.99) WITHIN GROUP (
ORDER BY
	total_area) AS total_area_limit,
	PERCENTILE_DISC(0.99) WITHIN GROUP (
ORDER BY
	rooms) AS rooms_limit,
	PERCENTILE_DISC(0.99) WITHIN GROUP (
ORDER BY
	balcony) AS balcony_limit,
	PERCENTILE_DISC(0.99) WITHIN GROUP (
ORDER BY
	ceiling_height) AS ceiling_height_limit_h,
	PERCENTILE_DISC(0.01) WITHIN GROUP (
ORDER BY
	ceiling_height) AS ceiling_height_limit_l
FROM
	real_estate.flats     
),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
SELECT
	id
FROM
	real_estate.flats LEFT JOIN real_estate.advertisement USING(id) LEFT JOIN real_estate.TYPE using(type_id)
WHERE
	total_area < (
	SELECT
		total_area_limit
	FROM
		limits)
	AND (rooms < (
	SELECT
		rooms_limit
	FROM
		limits)
	OR rooms IS NULL)
	AND (balcony < (
	SELECT
		balcony_limit
	FROM
		limits)
	OR balcony IS NULL)
	AND ((ceiling_height < (
	SELECT
		ceiling_height_limit_h
	FROM
		limits)
	AND ceiling_height > (
	SELECT
		ceiling_height_limit_l
	FROM
		limits))
	OR ceiling_height IS NULL) AND EXTRACT(YEAR FROM first_day_exposition) IN (2015, 2016, 2017, 2018)
	AND TYPE='город'
    ),
-- Посчитаем сезонность для старта продаж:
sales_start_seasonality AS(
SELECT
	to_char(first_day_exposition, 'month') AS months,
	count(id) AS ads_number,
	round(avg(total_area::NUMERIC), 3) AS avg_total_area_start, 
	round(avg(last_price / total_area)::NUMERIC, 3) AS price_per_m2_start
FROM
	real_estate.flats
LEFT JOIN real_estate.advertisement USING(id) INNER JOIN filtered_id
		USING(id)
GROUP BY
	months),
    sales_end_seasonality AS(
SELECT to_char(first_day_exposition+ days_exposition::integer, 'month') AS months,
count(id) AS ads_number,
round(avg(total_area::NUMERIC), 3) AS avg_total_area_end, 
round(avg(last_price / total_area)::NUMERIC, 3) AS price_per_m2_end
FROM
	real_estate.flats
LEFT JOIN real_estate.advertisement
		USING(id) INNER JOIN filtered_id using(id)
WHERE
	days_exposition IS NOT NULL
GROUP BY
	months)
-- Расчитаем среднюю площадь недвижимости и цену по месяцам:
	SELECT
	months,
	sales_start_seasonality.ads_number AS ads_start_number,
	sales_start_seasonality.ads_number/sum(sales_start_seasonality.ads_number) over() AS ads_start_share,
	RANK() OVER(ORDER BY sales_start_seasonality.ads_number DESC) AS month_start_rank,
	sales_end_seasonality.ads_number AS ads_end_number,
	sales_end_seasonality.ads_number/sum(sales_end_seasonality.ads_number) over() AS ads_end_share,
	RANK() OVER(ORDER BY sales_end_seasonality.ads_number DESC) AS month_end_rank,
	avg_total_area_start,
	RANK() OVER(ORDER BY avg_total_area_start) AS month_area_rank_start,
	price_per_m2_start,
	RANK() OVER(ORDER BY price_per_m2_start) AS month_price_rank_start,
	avg_total_area_end,
	RANK() OVER(ORDER BY avg_total_area_end) AS month_area_rank_end,
	price_per_m2_end,
	RANK() OVER(ORDER BY price_per_m2_end) AS month_price_rank_end
FROM
	sales_start_seasonality
FULL JOIN sales_end_seasonality
		USING(months);
-- 3.Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     
),
-- Найдём id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    ),
-- Выведем объявления без выбросов:
total_ads_count AS(
SELECT count(id) AS ads_total
FROM real_estate.flats LEFT JOIN real_estate.city using(city_id)
WHERE id IN (SELECT * FROM filtered_id) AND city<>'Санкт-Петербург')
SELECT city, 
count(id) AS ads_count,
round(count(id)/(SELECT ads_total FROM total_ads_count)::NUMERIC,4) AS ads_share,
count(id) FILTER(WHERE days_exposition IS NULL) AS closed_ads,
round(count(id) FILTER(WHERE days_exposition IS NULL)/count(id)::NUMERIC,4) AS closed_ads_share,
round(avg(last_price / total_area)::NUMERIC, 3) AS avg_price_p_m2,
round(avg(total_area::NUMERIC), 3) AS avg_tot_area,
round(avg(days_exposition)::numeric,2) AS avg_act_day
FROM real_estate.flats LEFT JOIN real_estate.city using(city_id) RIGHT JOIN real_estate.advertisement using(id)
INNER join filtered_id USING(id)  where city<>'Санкт-Петербург'
GROUP BY city
ORDER BY count(id) DESC
LIMIT 15;