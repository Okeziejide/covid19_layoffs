-- Data Cleaning
/* 
1. Remove Duplicates
2. Standardise the Data
3. Null Values or Blank Values
4. Remove Any Columns
*/

-- Create a staging table: layoffs_staging3
CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging3
select *,
row_number() OVER(partition by company, location,industry,total_laid_off,percentage_laid_off,
 `date`, stage,country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging3
where row_num > 1;

delete
from layoffs_staging3
where row_num > 1;

select company, trim(company)
from layoffs_staging3;

update layoffs_staging3
set company = trim(company);

select *
from layoffs_staging3;

select distinct industry
from layoffs_staging3;

select *
from layoffs_staging3
where industry like 'crypto%';

update layoffs_staging3
set industry = 'crypto'
where industry like 'crypto%';

select *
from layoffs_staging3
where industry is null or 
industry = '';

select distinct country, trim( trailing '.' from country)
from layoffs_staging3;

select distinct country
from layoffs_staging3
order by 1;

update layoffs_staging3
set country = 'united states'
where country = 'united states.';

select `date`, str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging3;

update layoffs_staging3
set `DATE` =  str_to_date(`date`,'%m/%d/%Y');

select *
FROM layoffs_staging3;

ALTER table layoffs_staging3
modify column `DATE` DATE;

select *
FROM layoffs_staging3
where total_laid_off is null and
percentage_laid_off is null;

-- populate null and empty values
select *
from layoffs_staging3
where industry is null
or industry = '';

update layoffs_staging3
set industry = null
where industry = '';

select * 
from layoffs_staging3 t1
join layoffs_staging3 t2
on t1.company = t2.company
and t1.location = t2.location
where t1.industry is null and 
t2.industry is not null;

update layoffs_staging3 t1
join layoffs_staging3 t2
on t1.company = t2.company
and t1.location = t2.location
set t1.industry = t2.industry
where t1.industry is null and 
t2.industry is not null;

select *
from layoffs_staging3
where total_laid_off is null 
and percentage_laid_off is null;

-- delete rows where total_laid_off is null and percentage_laid_off is null;

delete 
from layoffs_staging3
where total_laid_off is null 
and percentage_laid_off is null;

-- drop not needed column
alter table layoffs_staging3
drop column row_num;


-- Exploratory Data Analysis Queries for Layoffs Dataset

-- Basic Overview Queries

-- 1. Total number of layoff records
SELECT COUNT(*) AS total_records FROM layoffs_staging3;

-- 2. Date range of the data
SELECT MIN(date) AS earliest_date, MAX(date) AS latest_date FROM layoffs_staging3;

-- 3. Companies with the most layoffs (top 10)
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
WHERE total_laid_off IS NOT NULL
GROUP BY company
ORDER BY total_laid_off DESC
LIMIT 10;

-- 4. Companies with highest percentage laid off (top 10)
SELECT company, percentage_laid_off
FROM layoffs_staging3
WHERE percentage_laid_off IS NOT NULL
ORDER BY percentage_laid_off DESC
LIMIT 10;

-- Industry Analysis

-- 5. Layoffs by industry (top 10)
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
WHERE industry IS NOT NULL AND total_laid_off IS NOT NULL
GROUP BY industry
ORDER BY total_laid_off DESC
LIMIT 10;

-- 6. Average percentage laid off by industry
SELECT industry, AVG(percentage_laid_off) AS avg_percentage_laid_off
FROM layoffs_staging3
WHERE industry IS NOT NULL AND percentage_laid_off IS NOT NULL
GROUP BY industry
ORDER BY avg_percentage_laid_off DESC
LIMIT 10;

-- Geographic Analysis

-- 7. Layoffs by country (top 10)
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
WHERE country IS NOT NULL AND total_laid_off IS NOT NULL
GROUP BY country
ORDER BY total_laid_off DESC
LIMIT 10;

-- 8. Layoffs by city (top 10)
SELECT location, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
WHERE location IS NOT NULL AND total_laid_off IS NOT NULL
GROUP BY location
ORDER BY total_laid_off DESC
LIMIT 10;

-- Company Stage Analysis

-- 9. Layoffs by company stage
SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging3
WHERE stage IS NOT NULL AND stage != 'Unknown' AND total_laid_off IS NOT NULL
GROUP BY stage
ORDER BY total_laid_off DESC;

-- 10. Average percentage laid off by company stage
SELECT stage, AVG(percentage_laid_off) AS avg_percentage_laid_off
FROM layoffs_staging3
WHERE stage IS NOT NULL AND stage != 'Unknown' AND percentage_laid_off IS NOT NULL
GROUP BY stage
ORDER BY avg_percentage_laid_off DESC;

-- Temporal Analysis

-- 11. Layoffs by month
SELECT
    DATE_FORMAT(date, '%Y-%m') AS month,
    SUM(total_laid_off) AS total_laid_off,
    COUNT(*) AS number_of_layoffs
FROM layoffs_staging3
WHERE total_laid_off IS NOT NULL
AND DATE_FORMAT(date, '%Y-%m') IS NOT NULL
GROUP BY DATE_FORMAT(date, '%Y-%m')
ORDER BY month;

-- 12. Layoffs by day of week
SELECT 
    DAYNAME(date) AS day_of_week,
    SUM(total_laid_off) AS total_laid_off,
    COUNT(*) AS number_of_layoffs
FROM layoffs_staging3
WHERE total_laid_off IS NOT NULL
AND 
DAYNAME(date) IS NOT NULL
GROUP BY DAYNAME(date), DAYOFWEEK(date)
ORDER BY DAYOFWEEK(date);


-- Funding Analysis

-- 13. Layoffs by funding raised buckets
SELECT 
    CASE 
        WHEN funds_raised_millions IS NULL THEN 'Unknown'
        WHEN funds_raised_millions < 100 THEN 'Below $100M'
        WHEN funds_raised_millions < 500 THEN '$100M-$500M'
        WHEN funds_raised_millions < 1000 THEN '$500M-$1B'
        ELSE 'Above $1B'
    END AS funding_bucket,
    SUM(total_laid_off) AS total_laid_off,
    COUNT(*) AS number_of_layoffs,
    AVG(percentage_laid_off) AS avg_percentage_laid_off
FROM layoffs_staging3
GROUP BY funding_bucket
ORDER BY total_laid_off DESC;

-- 14. Correlation between funds raised and layoffs
SELECT 
    date_format(date,'%Y-%m'),
    company,
    funds_raised_millions,
    total_laid_off,
    percentage_laid_off
FROM layoffs_staging3
WHERE funds_raised_millions IS NOT NULL 
AND (total_laid_off IS NOT NULL or percentage_laid_off IS NOT NULL)
ORDER BY funds_raised_millions DESC
LIMIT 20;

-- Data Quality Checks

-- 15. Count of NULL values in each column
SELECT 
    SUM(CASE WHEN company IS NULL THEN 1 ELSE 0 END) AS null_company,
    SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) AS null_location,
    SUM(CASE WHEN industry IS NULL THEN 1 ELSE 0 END) AS null_industry,
    SUM(CASE WHEN total_laid_off IS NULL THEN 1 ELSE 0 END) AS null_total_laid_off,
    SUM(CASE WHEN percentage_laid_off IS NULL THEN 1 ELSE 0 END) AS null_percentage_laid_off,
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date,
    SUM(CASE WHEN stage IS NULL THEN 1 ELSE 0 END) AS null_stage,
    SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN funds_raised_millions IS NULL THEN 1 ELSE 0 END) AS null_funds_raised
FROM layoffs_staging3;

-- 16. Records with both total_laid_off and percentage_laid_off NULL
SELECT COUNT(*) AS records_without_layoff_data
FROM layoffs_staging3
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

/*These queries provide a comprehensive starting point for exploring the layoffs dataset,
covering basic statistics, industry trends, geographic patterns, company stages, temporal analysis, 
funding relationships, and data quality checks.*/
