-- Data Cleaning


SELECT *
FROM Layoffs;

-- 1. Remove Duplicate
-- 2. standardize the Data
-- 3. Null values or blank Values
-- 4. remove columns or rows



SELECT *
FROM Layoff_staging;


INSERT INTO Layoff_staging
SELECT *
FROM Layoffs
;


SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off, country, `date`) AS row_num
FROM Layoff_staging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off, country, `date`) AS row_num
FROM Layoff_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;




CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



SELECT *
FROM Layoff_staging2
WHERE row_num > 1;

INSERT INTO Layoff_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off, country, `date`) AS row_num
FROM Layoff_staging;


DELETE
FROM Layoff_staging2
WHERE row_num > 1;

SELECT *
FROM Layoff_staging2;


-- Standardizing data

SELECT Company, TRIM(Company)
FROM Layoff_staging2;

UPDATE Layoff_staging2
SET company = TRIM(Company);

SELECT DISTINCT(industry)
FROM Layoff_staging2
ORDER BY 1;

SELECT *
FROM Layoff_staging2
WHERE industry LIKE 'crypto%';

UPDATE Layoff_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';
 

SELECT DISTINCT country, TRIM(trailing '.' from country)
from Layoff_staging2
order by 1;


UPDATE Layoff_staging2
SET country = TRIM(trailing '.' from country)
WHERE country LIKE 'United states%';


SELECT `date`
FROM Layoff_staging2;

UPDATE Layoff_staging2
SET `date` =str_to_date(`date`,'%m/%d/%Y');


ALTER TABLE Layoff_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM Layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM Layoff_staging2
WHERE industry IS NULL
OR industry = '';

UPDATE Layoff_staging2
SET industry = null
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM Layoff_staging2 t1
JOIN Layoff_staging2 t2
    ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE Layoff_staging2 t1
JOIN Layoff_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


SELECT *
FROM Layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM Layoff_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM Layoff_staging2;

ALTER TABLE Layoff_staging2
DROP COLUMN row_num;








