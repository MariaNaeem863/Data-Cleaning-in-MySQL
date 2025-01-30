-- Data Cleaning 
select *
From layoffs; 

-- 1 Remove duplicate values
-- 2 Standardize the data
-- 3 Null values or blank values
-- 4 Remove any column


create table layoffs_staging
like layoffs ;

select *
From layoffs_staging; 

insert layoffs_staging
select *
From layoffs; 

select * ,
Row_number() over(
Partition by company, industry , total_laid_off, percentage_laid_off,'date' ) as Row_num
From layoffs_staging; 

with duplicate_cte as
( select * ,
Row_number() over(
Partition by company, location, industry , total_laid_off, percentage_laid_off,'date' ,
stage, country, funds_raised_millions) as Row_num
From layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
From layoffs_staging
where company = 'Casper'; 

with duplicate_cte as
( select * ,
Row_number() over(
Partition by company, location, industry , total_laid_off, percentage_laid_off,'date' ,
stage, country, funds_raised_millions) as Row_num
From layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

CREATE TABLE `layoffs_staging2` (
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

select *
From layoffs_staging2 
where row_num > 1;

insert into layoffs_staging2
select * ,
Row_number() over(
Partition by company, location, industry , total_laid_off, percentage_laid_off,'date' ,
stage, country, funds_raised_millions) as row_num
From layoffs_staging;

delete
from layoffs_staging2
where row_num > 1;

select *
From layoffs_staging2 ;

-- standardizing data

select company, trim(company)
From layoffs_staging2 ;  
 
 -- Trim removes whites spaces
 
UPDATE layoffs_staging2
set company = trim(company);

select distinct industry
From layoffs_staging2
order by 1 ;  -- To standarize the data like here crypto and crypto currency our same lets see which our more so we standarize data

select *
From layoffs_staging2
where industry like 'crypto%' ;

-- updating data
update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%' ;

-- checking 
select distinct industry
From layoffs_staging2 ;

-- now look at another column 

select distinct location
From layoffs_staging2; -- no need to change data is already standard

-- now look at another column 

select distinct country
From layoffs_staging2
order by 1 ; 

-- updating
select *
From layoffs_staging2
where country like 'united states' ; -- here issue is of '.'

select distinct country , Trim(trailing '.' from country) 
From layoffs_staging2
order by 1 ; 

UPDATE layoffs_staging2
set country = Trim(trailing '.' from country)
where country like 'united states'
;
-- updating Date column like ist in text so we have to change into date type for which w'ill use function str_to_date() in this we have to pass two parameters inside parenthesis first is coloumn name 
-- and then date format which is '%m/%d(lower case both)/%Y(upper case)'

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y')
 ;
-- checking 
select *
from layoffs_staging2;

-- modifiying date data type to str to date:
alter table layoffs_staging2
modify `date` Date;

-- NULL values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '' ;

select *
from layoffs_staging2
where company ='Airbnb';

update layoffs_staging2
set industry = null
where industry = '' ;

select t1.industry , t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
where t1.industry is null
and t2.industry is not null
;

update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
set t1.industry = t2.industry 
where t1.industry is null
and t2.industry is not null
;

select *
from layoffs_staging2 ;

alter table layoffs_staging2
drop column row_num;

-- DATA CLEANED --
















