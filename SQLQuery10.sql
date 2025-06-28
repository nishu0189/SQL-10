
----------------------------------------------(**1**) ROW_NUMBER()---------------------------------------------------------------


--------------------------------------- -- ONE WINDOW (without partition by---------------------------------------------------------------------------------
select * ,  
ROW_NUMBER() over (order by salary)as rn from emp --row_number just gove u running no. based on over clause


----------------------------------------------2.Partition By -------------------------------------------------------------------------

-------------------------------------------THREE WINDOW-----------------------------------------------------------------------------------

select * ,--partition based on dept_id (observe the m col)
ROW_NUMBER() over (partition by  dept_id  order by salary )as rn from emp--and salary is in asc in each dept  

------------------------------------ when two col in order by salary desc, name asc-------------------------------------------------------------------

select * ,row_number() over  --this show that when having same salary, then according by name order in asc
(partition by dept_id order by salary desc, name asc) 
from emp

----------------------------------Q1. find the top  2 highest salary of each dept -------------------------------------------------------
 
 with cte as ---Q1 using cte
 (select * ,
 row_number() over (partition by dept_id order by salary desc) row_no
 from emp
 ) 

 select *
 from cte 
 where row_no <= 2

--------------------------------------------------------------------------------------------------------------------------------------------
---Q1 SUBQUERY	
 select * from
 (select * ,
 row_number() over (partition by dept_id order by salary desc) row_no
 from emp) A
 where row_no <=2



 -------------------------------------------(**2**) RANK() ------------------------------------------------------------------------------------

/* rows with the same value in the ORDER BY column, they are assigned the same rank, 
 and the next rank is skipped. This creates gaps in the ranking. */

 --------------------------1. without partition by-----------------------------------------------------
 select * ,  
 ROW_NUMBER() over (order by salary) as row_no, 
 RANK() over (order by salary )as rank_no 
 from emp;


 --------------------2. with partition by ------------------------------------------------------------------- 
 select * , 
 ROW_NUMBER() over (partition by dept_id order by salary) as row_no,
 RANK() over (partition by dept_id order by salary )as rank_no 
 from emp;


 -------------1. in resultant table salary would be in ASC order.... ORDER BY AND PARTITION IS happened acc to last func---------- 

  SELECT *,
       ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS row_no,
       RANK() OVER (PARTITION BY dept_id ORDER BY salary ASC ) AS rank_no
FROM emp;

 SELECT *,    
       ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary ASC) AS row_no,
       RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rank_no
FROM emp;

----------------------------------------------2. partition diff in both func --------------------
  select * , 
 ROW_NUMBER() over (partition by salary order by salary) as row_no,
 RANK() over (partition by dept_id  order by salary )as rank_no 
 from emp;

   select * , 
 ROW_NUMBER() over (partition by dept_id order by salary) as row_no,
 RANK() over (partition by salary order by salary )as rank_no 
 from emp;

 --------------------------------------------3. partition with 2 col combination--------------------------------------
  select * , 
 ROW_NUMBER() over (partition by manager_id order by salary) as row_no,
 RANK() over (partition by dept_id,manager_id order by salary )as rank_no 
 from emp;

 ----------------------------------------------


 -------------------------------------------------------(**3**) Dense_rank() ---------------------------------------------------------
 /* dense_rank same as the rank() but it does not skip the numbering  */

 select * ,
 rank() over (partition by dept_id order by age) as RANK_NO,
 DENSE_RANK() over (partition by dept_id order by age) as Dense 
 from emp


---------------------------------------------------new table PRODUCTS --------------------------------------------------------

  -- drop table products
CREATE TABLE products (
    cust_id INT NOT NULL,
    category CHAR(1) NOT NULL,
    product_name VARCHAR(50) NOT NULL,
    sales INT NOT NULL
);

INSERT INTO products (cust_id, category, product_name, sales)
VALUES 
    (1, 'A', 'iPhone', 1500),
    (2, 'A', 'samsung', 1200),
	(3, 'A', 'MacBook', 2100),
    (4, 'A', 'MacBook', 2000),
	(8, 'A', 'Sony', 600), 
    (5, 'B', 'Dell', 1800),
	(6, 'B', 'Dell', 1800),
    (5, 'B', 'iPhone', 1400),
    (6, 'B', 'Samsung', 1100), 
    (7, 'B', 'Sony', 500);
   
   select * from products

---------------------------------------Q find  top 2 selling product of each category by sale(same product under one category then sale price will be sum up) --------------------------------------
	
-----------------------------------two cte -----------------------------------------------------
	with cte1 as
	(select category, product_name, sum( sales) as sumation 
	from products
	group by product_name, category
	),
	cte2 as
	( select *,
	ROW_NUMBER() over (partition by category order by sumation desc) as row_no
	from cte1
	)
	select * 
	from cte2
	where row_no <= 2


----------------------------------only one cte---------------------------------------
	with cte1 as
	(select category, product_name, sum( sales) as sumation ,
	row_number() over (partition by category order by  sum( sales) desc) as row_no
	from products
	group by product_name, category
	)
	select * 
	from cte1
	where row_no <= 2

--------------------------------------------- (**4**)LEAD(col_1, offset, default_value)--------------------------------------------------------
--FORWARD value 
--salary higher than curr emp

-------------------------------1. Lead(col_1)--------by default 1 no leading-------------------------
	select * ,
	lead(e_id) over (order by salary) as lead_no   
	from emp 

-------------------------------2. Lead(col_1,2)--------e_id after 2 no leading in current row-------------------------

	select * ,
	lead(e_id,2) over (order by salary) as lead_no  
	from emp

-------------------------------3. Lead(col_1,2,default_value)-----default value insetead of null e_id -------------------------

	select * ,
	lead(e_id,2,90909090) over (order by salary) as lead_no  
	from emp

-------------------------------4. Lead(col_1 ,2,col2) e_id which h row-------------------------

	select * ,
	lead(e_id,2,age) over (order by salary) as lead_no  
	from emp
	
-------------------------------5. Lead(col_1,1) using paratition by -------------------------

	select * ,
	lead(e_id,1) over (partition by dept_id order by salary) as lead_no  
	from emp

--------------------------------------------- (**5**) Lag(col_1,1)--------------------------------------------------------
--backward 
--salary lower than curr emp
    select * ,
	lag(e_id,1) over (partition by dept_id order by salary) as lag_no  
	from emp

---------------------------------------lag can be achieve using lead just changing the order by-----------------------------------------------------------------

	select * ,
	lead(e_id,1) over (partition by dept_id order by salary ASC) as lead_no
	from emp
	--OR
	SELECT *,
    LAG(e_id, 1) OVER (PARTITION BY dept_id ORDER BY salary DESC) AS lead_no
FROM emp;

	select * ,
	lead(e_id,1) over (partition by dept_id order by salary ASC) as lead_no ,
	lag(e_id,1) over (partition by dept_id order by salary DESC) as lag_no
	from emp


-------------------------------------------------USE CASE ------------------------------------------------------------
 -- how much of year growth can be find by either by the lead or lag 

 --------------------------------------------- (**6**) first_value()--------------------------------------------------------

 	select * ,
	first_value(salary) over (partition by dept_id order by salary ASC) as lead_no 
	from emp






----------------------------------------------------QUESTIONS -------------------------------------------------------------------------------

/* 1- write a query to print 3rd highest salaried employee details for each department (give preferece to younger employee in case
of a tie).In case a department has less than 3 employees then print the details of highest salaried employee in that department. */

SELECT * from emp
-------------------

with cte1 as (
select dept_id,name , ROW_NUMBER() over (partition by dept_id order by salary desc , age asc)  as rn,
count(*) over (partition by dept_id) as total_emp 
from emp)

select dept_id, name from cte1 where( rn=3 and total_emp >=3) OR (rn = 1 and total_emp<3)
		
		
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
--Q2:- write a query to find top 2 and bottom 2 products by sales in each category.

select * from orders
order by category

---------------------1. using only where clause----------------------------------------------------
with cte1 AS(
select *,
row_number() over (partition by category  order by sales) as buttom_row ,
row_number() over (partition by category  order by sales desc) as top_row 
from orders 
)

Select category, sales
from cte1
where (top_row <= 2) OR
     (buttom_row <= 2)


---------------------------------------------2. using case when and WHERE ------------------------------------------
	  WITH RankedProducts AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS bottom_rank ,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales ASC) AS top_rank  
    FROM  orders
)
SELECT 
    category, sales,
    CASE 
        WHEN top_rank <= 2 THEN 'Top 2'
        WHEN bottom_rank <= 2 THEN 'Bottom 2'
    END AS rank_type
FROM RankedProducts
WHERE top_rank <= 2 OR bottom_rank <= 2
ORDER BY category, rank_type, sales DESC;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

 -- Q3:- Among all the sub categories .. which sub category had highest month over month growth by sales in Jan 2020.

--select * from orders

WITH monthly_sales AS (
SELECT category,FORMAT(or_date, 'yyyy-MM') AS year_month, SUM(sales) AS total_sales
FROM orders
GROUP BY category, FORMAT(or_date, 'yyyy-MM')),

sales_with_lag AS (
SELECT category, year_month, total_sales,LAG(total_sales) OVER (PARTITION BY category ORDER BY year_month) AS prev_month_sales
FROM monthly_sales),

oct_2024_growth AS (
SELECT category,total_sales,prev_month_sales,(total_sales - prev_month_sales) * 1.0 / NULLIF(prev_month_sales, 0) AS growth
FROM sales_with_lag
WHERE year_month = '2024-10')
SELECT TOP 1 category,growth
FROM oct_2024_growth
ORDER BY growth DESC;
		
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
--Q4:-
--------------------------------------------------TABLE PROD --------------------------------------------------------------
	CREATE TABLE prod (
    product_id INT PRIMARY KEY,
    category VARCHAR(50),
    product_name VARCHAR(50),
    sales INT,
    sale_year INT
);


INSERT INTO prod(product_id, category, product_name, sales, sale_year) VALUES
(1, 'A', 'Table', 100, 2020),
(2, 'A', 'Chair', 80, 2020),
(3, 'A', 'Sofa', 120, 2020),
(4, 'A', 'Table', 90, 2019),
(5, 'A', 'Chair', 85, 2019),
(6, 'A', 'Sofa', 100, 2019),
(7, 'A', 'Table', 110, 2021),
(8, 'A', 'Chair', 70, 2021),
(9, 'A', 'Sofa', 130, 2021),
(10, 'B', 'Laptop', 200, 2020),
(11, 'B', 'Phone', 250, 2020),
(12, 'B', 'Tablet', 150, 2020),
(13, 'B', 'Laptop', 190, 2019),
(14, 'B', 'Phone', 260, 2019),
(15, 'B', 'Tablet', 140, 2019),
(16, 'B', 'Laptop', 210, 2021),
(17, 'B', 'Phone', 240, 2021),
(18, 'B', 'Tablet', 160, 2021),
(19, 'A', 'Table', 95, 2020),   -- Same product multiple sales in the same year
(20, 'B', 'Phone', 255, 2020); -- Same product multiple sales in the same year


--4- write a query to print top 3 products in each category by year over year sales growth in year 2020.


with cte1 as   --sum of product in particular category and particular year
(Select category,product_name , sale_year,
sum(sales) as total_sale1
from prod
group by category,product_name,sale_year),

  cte2 as(
  select  *,
 lag(total_sale1) over (partition by category order by product_name, sale_year ) as previous_sale2
 from cte1
 ),
  yoy_growth3 as(
 select  *,
 case when previous_sale2 IS NOT NULL then
 (total_sale1 - previous_sale2)*100 /  previous_sale2 
 Else null
 end as yoy_percentage3
 from cte2
 where sale_year = 2020 
 ),
 ranked as (
 select * ,
  ROW_NUMBER()over (partition by category order by yoy_percentage3) as row_no
 from yoy_growth3
 )

 select *
 from ranked
 where row_no <= 3

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

 -- drop table call_start_logs
create table call_start_logs(phone_number varchar(10),
start_time datetime)

insert into call_start_logs values
('PN1','2022-01-01 10:20:00'), ('PN1','2022-01-01 16:25:00'), ('PN2','2022-01-01 12:30:00')
,('PN3','2022-01-02 10:00:00'), ('PN3','2022-01-02 12:30:00'), ('PN3','2022-01-03 09:20:00')

create table call_end_logs(phone_number varchar(10),
end_time datetime )

insert into call_end_logs values
('PN1', '2022-01-01 10:45:00'), ('PN1','2022-01-01 17:05:00'), ('PN2','2022-01-01 12:55:00')
,('PN3','2022-01-02 10:20:00'),('PN3','2022-01-02 12:50:00'),('PN3','2022-01-03 09:40:00')


/* Q5:- write a query to get start time and end time of each call from above 2 tables.Also create a column of call duration in minutes. Please do take into account
there will be multiple calls from one phone number and each entry in start table has a corresponding entry in end table. */

Select s.phone_number, s.row_no1,s.start_time,e.end_time,  DATEDIFF(minute,start_time,end_time) 
from
(Select * , 
row_number() over (partition by  phone_number  order by start_time) as row_no1
from call_start_logs)  s
inner join 
(Select * ,
row_number() over (partition by  phone_number  order by end_time) as row_no2
from call_end_logs) e 
on s.phone_number = e.phone_number AND s. row_no1 = e. row_no2


-----------------------------------------------------------------------------------------------------------------------------------------------------------------
