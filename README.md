# SQL-10

## SQL Window Functions and Ranking Practice

### 1. ROW_NUMBER()
Assigns a unique number to each row based on ORDER BY.
ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC)

### 2. RANK() vs DENSE_RANK()
RANK() skips numbers after a tie (e.g. 1, 1, 3).
DENSE_RANK() doesn't skip ranks (e.g. 1, 1, 2).
Used when dealing with ties in values.

RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC)
DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY age ASC)

### 3. Partitioning
Creates separate "windows" for each partition group (like dept_id or manager_id).
Often combined with ORDER BY in window functions.

### 5. LEAD() and LAG()
LEAD() looks forward in a partition (next row).
LAG() looks backward (previous row).
Used to calculate trends, e.g. year-over-year growth.

LEAD(sales, 1, 0) OVER (PARTITION BY product_id ORDER BY year)

### 6. FIRST_VALUE()
Retrieves the first value in an ordered partition.
FIRST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY salary)
