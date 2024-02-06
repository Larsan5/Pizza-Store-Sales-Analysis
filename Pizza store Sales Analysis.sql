/*
PART 1 - DATABASE AND TABLE CREATION 
*/

-- Create a Database for the Dataset
create schema pizza_place;


-- Create the first table "pizza_types" which has primary key and no foreign key 
CREATE TABLE pizza_types (
    pizza_type_id VARCHAR(25) PRIMARY KEY,
    name VARCHAR(125),
    category VARCHAR(20),
    ingredients TEXT
);


-- Upload the pizza_types table form an existing csv file
load data infile 'E:/Datasets/Pizza+Place+Sales/pizza_sales/pizza_types.csv'
into table pizza_types
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- checking whether data from CSV has correctly uploaded into the table
SELECT 
    *
FROM
    pizza_types;


-- SIMILARY CREATE OTHER TABLE WITH FOREIGN KEY FROM FIRST TABLE
CREATE TABLE pizzas (
    pizza_id VARCHAR(20) PRIMARY KEY,
    pizza_type_id VARCHAR(20),
    size VARCHAR(5),
    price DOUBLE,
    FOREIGN KEY (pizza_type_id)
        REFERENCES pizza_types (pizza_type_id)
);

load data infile 'E:/Datasets/Pizza+Place+Sales/pizza_sales/pizzas.csv'
into table pizzas
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


/* HERE INSTEAD OF DIRECTLY LOADING THE FIELDS INTO THE TABLE, I HAVE CONCATEANTED TWO FIELDS
DATE AND TIME INTO DATETIME FIELD
*/
create table orders
(order_id int,
date datetime
);

load data infile 'E:/Datasets/Pizza+Place+Sales/pizza_sales/orders.csv'
into table orders
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(order_id, @date , @time) -- ASSIGNING FIELDS TO USER- ASSIGNED VARIABLE
 set date = cast(concat(@date ," ",@time) as datetime); -- CONCATENATE AND CAST INTO DATETIME DATATYPE
 
 
 SHOW FIELDS FROM orders; -- checking the datatype of the columns


-- CREATE THE LAST TABLE 
create table order_details
(order_details_id int primary key,
order_id int not null ,
pizza_id varchar(20),
quantity int,
foreign key (order_id) references orders(order_id),
foreign key(pizza_id) references pizzas(pizza_id)			

);


load data infile 'E:/Datasets/Pizza+Place+Sales/pizza_sales/order_details.csv'
into table order_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


select * 
from order_details;


/* PART- 2 ANALYSING THE DATA */

/*
FINDING THE DAILY CUSTOMERS FOR EACH DAY AND AVERAGE CUSTOMER PER DAY
USING WINDOWS FUNCTIONS
 */
SELECT 
	O.*,
	AVG(CUSTOMERS) OVER(PARTITION BY MONTH) AS MONTHLY_AVERAGE, -- AGGREAGATE FUNCTION AS WINDOWS FUNCTIONS
	AVG(CUSTOMERS) OVER() AS YEARLY_AVERAGE
FROM (
	SELECT DATE(DATE) AS DATE,
	MONTH(DATE) AS MONTH,
	COUNT(ORDER_ID) AS CUSTOMERS
	FROM ORDERS 
	GROUP BY 1,2) AS O;-- SUBQUERY IN THE FROM STATEMENT
-- ON AVERAGE  60 CUSTOMERS WILL COME TO THE STORE EVERY DAY


-- FINDING THE PEAK HOUR OF THE STORE
SELECT 
    HOUR(DATE) AS HOUR_OF_THE_DAY, COUNT(ORDER_ID) AS CUSTOMERS
FROM
    ORDERS
GROUP BY 1
ORDER BY 1;
/* 12TH - 13TH HOUR AND  17TH - 18TH HOUR ARE PEAK HOURS OF THE STORE
AS THE NUMBER OF ORDERS ARE VERY HIGH*/


--  FINDING THE NUMBER PIZZAS IN EACH ORDER
select 
	order_id,
	SUM(QUANTITY) as pizzas_per_order,
AVG(SUM(QUANTITY)) OVER() AS AVERAGE_PIZZA_PERORDER
FROM order_details
GROUP BY 1;
-- THERE ARE 2.3 PIZZAS PER ORDER 


-- BEST SELLING PIZZA AND WORST SELLING PIZZA WITH THEIR SALES AND REVENUE
SELECT 
    pizza_types.name,
    pizza_types.category,
    SUM(CASE
        WHEN pizzas.size = 'S' THEN ORDER_DETAILS.quantity  -- PIVOTING SIZE INTO COLUMNS USING CASE STATEMENTS
        ELSE NULL
    END) AS orders_S_size,
    SUM(CASE
        WHEN pizzas.size = 'M' THEN ORDER_DETAILS.quantity
        ELSE NULL
    END) AS orders_M_size,
    SUM(CASE
        WHEN pizzas.size = 'L' THEN ORDER_DETAILS.quantity
        ELSE NULL
    END) AS orders_L_size,
    SUM(CASE
        WHEN pizzas.size = 'XL' THEN ORDER_DETAILS.quantity
        ELSE NULL
    END) AS orders_XL_size,
    SUM(CASE
        WHEN pizzas.size = 'XXL' THEN ORDER_DETAILS.quantity
        ELSE NULL
    END) AS orders_XXL_size,
    SUM(ORDER_DETAILS.quantity) AS no_of_orders,
    ROUND(SUM(CASE
                WHEN pizzas.size = 'S' THEN (ORDER_DETAILS.QUANTITY * PIZZAS.PRICE)
                ELSE NULL
            END)) AS revenue_S_Size,
    ROUND(SUM(CASE
                WHEN pizzas.size = 'M' THEN (ORDER_DETAILS.QUANTITY * PIZZAS.PRICE)
                ELSE NULL
            END)) AS revenue_M_Size,
    ROUND(SUM(CASE
                WHEN pizzas.size = 'L' THEN (ORDER_DETAILS.QUANTITY * PIZZAS.PRICE)
                ELSE NULL
            END)) AS revenue_L_Size,
    ROUND(SUM(ORDER_DETAILS.QUANTITY * PIZZAS.PRICE)) AS REVENUE,
    ROUND(SUM(ORDER_DETAILS.QUANTITY * PIZZAS.PRICE) / SUM(ORDER_DETAILS.quantity),2)  as Average_Revenue
FROM
    order_details
        LEFT JOIN
    PIZZAS ON PIZZAS.pizza_id = ORDER_DETAILS.PIZZA_ID
        LEFT JOIN
    pizza_types ON pizza_types.pizza_type_id = PIZZAS.pizza_type_id
GROUP BY 1 , 2
ORDER BY 8 DESC;

/*
Best Selling            : The Classic Deluxe Pizza   - 2453 Pizzas
Highest Revenue Earning : The Thai Chicken Pizza      - $43434
Least Selling           : The Brie Carre Pizza        - 490 Pizzas
Highest Average Revenue : The Brie Carre  Pizza       - 23.65 Dollars per pizza

The Brie Carre Pizza is The Costliest Pizza Costing about $23.65 for small size.
The Pizza store also sells only Small Size Brie Carre Pizza.
 */


-- MONTHLY SALES TREND AND PERCENT INCREASE/ DECREASE 
WITH TREND 
	AS	(   	-- USING CTE TABLES
		SELECT 
			year(date) AS YEAR,
			quarter(date) as QUARTER,
			MONTH(DATE) AS MONTH,
			COUNT(distinct ORDERS.ORDER_ID) AS MONTHLY_ORDERS,
			ROUND((COUNT(distinct ORDERS.ORDER_ID) - LAG(COUNT(distinct ORDERS.ORDER_ID)) OVER()) /LAG(COUNT(distinct ORDERS.ORDER_ID)) OVER() * 100,2) AS MONTLHY_PERCENT_INCREASE ,
			round(sum(order_details.quantity * pizzas.price)) as REVENUE,
			ROUND((round(sum(order_details.quantity * pizzas.price)) -lag(round(sum(order_details.quantity * pizzas.price))) over()) / lag(round(sum(order_details.quantity * pizzas.price))) over()  * 100, 2) as REVENUE_PCNT_INCREASE
			FROM ORDERS 
			lEFT JOIN order_details
				ON ORDERS.ORDER_ID = ORDER_DETAILS.order_id 
			LEFT JOIN PIZZAS
				ON ORDER_DETAILS.PIZZA_ID = PIZZAS.pizza_id 
			GROUP BY 1,2,3)

SELECT TREND.*,
SUM(revenue) over( PARTITION BY quarter) as Quarterly_REV,
SUM(REVENUE) OVER() AS TOTAL_REVENUE
FROM TREND;
/*
There is no significant change in the revenue  over the year.  
Reductionin sales and Increasing  in sale s are almost the
JULY Month has highest revenue and sales - 1935  orders
OCTOBER month has lowest revenue and sales - 1646 orders.
In Third Quarter, The decrease in sales is consequent which leads to lowest sales in October 
*/


-- CATEGORY WISE SALES IN EACH HOUR OF THE DAY 
SELECT 
    PIZZA_TYPES.category,
    SUM(ORDER_DETAILS.QUANTITY) AS TOTAL_ORDERS,
    SUM(CASE
        WHEN HOUR(DATE) = '9' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 9_AM,
    SUM(CASE
        WHEN HOUR(DATE) = '10' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 10_AM,
    SUM(CASE
        WHEN HOUR(DATE) = '11' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 11_AM,
    SUM(CASE
        WHEN HOUR(DATE) = '12' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 12_AM,
    SUM(CASE
        WHEN HOUR(DATE) = '13' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 1_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '14' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 2_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '15' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 3_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '16' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 4_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '17' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 5_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '18' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 6_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '19' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 7_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '20' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 8_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '21' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 9_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '22' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 10_PM,
    SUM(CASE
        WHEN HOUR(DATE) = '23' THEN ORDER_DETAILS.QUANTITY
        ELSE 0
    END) AS 11_PM
FROM
    ORDERS
        LEFT JOIN
    order_details ON ORDERS.ORDER_ID = ORDER_DETAILS.order_id
        LEFT JOIN
    PIZZAS ON ORDER_DETAILS.PIZZA_ID = PIZZAS.pizza_id
        LEFT JOIN
    pizza_types ON pizza_types.pizza_type_id = PIZZAS.pizza_type_id
GROUP BY 1
ORDER BY 2 DESC
;

/* We have seen that 12 pm,1 pm ,5 pm and  6pm are the peak hours 
In that time period, Classic Pizza Types are the most selling.
*/

-- TOP PIZZAS THAT ARE SOLD TOGETHER
with  t1 as (
SELECT 
    order_id,
    COUNT(order_details_id) AS no_of_pizzas
FROM
    order_details 	
GROUP BY 1
HAVING COUNT(order_details_id) > 1 ),

 t2 as (
select 
od.order_details_id,
od.order_id,
od.pizza_id,
pizza_types.name
from order_details as od
 inner join t1 
 on t1.order_id = od.order_id
  LEFT JOIN
    PIZZAS ON od.PIZZA_ID = PIZZAS.pizza_id
        LEFT JOIN
    pizza_types ON pizza_types.pizza_type_id = PIZZAS.pizza_type_id
 )
   select 
	 pizza_id,
	 name,
	 count(order_details_id) AS ORDERS,
     DENSE_RANK() OVER( ORDER BY COUNT(ORDER_DETAILS_ID) DESC) AS RANKING
    from t2
	 Group by 1
     order by 3 desc
     LIMIT 6;

/* The Pizzas that are mostly sold together are
	THE BIG MEAT PIZZA , 
	THE THAI CHICKEN PIZZA, 
	THE FIVE CHEESE PIZZA, 
	THE FOUR CHEESE PIZZA, 
	THE CLASSIC DELUXE PIZZA
*/     

-- TOTAL SALES ON EACH WEEKDAY 
SELECT 
    DAYNAME(date),
    COUNT(order_details.order_details_id) AS No_of_orders
FROM
    orders
        LEFT JOIN
    order_details ON orders.order_id = order_details.order_id
GROUP BY 1
ORDER BY 2 DESC;

-- FRIDAY AND SATURDAY HAS HIGHEST SALES OF PIZZAS
-- Friday -8106
-- Saturday - 7355
