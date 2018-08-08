-- Shamir Alavi
-- Oracle Database 11g Release 11.2.0.1.0 - 64bit
-- Find the database schema at: https://docs.oracle.com/cd/E11882_01/server.112/e10831/diagrams.htm#COMSC112
/* 

3.1 The company wants to select the top 3 employees for performance, in each
department, in Q1 of 2016. (Assume date columns are stored as standard
timestamps)
	a. 	Define a metric for employee performance and write a query that finds the
		top 3 employees per department in Q1 of 2016.
	b. 	Expand the query in part a to look at most improved employees Year over
		Year.

Part a:
    Create indices for 2 additional columns that have high sparsity and are used in the query.
    The columns are order_total, cust_first_name.

*/

create index ord_total_ix on oe.orders(order_total);
analyze table oe.orders compute statistics;

create index cust_first_name_ix on oe.customers(cust_first_name);
analyze table oe.customers compute statistics;


-- Top 3 employees per department in Q1 of 2016 based on performance

select *
from
(
	-- rank employees based on their total points
	select 	sales_rep,
			department,
			revenue_points,
			customer_points,
			product_points,
			sum(revenue_points + customer_points + product_points) as total_points,
			rank() over (partition by department order by sum(revenue_points + customer_points + product_points) desc) as employee_rank
	from
	(
		-- normalize features and calculate points for each employee per department to generate performance metrics
		select 	sales_rep, 
				department,
				-- revenue_points = revenue_generated normalized with a 50% weight, i.e.  (normalized_value * 100% * 0.5)
				-- customer_points = cust_served_points normalized with a 30% weight, i.e. (normalized_value * 100% * 0.3)
				-- product_points = unique_products_sold normalized with a 20% weight, i.e. (normalized_value * 100% * 0.2)
				((revenue_generated / sum(revenue_generated) over (order by department)) * 50) as revenue_points,
				((cust_served_points / sum(cust_served_points) over (order by department)) * 30) as customer_points,
				((unique_products_sold / sum(unique_products_sold) over (order by department)) * 20) as product_points
		from
		(
			-- calculate features: 	revenue_generated = amount of revenue generated (order_total)
			--						cust_served_points = number of unique customers served 
			--						unique_products_sold = number of unique products sold
			-- These values are used to calculate employee performance metrics above.
			select 	first_name as sales_rep,
					department_name as department,
					sum(order_total) as revenue_generated,
					count(distinct cust_first_name) as cust_served_points,
					count(distinct product_id) as unique_products_sold
			from 	oe.customers 
			join    oe.orders on orders.customer_id = customers.customer_id
			join    hr.employees on employees.employee_id = orders.sales_rep_id
			join    hr.departments on employees.department_id = departments.department_id
			join    oe.order_items on order_items.order_id = orders.order_id
			where   order_date between '01-JAN-08' and '31-MAR-08'
			group by first_name, department_name
		)
	)
	group by sales_rep, department, revenue_points, customer_points, product_points
)
where employee_rank <= 3;

/* **************************************************************************************************************** */

/* 

3.1 Part b 
    Top 3 employees per department per year based on their performance
   
    I could not do exactly what the question asked for, i.e. finding the most improved employees. Instead, I expanded
    part a to find the top 3 employees per department per year. That way, we can look at the employee rankings year 
    over year and see who climbed up the ladder. As the next step, I would have stored the result (rankings) in a table/view
    and calculate another metric which would show how many rank(s) an employee has gone up or down over the years, similar 
    to that of club/team rankings in the sports industry.

*/

/* Create a table that stores the value of the first year and the latest year orders were recieved.
   We will use these values later to rank employees on a yearly basis */

create table years_in_review as
select 	extract(year from min(order_date)) "min_year", 
	extract(year from max(order_date)) "max_year"
from oe.orders;

-- MAIN QUERY --
declare
	type namearray is varray(1000) of hr.employees.first_name%type;
	type numberarray is varray(1000) of oe.orders.order_total%type;
	min_year number;
	max_year number;
	emp_f_name namearray;
	emp_l_name namearray;
	dept namearray;
	rev_pts numberarray;
	cust_pts numberarray;
	prod_pts numberarray;
	tot_points numberarray;
	ranking numberarray;
	num_rows number;

--This cursor executes the original query (part a) for each year based on when orders were received.
--The 'where clause' is parameterized with a variable 'var1' that sets the value of the year.

cursor performance_cursor (var1 number) is
-- start of query (part a)
	select 	rep_first_name,
			rep_last_name,
			department,
			revenue_points,
			customer_points,
			product_points,
			sum(revenue_points + customer_points + product_points) as total_points,
			rank() over (partition by department order by sum(revenue_points + customer_points + product_points) desc) as employee_rank
	from
	(
		select 	rep_first_name,
				rep_last_name,
				department,
				((revenue_generated / sum(revenue_generated) over (order by department)) * 100) * 0.5 as revenue_points, 
				((cust_served_points / sum(cust_served_points) over (order by department)) * 100) * 0.3 as customer_points,
				((unique_products_sold / sum(unique_products_sold) over (order by department)) * 100) * 0.2 as product_points
		from
		(
			select 	first_name as rep_first_name,
					last_name as rep_last_name,
					department_name as department,
					sum(order_total) as revenue_generated,
					count(distinct cust_first_name) as cust_served_points,
					count(distinct product_id) as unique_products_sold
			from 	oe.customers 
			join    oe.orders on orders.customer_id = customers.customer_id
			join    hr.employees on employees.employee_id = orders.sales_rep_id
			join    hr.departments on employees.department_id = departments.department_id
			join    oe.order_items on order_items.order_id = orders.order_id
			where extract(year from order_date) = var1
			group by first_name, last_name, department_name
		)
	)
	group by rep_first_name, rep_last_name, department, revenue_points, customer_points, product_points
;
-- end of query (part a)

	e_type performance_cursor%rowtype;	-- will be used to fetch data from the table the cursor above points to

-- This cursor executes a portion of the query from part a to obtain the total row count after performance features are measured on a yearly basis.
cursor rowcount_cursor (var2 number) is
	select count(*) as row_count from
		(
		select 	first_name as rep_first_name,
			last_name as rep_last_name,
			department_name as department,
			sum(order_total) as revenue_generated,
			count(distinct cust_first_name) as cust_served_points,
			count(distinct product_id) as unique_products_sold
		from 	oe.customers 
		join    oe.orders on orders.customer_id = customers.customer_id
		join    hr.employees on employees.employee_id = orders.sales_rep_id
		join    hr.departments on employees.department_id = departments.department_id
		join    oe.order_items on order_items.order_id = orders.order_id
		where extract(year from order_date) = var2
		group by first_name, last_name, department_name
		);
	
begin
	select * into min_year, max_year from years_in_review;
	
	dbms_output.put_line('Top 3 employees every year:');
	
	-- Loop for each year orders were received and execute query using 'performance_cursor' 
	-- to rank employees on a yearly basis based on their performace
	for i in min_year..max_year 
	loop
		open performance_cursor(i);
		fetch performance_cursor bulk collect into emp_f_name, emp_l_name, dept, rev_pts, cust_pts, prod_pts, tot_points, ranking;
		if performance_cursor%rowcount = 0 then		-- pass on to the next year if there is no data for a certain year
			dbms_output.put_line('---');
			dbms_output.put_line('NO DATA FOR YEAR ' || i);
			close performance_cursor;
			continue;
		else
			close performance_cursor;
			
			open rowcount_cursor(i);
			fetch rowcount_cursor into num_rows;
			close rowcount_cursor;
			dbms_output.put_line('---');
			dbms_output.put_line('FOR THE YEAR '|| i ||':');
			if num_rows < 3 then
				for j in 1 .. num_rows
				loop
					dbms_output.put_line(emp_f_name(j) ||' '|| emp_l_name(j) ||' from '|| dept(j) ||' got '|| tot_points(j) ||' POINTS and acheived RANK # '|| ranking(j) );
				end loop;
			else
				for j in 1 .. 3
				loop
					dbms_output.put_line(emp_f_name(j) ||' '|| emp_l_name(j) ||' from '|| dept(j) ||' got '|| tot_points(j) ||' POINTS and acheived RANK # '|| ranking(j) );
				end loop;
			end if;
		end if;
	end loop;
end;
/

/* **************************************************************************************************************** */
