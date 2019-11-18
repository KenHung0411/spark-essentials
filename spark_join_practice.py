from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col, max, desc
from pyspark.sql.types import stringType


spark = SparkSession.builder.appName("spark_practice").getOrCreate
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://localhost:5432/rtjvm"
user = "docker"
password = "docker"


def read_jdbc_table(table_name):
    return spark.read.\
            format("jdbc").\
            option("driver", driver).\
            option("url",url).\
            option("user", user).\
            option("password", password).\
            option("dbtable","public.{}".format(table_name)).\
            load()

employee_df = read_jdbc_table("employees")

salary_df = read_jdbc_table("salaries")

dept_mgr_df = read_jdbc_table("dept_manager")

titles_df = read_jdbc_table("titles")


'''
Show all employees and their max salary

SELECT * FROM
(SELECT e.emp_no, MAX(salary) FROM public.employees as e
INNER JOIN public.salaries s ON e.emp_no = s.emp_no
GROUP BY e.emp_no) as a
WHERE a.emp_no = 12940;
'''
max_salary_per_emp = employee_df.\
                        join(salary_df, employee_df["emp_no"] == salary_df["emp_no"]).\
                        drop(salary_df["emp_no"]).\
                        groupBy(col("emp_no")).\
                        agg(max(col("salary"))).show()

'''
Show all employees who were never managers
'''
emp_never_been_manager = employee_df.\
                            join(dept_mgr_df, employee_df["emp_no"] == dept_mgr_df["emp_no"], "left_anti").\
                            show()

'''
find the job titles of the best paid 10 employees in the company
'''
best_10_title_df = employee_df.\
                    join(titles_df, employee_df["emp_no"] == titles_df["emp_no"]).\
                    join(salary_df, employee_df["emp_no"] == salary_df["emp_no"]).\
                    groupBy(col("title")).\
                    agg(sum(col("salary")).alias("sum_salary")).\
                    sort(desc("sum_salary")).show()
