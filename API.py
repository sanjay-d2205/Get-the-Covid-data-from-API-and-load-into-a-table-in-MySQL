import pandas as pd
import sqlalchemy
import mysql.connector

engine = sqlalchemy.create_engine('mysql://root:password@localhost/students')
#create dataframe by reading json file
df = pd.read_json('covid.json')
#load dataframe to table
df.to_sql('covid_data',engine)

mysqldb = mysql.connector.connect(host="localhost",user="root",password="password",database="students")
cursor = mysqldb.cursor()

#Write a select query to fetch and display all the columns
print("Write a select query to fetch and display all the columns")
qry="select * from covid_data"
cursor.execute(qry)
myresult = cursor.fetchall()
for x in myresult:
  print(x)

#Get the covid positive count on date wise
print("\n#Get the covid positive count on date wise")
qry2="select date,positive from covid_data"
cursor.execute(qry2)
myresult = cursor.fetchall()
for x in myresult:
  print(x)


#Get the monthly hospitalization count.
print("\nGet the monthly hospitalization count.")
qry3="SELECT  MONTH(date) AS month,sum(hospitalized) FROM covid_data group by month"
cursor.execute(qry3)
myresult = cursor.fetchall()
for x in myresult:
  print(x)


