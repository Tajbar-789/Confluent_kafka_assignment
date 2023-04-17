import mysql.connector
import datetime
# import mysql.connector
#create user 'user'@'%' identified by 'password'
try:
  mydb = mysql.connector.connect(
  host="localhost",
  user="abc",
  password="password",
  database="newdb")

  mycursor = mydb.cursor()
except:
  print("Connection failed")

mycursor.execute("create table if not exists emp (emp_id int,emp_name varchar(25),salary int, dept_id int ,updated_at TIMESTAMP );")
mydb.commit()
while True:
  emp_details=input("Enter the details of the employee\n").split(" ")
  emp_id=int(emp_details[0])
  emp_name=str(emp_details[1])
  salary=int(emp_details[2])
  dept_id=int(emp_details[3])

  query="INSERT INTO emp (emp_id,emp_name,salary,dept_id,updated_at) VALUES (%s,%s,%s,%s,%s)"
  record=(emp_id,emp_name,salary,dept_id,datetime.datetime.now().replace(microsecond=0))
  mycursor.execute(query,record)
  mydb.commit()





