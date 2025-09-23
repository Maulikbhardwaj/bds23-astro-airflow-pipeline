"""
task 1 to take an input number 
task 2 add 50 to te input number
task 3 multiply the result by 2
task 4 to divide the result by 10 
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#function for each task 

def start_number(**context):
    context["ti"].xcom_push(key="current_value", value=100)
    print("Starting the number is 100 ")

def add_fifty(**context):
    current_value = context["ti"].xcom_pull(key="current_value", task_ids="start_number")
    new_value = current_value + 50
    context["ti"].xcom_push(key="current_value", value=new_value)
    print("add 50 : {current_value} +50 = {new_value}")

def multiply_by_two(**context):
    current_value = context["ti"].xcom_pull(key="current_value" , task_ids="add_fifty")
    new_value = current_value * 2
    context["ti"].xcom_push(key="current_value" , value = new_value)
    print("multiply by 2 : {current_value} *2 = {new_value}")

def divide_by_ten(**context):
    current_value = context["ti"].xcom_pull(key="current_value" , task_ids="multiply_by_two")
    new_value = current_value / 10
    context["ti"].xcom_push(key="current_value" , value = new_value)
    print("divide by 10 : {current_value} /10 = {new_value}")

#define dag 
with DAG (dag_id="arithmetic_operations") as dag:
    start_number = PythonOperator(
        task_id = "start_number" ,
        python_callable = start_number
        #provide_context = True ) 
    )
    add_fifty = PythonOperator(
        task_id = "add_fifty" ,
        python_callable = add_fifty
        #provide_context = True )
    )
    multiply_by_two= PythonOperator(
        task_id = "multiply_by_two" ,
        python_callable = multiply_by_two
        #provide_context = True )          
    )  
    divide_by_ten = PythonOperator(
        task_id = "divide_by_ten" ,
        python_callable = divide_by_ten
        #provide_context = True )
    )
#dependency
start_number >> add_fifty >> multiply_by_two >> divide_by_ten