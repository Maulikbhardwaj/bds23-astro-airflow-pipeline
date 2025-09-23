"""
task flow api allows us to use decoratoers instead of operators such as python operator
task 1 to take an input number 
task 2 add 50 to te input number
task 3 multiply the result by 2
task 4 to divide the result by 10 

"""
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG( dag_id = "arithmetic_operations_tfapi" ) as dag:
    # to stast with the numebr 
    @task
    def start_number():
       initial_value = 100 
       print("starting number : {initial_value}")
       return initial_value
    
    @task
    def add_fifty(number):
        new_vlaue = number + 50 
        print (" Add fifit : {number} + 50 = {new_vlaue}")
        return new_vlaue
    @task
    def multiply_by_two(number):
        new_vlaue = number * 2 
        print (" multiply by two : {number} * 2 = {new_vlaue}")
        return new_vlaue
    @task
    def divide_by_ten(number):      
        new_vlaue = number / 10 
        print (" divide by ten : {number} / 10 = {new_vlaue}")
        return new_vlaue
    # task depedency
    start_value = start_number()
    second_value = add_fifty(start_value)
    third_value = multiply_by_two(second_value)
    fourth_value = divide_by_ten(third_value)

