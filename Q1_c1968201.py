import random #importing random module to generate task's items.
import string #importing string module to generate capital and lowercase letters.
import math #importing math module to utilise ceiling ceil() method, when rounding up duration time.
import sqlite3 #importing sqlite3 module to initialise a database.


def create_id(): #Creating task's id.
    letters = string.ascii_letters #Capital and lowercase letters, Method retrieved from https://stackoverflow.com/questions/2823316/generate-a-random-letter-in-python
    special_chr = "-_@#*&"
    numbers = "".join([]) #join numbers into a string
    for num in range(10):
        numbers += str(num)

    characters = letters + special_chr + numbers #All Id Characters joined together.
    #Generate ID
    id = "".join(random.choices(characters, weights=None, cum_weights=None, k=6)) #Method retrieved from: https://www.w3schools.com/python/ref_random_choices.asp
    return id

def arrival_time(): #Generate arrival time.
    arrival = random.uniform(0,100) #Return a random floating point number. #Method retrieved: https://docs.python.org/3/library/random.html
    return arrival

def duration_time(): #Generate duration time.
    distribution = random.expovariate(1) #exponential distribution.
    duration = math.ceil(distribution) #"rounding up to ceiling value of x, the smallest intager not less than x" Retrieved from: https://www.geeksforgeeks.org/floor-ceil-function-python/
    return duration

def create_tasks(db_size): #Generate task.
    tasks = []
    for task in range(100 - db_size): #db_size (line 63) used to ensure inserting only 100 tasks into the database.
        task = [(create_id(), arrival_time(), duration_time())] #inserting generated id, arrival time and duration into a tuple, which represents a task.
        tasks += task #storing tasks into a list.
    return tasks

def create_database():
    db = sqlite3.connect("database.db") #Creating a database, named "database.db".
    cursor = db.cursor() #Initialise cursor, through which queries are executed.

    #Database's column headings.
    heading0 = "PRIMARY_KEY INTEGER PRIMARY KEY AUTOINCREMENT"
    heading1 = "task_id TEXT NOT NULL"
    heading2 = "arrival_time REAL NOT NULL"
    heading3 = "duration_time INTEGER NOT NULL"

    #Create a table named DataTable inside the database.
    sql_table = f"CREATE TABLE DataTable ({heading0},{heading1},{heading2},{heading3})"
    try:
      cursor.execute(sql_table)
      print("database.db created.")
    except sqlite3.OperationalError:    #if the database already exists, then the following message will be displayed.
      print("database.db already exists.")

    db.commit() #commit transactions.
    db.close()  #closing connection to the database.

def insert_tasks(): #Insert the created tasks into the database.
    conn = sqlite3.connect("database.db") #connect to the database.
    cursor = conn.cursor() # Again initialise the cursor.
    db = cursor.execute("SELECT COUNT(*) FROM DataTable") #Count all the rows inside the database table DataTable.
    (db_size,)=cursor.fetchone() #Method Retrieved from: https://stackoverflow.com/questions/2511679/python-number-of-rows-affected-by-cursor-executeselect?fbclid=IwAR3W1bl8w8nuFTF_diOgqLwAIULa6nqTy9M6A0RBtbbep6rTkJaTZTt786s
    #db_size returns an intager showing the number of rows inside the database.

    #ensuring that 100 tasks are inserted into the database.
    if db_size <100:
        try:    #iterating over 100 tasks and inserting in the DataTable at respective columns.
            cursor.executemany("INSERT INTO DataTable (task_id, arrival_time, duration_time) VALUES (?,?,?)", create_tasks(db_size))
        except:
            print("Failed to insert tasks into the database, check DataTable")

    conn.commit()
    conn.close()

if __name__ == "__main__": #indicates script is being run as main module, refers to the scripts name.
    #when script runs, database will be created and tasks inserted.
    create_database()
    insert_tasks()
