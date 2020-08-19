import sqlite3  #importing sqlite3 module, to retrieve data from database.
import queue    #importing a queue module, to store tasks inside the queue.
import re       #importing a module for regular expressions, to check task's id.

queue = queue.Queue() #creating an empty queue object, which will store the tasks retrieved from the database.

#__________________________________________Defining Clock, Processor and Task classes______________________________
class Clock:
    def __init__(self, time): #Parameterized constructor.
        self.time = time #Self, refers to an instance(object) clock, which takes time value and updates the simulations time according to the earliest event.

class Processor:
    def __init__(self, id, busy_until, task_in_progress):#Parameterized  constructor.
        #Processor objects contain:
        self.id = id #id, (1,2 or 3).
        self.busy_until = busy_until #Attribute to represent task object's completion time.
        self.task_in_progress = task_in_progress #Attribute for containing a task object.

        #Callable attributes, used for updating the initial processor object's attributes:
    def assign_task(self, task, clock):
        self.task_in_progress = task #assigns the current task object to be processed.
        print(f"** [{clock.time}] : Task [{task.id}] assigned to the processor [{self.id}]. \n")
        self.busy_until = clock.time + task.duration #assigning current clock time with task's duration, represents task's completion time.

    def process_task(self, clock):
        clock.time = self.busy_until #when task gets completed, and that completion is the earliest event, then clock is set to task's comletion time.
        print(f"** [{clock.time}] : Task [{self.task_in_progress.id}] completed. \n")
        #making the processor available:
        self.busy_until = 0
        self.task_in_progress = None

    #Sorting the processors by their earliest task completion time:
    #Method continues on line 108, inside update_processors() function, looping through processor object list.
    #Method adopted and implemented from: https://stackoverflow.com/a/48731059
    def __eq__(self, other):    #__eq__ equals to:
        return self.busy_until == other.busy_until

    def __lt__(self, other):    #__lt__ less then:
        return self.busy_until < other.busy_until

class Task:
    def __init__(self, id, arrival, duration):#Parameterized constructor.
        #Task objects contain:
        self.id = id #attribute to contain 6 charachter string value, as task's id.
        self.arrival = arrival #attribute to contain float value, as task's arrival time.
        self.duration = duration #attribute to contain intager value, as task's duration.
#_____________________________________________________________________________________________________________________

def retrieve_data(): #retrieving tasks from the database, which are created by running Q1_c1968201.py module.
    conn = sqlite3.connect("database.db") #connect to database "database.db".
    cursor = conn.cursor() #initialise cursor, through which queries are executed.

    # Retrieving and storing tasks inside a variable, in a form of a list of tuples,
    # from respective task_id, arrival_time, duration_time columns.
    data = cursor.execute("SELECT task_id, arrival_time, duration_time FROM DataTable ORDER BY arrival_time ASC")
    for task in data:
        queue.put(task) #inserting tasks as tuples inside the empty queue.


    conn.commit() #commit the transactions.
    conn.close()  #closing connection to the database.
    return queue #object contains 100 tasks, as tuples.



#Method adopted and implemented based on:
#https://stackoverflow.com/questions/1559751/regex-to-make-sure-that-the-string-contains-at-least-one-lower-case-char-upper
#https://www.geeksforgeeks.org/python-program-check-string-contains-special-character/

#if the generated task's id contains at least 3 types of the required charachters,
#the below function accepts the task to be assigned to the available processor.

def is_accepted(id): #function takes task's id as an argument, to check if it can be validated.
    validation = 0

    #Using regular expressions.
    lower = re.compile('[a-z]')         #lower case letter
    capital = re.compile('[A-Z]')       #upper case letter
    number = re.compile('\d')           #digit
    special = re.compile('[-_@#*&]')    #special character

    #if contains one of the required charachters, update validation variable by 1, scoring at least 3, returns True (validated).
    if lower.search(id):
        validation += 1
    if capital.search(id):
        validation += 1
    if number.search(id):
        validation += 1
    if special.search(id):
        validation += 1
    if validation >= 3:
        return True


def create_processors():
    processors = []
    for i in range(3):
        processors.append(Processor(i+1, 0, None)) #creates 3 objects, as processors(1,2,3) from class Processor and appends it into the empty list processors.
    return processors

def get_free_processor(processors):
    for processor in processors: #if two or three processors are available, will return based on processor's id, priority starting from processor 1.
        if not processor.task_in_progress:
            return processor   #if the processor has no task assigned to it, returns the available processor.
    return None #if all processors are busy, returns none.

def determine_if_has_held_tasks(held_tasks):#checks the list held_tasks, if it has any tasks on hold.
    if held_tasks:
        return True
    else:
        return False

def update_processors(processors, clock, next_task=None, has_held_tasks=False, finish_entering=False, no_more_tasks_left=False):
    # Loop processors sorted in ascending order by their busy_until (assigned task's completion time) using the method adopted and implemented from https://stackoverflow.com/a/48731059
    # See __eq__ and __lt__ in Processor class.

    for processor in sorted(processors):
        if not finish_entering: # If tasks are still entering the system,

            if processor.task_in_progress and processor.busy_until <= next_task.arrival:
                # and if a processor has a task and it will be completed before the next task arrives.

                processor.process_task(clock)
                # Then processor object invokes process_task(clock) callable attribute, completes its task and frees up the processor.

                if has_held_tasks:
                    break
                    #If there are tasks on hold, only free up processor with shortest busyness and stop the loop, because a task will need to be assigned to the free processor.
                    #However, if there are no on hold tasks, keep going through the loop, completing tasks.

        else: #if no new tasks are entering the system
            if processor.task_in_progress: #processor has a task inside, then complete it.
                processor.process_task(clock)
                if not no_more_tasks_left:
                #if True on no_more_tasks_left, but still have something in processors loop does not break and completes all 3 processors.
                    break
                    #break the loop here, because only need the earliest free processor to assign whatever is on hold, do not want all of them processing before assigning on hold task.

if __name__ == "__main__": #indicates script is being run as main module, refers to the scripts name.

    tasks = retrieve_data()          #Object with 100 tasks.
    processors = create_processors() #A list containing 3 processors.
    clock = Clock(0)                 #Clock object created.
    held_tasks = []                  #Empty list to contain on hold tasks.

    print("** SYSTEM INITIALISED **\n")


    for x in range(tasks.qsize()): #looping through the queue, with tuples.
        x = tasks.get()            #holds a tuple.
        id, arrival, duration = x  #unpacks the tuple.
        task = Task(id, arrival, duration) #creates a task object with tuple's values.
        has_held_tasks = determine_if_has_held_tasks(held_tasks)  #checks if there are any on hold tasks.
        update_processors(processors, clock, task, has_held_tasks) #has_held_tasks will only be passed, if the above variable is true.

        #retrieve a free processor.
        free_processor = get_free_processor(processors)

        #if there is a free processor and tasks on hold:
        while free_processor and held_tasks:

            #takes the first held task in held_tasks list, and passes it into the free processor, together with the clock value.
            first_held_task = held_tasks.pop(0)
            free_processor.assign_task(first_held_task, clock)

            #if there are no on hold tasks, then next line(update_processors), continues simulation just processing arriving tasks as standard.
            has_held_tasks = determine_if_has_held_tasks(held_tasks)

            #However if has_held_tasks == True, then will update processors and break again on line 114 in order to deal with other on held tasks.
            update_processors(processors, clock, task, has_held_tasks)

            #here need this line to check if continue loop for processing held tasks, because if even have held tasks, but no free processors, then need to stop.
            free_processor = get_free_processor(processors)


        clock.time = task.arrival #set clock time to task object's arrival time.
        print(f"** [{clock.time}] : Task [{task.id}] with duration [{task.duration}] enters the system. \n")


        if is_accepted(id) == True: #filter task's id if it is accepted or denied
            print(f"** Task [{task.id}] accepted. \n")
            free_processor = get_free_processor(processors) #if accepted need a free processor to assign the task.
            if free_processor:
                #if a processor is available, then assign the task to that processor with current simulation's time.
                free_processor.assign_task(task, clock)
            else:
                #otherwise print task is on hold and add it to the held_tasks list.
                print(f"** Task [{task.id}] is on hold. \n")
                held_tasks.append(task)
        else:
            #If is_accepted(id) is False, then deny the task and print appropriate statement:
            print(f"** Task [{task.id}] unfeasible and discarded. \n")


####### THIS APPLIES AFTER FINISHED GOING THROUGH THE WHOLE TASK'S QUEUE, NO NEW ENTRIES INTO THE SYSTEM ##########

    free_processor = get_free_processor(processors) #retrieve a free processor.
    while held_tasks:       #if there are tasks on hold,
        if not free_processor:  #but no free processors available

            #complete the task inside the processor and retrieve the available processor. No new tasks passed.
            update_processors(processors, clock, None, False, True)
            free_processor = get_free_processor(processors)

        #take the first task on hold inside the held_tasks list and pass it in to the free processor.
        first_held_task = held_tasks.pop(0)
        free_processor.assign_task(first_held_task, clock)

        #continue processing tasks as standard.
        update_processors(processors, clock, first_held_task, False, True)
        free_processor = get_free_processor(processors)


    # No more held tasks, so just make all processors finish their current tasks.
    if not held_tasks:

        #if no more tasks entering the system and no more on hold, then set "no_more_tasks_left" as True, but complete the last ones assigned to processors.
        update_processors(processors, clock, None, False, True, True)

    print(f"\n** [{clock.time}] : SIMULATION COMPLETED. **")
