import sched
import time

scheduler = sched.scheduler(time.monotonic, time.sleep) # monotonic timing, no drawbacks
interval = 60
def event_handler():
    # Code for the event or task
    print("event")
    scheduler.enter(interval, 1, event_handler)  # Schedule the next iteration

scheduler.enter(0, 1, event_handler)  # Schedule the initial iteration, immediately
scheduler.run()  # Start the scheduler