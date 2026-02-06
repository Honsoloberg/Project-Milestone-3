from google.cloud import pubsub_v1
import glob
import json
import os 
from time import sleep

files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]

project_id="project-milestones-485816" # Needs to be changed when used in different Projects
subscription_id = "mnist_predict-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}..\n")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # deserialize the message back into JSON then convert it to a Python dictionary
    message_data = json.loads(message.data.decode('utf-8'))

    #break each data point into a formatted string
    location = "" if message_data.get("profileName") is None else "Location: {}\n".format(message_data["profileName"])
    time = "" if message_data.get("time") is None else "Time: {}\n".format(message_data["time"])
    temp = "" if message_data.get("temperature") is None else "Temperature: {}\n".format(message_data["temperature"])
    humid = "" if message_data.get("humidity") is None else "Humidity: {}\n".format(message_data["humidity"])
    pressure = "" if message_data.get("pressure") is None else "Pressure: {}\n".format(message_data["pressure"])

    #format final output by combining the formatted strings
    print("{}{}{}{}{}".format(time, location, temp, humid, pressure))
    print("<---------------------------------------------------------------------->")
   
    message.ack()
    
with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        # wait Keyboard Interrupts are now blocked. 
        # result() isn't used so Keyboard Interrupts aren't blocked.
        while True:
            sleep(1)
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        subscriber.close()
        exit(0)
