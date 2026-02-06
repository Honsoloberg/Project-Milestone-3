from google.cloud import pubsub_v1
import glob
import json
import csv
import os                      
from time import sleep

files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]

project_id="project-milestones-485816" # Needs to be changed when used in different Projects
topic_name = "mnist_image"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

try:
    with open("Labels.csv", 'r') as file:
        csvFile = csv.DictReader(file)

        for line in csvFile:
            # Validate each value before it is converted to JSON
            time = None if line["time"] == "" else float(line["time"])
            profileName = None if line["profileName"] == "" else str(line["profileName"])
            temperature = None if line["temperature"] == "" else float(line["temperature"])
            humidity = None if line["humidity"] == "" else float(line["humidity"])
            pressure = None if line["pressure"] == "" else float(line["pressure"])

            # Structure dictionary message
            message = {"time" : time, "profileName" : profileName, "temperature" : temperature, "humidity" : humidity, "pressure" : pressure}

            # Convert message to JSON then serialize
            send = json.dumps(message).encode('utf-8')

            try:    
                # Publish the binary message to the google cloud topic
                future = publisher.publish(topic_path, send)
                
                # wait for the transaction to complete
                future.result()    
                print("The messages {} has been published successfully".format(message))
            except: 
                print("Failed to publish the message")
            
            sleep(.5) # wait is to ensure no messages are missed in sending. As the "future.result()" can still fail.

            # Uncomment the below line to send only one line (for testing)
            # break
except KeyboardInterrupt:
    #if the user wants to manually stop the script. Catch the the Interrupt then close the script.
    publisher.stop()
    exit(0)

#if the script completes then stop the publisher
publisher.stop()