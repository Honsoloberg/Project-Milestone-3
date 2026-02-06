# Project Milestone 2: Data Storage and Integration Connectors
## SOFE4630U-MS1

### Objectives
1. Get familiar with Docker images and containers.
2. Deploy tabular and key-value data storage using Google Kubernetes Engine (GKE).
3. Get familiar with key-value data storage.
4. Create and configure connectors from Google Pub/Sub to MySQL server.
5. Create and configure connectors from Google Pub/Sub to Redis server.

### Content
1. Labels.csv
2. Producer.py
    - Iterates over the "Labels.csv" and validates each value that it's a float, String or None.
    - Converts each row in CSV into JSON and serializes the message
    - Publishes the JSON objects to a google cloud topic.
        - Google Cloud then archives the messages into a mySQL database
        - Messages sit inside the Topic until they are consumed by a client
3. Consumer.py
    - Subscribes to a google cloud topic and consumes the JSON messages then formats the data to the termial.