# -*- coding: utf-8 -*-



from kafka import KafkaProducer

from time import sleep

import json, sys

import requests

import time



def getData(url):

    jsonData = requests.get(url).json()

    data = []

    labels = {}

    labels["Australia news"]=0

    labels["US news"]=1

    labels["Football"]=2

    labels["World news"]=3

    labels["Sport"]=4

    labels["Television & radio"]=5

    labels["Environment"]=6

    labels["Science"]=7

    labels["Media"]=8

    labels["News"]=9

    labels["Opinion"]=10

    labels["Politics"]=11

    labels["Business"]=12

    labels["UK News"]=13

    labels["Social"]=14

    labels["Life and style"]=15

    labels["Inequality"]=16

    labels["Art and design"]=17

    labels["Books"]=18

    labels["Stage"]=19

    labels["Film"]=20

    labels["Music"]=21

    labels["Global"]=22

    labels["Food"]=23

    labels["Culture"]=24

    labels["Community"]=25

    labels["Money"]=26

    labels["Technology"]=027

    labels["Travel"]=28

    labels["From The Observer"]=29

    labels["Fashion"]=30

    labels["Crosswords"]=31

    labels["Law"]=32



    for i in range(len(jsonData["response"]['results'])):

        headline = jsonData["response"]['results'][i]['fields']['headline']

        bodyText = jsonData["response"]['results'][i]['fields']['bodyText']

        headline += bodyText

        label = jsonData["response"]['results'][i]['sectionName'] 

        #data.append({'label':labels[label],'Descript':headline})

    if label in labels:

        toAdd=str(labels[label])+'||'+headline

            data.append(toAdd)

    return(data)



def publish_message(producer_instance, topic_name, value):

    try:

        producer_instance.send(topic_name, key=b'foo', value=value.encode('utf-8','ignore'))

        producer_instance.flush()

        print('Message published successfully.')

    except Exception as ex:

        print('Exception in publishing message')

        print(ex)



def connect_kafka_producer():

    _producer = None

    try:

        #_producer = KafkaProducer(value_serializer=lambda v:json.dumps(v).encode('utf-8'),bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)

         _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)

    

    except Exception as ex:

        print('Exception while connecting Kafka')

        print(ex)

    finally:

        return _producer



if __name__== "__main__":

    

    if len(sys.argv) != 4: 

        print ('Number of arguments is not correct')

        exit()

    

    key = sys.argv[1]

    fromDate = sys.argv[2]

    toDate = sys.argv[3]

    #t=open("train.txt","w")

    url = 'http://content.guardianapis.com/search?from-date='+ fromDate +'&to-date='+ toDate +'&order-by=newest&show-fields=all&page-size=200&%20num_per_section=10000&api-key='+key        

    all_news=getData(url)

    if len(all_news)>0:

        prod=connect_kafka_producer();

        for story in all_news:

            #print(json.dumps(story))

        	#t.write(json.dumps(story.encode("ascii","ignore")))

        	#t.write('\n')

            publish_message(prod, 'guardian2', story)

            time.sleep(1)

        if prod is not None:

                prod.close()