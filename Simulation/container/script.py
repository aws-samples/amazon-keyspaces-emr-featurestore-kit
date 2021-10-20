#!/usr/bin/env python

import logging
import random
import argparse

log = logging.getLogger()
log.setLevel('ERROR')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

import io
import os
import boto3
import glob
import json
import time
import timeit
import pandas as pd
from multiprocessing import Pool
from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevConnection
from concurrent.futures import ThreadPoolExecutor
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider

global workerNo
global simulation_name
global KEYSPACE
global cluster
global session
global household_id
global num_processes
global requests


workerNo = int(os.environ['workerNo'])
num_processes = int(os.environ['num_processes'])
requests = int(os.environ['requests'])
KEYSPACE = "my_keyspace"
simulation_name = os.environ['simulation_name']
keyspaces_username = str(os.environ['keyspaces_username'])
keyspaces_password = str(os.environ['keyspaces_password'])
s3_bucket_name = str(os.environ['s3_bucket_name'])



# ---------
# Load IDs
# ---------
s3 = boto3.client('s3')
obj = s3.get_object(Bucket='YOUR S3 BUCKET HERE', Key='Dataset/daily_dataset.csv')
df = pd.read_csv(io.BytesIO(obj['Body'].read()))
household_id = df["household_id"].unique().tolist()


# ----------------------------
# Make Requests to Key Spaces
# ----------------------------
def start_process(procnum):
    try:
        print("Starting Process number", procnum)
        print("Current Working Dir", os.getcwd())
        file_object = open(os.getcwd() + "/simdata/worker_" + str(workerNo) + "_thread_" + str(procnum) +".txt", "a")
        list_times = []
        ssl_context = SSLContext(PROTOCOL_TLSv1_2 )
        # Place your certificate file (sf-class2-root.crt) in this folder for testing the simulation. For your own use cases
        # which might be sensitive and production environments should consider changing to alternative and more secure solutions.
        ssl_context.load_verify_locations(os.getcwd() + '/sf-class2-root.crt')
        ssl_context.verify_mode = CERT_REQUIRED
        #TODO Add your Keyspaces username and password
        auth_provider = PlainTextAuthProvider(username=keyspaces_username, password=keyspaces_password)
        global cluster
        cluster = Cluster(['cassandra.eu-west-1.amazonaws.com'],
                          ssl_context=ssl_context,
                          auth_provider=auth_provider, port=9142)
        global session
        session = cluster.connect(keyspace='my_keyspace',
                                  wait_for_all_pools=True)
        list_ids = []
        for i in range(requests):
            list_ids.append(random.randint(0,5565))
        global household_id
        for i in range(len(list_ids)):
            # ToDO make the id between 1-150
            query= "SELECT * FROM my_keyspace.energy_data_features WHERE id='" + str(household_id[list_ids[i]][0]) + "';"
            try:
                t_now = time.time()
                rows = session.execute(query)
                list_times.append(str(workerNo) + "," + str(procnum) + "," + str(time.time() - t_now) + "," + str(t_now))
            except:
                print("Failed at worker", workerNo)
                print("Failed Request", i)
                pass
        #global simulation_name
        for s in list_times:
            file_object.write(s + '\n')
        file_object.close()
        s3 = boto3.resource('s3')
        s3.Bucket(s3_bucket_name).upload_file(os.getcwd() + "/simdata/worker_" + str(workerNo) + "_thread_" + str(procnum) +".txt", "simulation_results/" + str(simulation_name) + "/worker_" + str(workerNo) + "_thread_" + str(procnum) +".txt")
    except:
        print("Thread Failed")
        raise Exception('Thread Failed') 


def main():
    if not os.path.exists(os.getcwd() + "/simdata"):
        os.makedirs(os.getcwd() + "/simdata")

    files = glob.glob(os.getcwd() + '/simdata/*')
    for f in files:
        os.remove(f)

    print("-----------------------")
    print("Starting the simulation")
    print("Threads::", num_processes)
    print("Requests::", requests)
    print("Number of workers::", workerNo)
    print("-----------------------")

    start_process(workerNo)


main()