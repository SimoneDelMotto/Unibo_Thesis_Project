#This Script is based on Python3 and is required to create Kafka-Consumers Calling BLAST and performing the analysis of the incoming data

from kafka import KafkaConsumer
import subprocess
import time
start = time.time()
consumer = KafkaConsumer("Cluster", bootstrap_servers = ["localhost:9092"])
ingresso=""
def callblast(incoming, filename, database= "human.1.protein.faa"):
        with open(filename, "w") as f:
                f.write(incoming)
        cmd=f"blastp -query {filename} -db {database}"
        s=subprocess.check_output(cmd.split(" "))
        end = time.time()
        print(s.decode())
        print("time occurred:", (end-start))
K=100
while 1 > 0:
        ingresso=""
        poll = consumer.poll(timeout_ms = 1000, max_records = K)
        if not poll:
                continue
        for tp,records in poll.items():
                for record in records:
                        record=(record.value).decode()
                        ingresso += record.replace("<","\n")
        callblast(ingresso, "seq_analysis.txt")

        
# exaple of possible BLAST analysis with more specific parameters:
# blastp -query <query_file> -db <database> -evalue 1e-5 -outfmt 6 -max_target_seqs 1
