from __future__ import absolute_import

import collections
import itertools
import logging

from kafka.vendor import six

from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment
from kafka.structs import TopicPartition
from threading import Thread

from kafka import KafkaConsumer
import subprocess 
import time
import concurrent.futures
import threading

log = logging.getLogger(__name__)

class RoundRobinPartitionAssignor(AbstractPartitionAssignor):

	name = 'roundrobin'
	version = 0

	@classmethod

	def assign(cls, cluster, member_metadata):
		all_topics = set()
		for metadata in six.itervalues(member_metadata):
			all_topics.update(metadata.subscription)
		all_topic_partitions=[]
		for topic in all_topics:
			partitions=cluster.partitions_for_topic(topic)
			if partitions is None:
				log.warning('No partition metadata for topic %s', topic)
				continue
			for partition in partitions:
				all_topic_partitions.append(TopicPartition(topic, partition))
			all_topic_partitions.sort()

		assignment=collections.defaultdict(lambda: collections.defaultdict(list))

		member_iter=itertools.cycle(sorted(member_metadata.keys()))
		for partition in all_topic_partitions:
			member_id=next(member_iter)

		while partition.topic not in member_metadata[member_id].subscription:
			member_id=next(member_iter)
		assignment[member_id][partition.topic].append(partition.partition)


		protocol_assignment={}
		for member_id in member_metadata:
			protocol_assignment[member_id]=ConsumerProtocolMemberAssignment(
				cls.version,
				sorted(assignment[member_id].items()),
				b'')
		return protocol_assignment

	@classmethod
	def metadata(cls, topics):
		return ConsumerProtocolMemberMetadata(cls.version, list(topics), b'')


	@classmethod
	def on_assignment(cls, assignment):
		pass


class ThreadWithResult(Thread): #extend Thread, put the callblast output in self.result
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, *, daemon=None):
        def function():
            self.result = target(*args, **kwargs)
        super().__init__(group=group, target=function, name=name, daemon=daemon)


def multithread(incoming, num_thread):
	thread_list=[] #create a list of thread 
	queries=[incoming[j::num_thread] for j in range(num_thread)] #create a list of n elements (depending on num_threads)
	for i in range (num_thread):
		query = "".join(queries[i]) #join the elements of the i-est list concateneting them in a string
		t=ThreadWithResult(target=callblast,args=(query, f"seq_analysis{i}.txt")) #it's a class, create a single thread
		t.start() #start the thread
		thread_list.append(t) # insert every thread in thread_list
	result = "" #string containing callblast results 
	for t in thread_list:
		t.join() # wait for the thread to finish 
		result += t.result + "\n" # insert output on results
	end = time.time()
	print(result)
	print("time occurred:", (end-start))

def worker(incoming): #function to call callblast
	query="".join(incoming) #create the query, join the elements of the list concateneting them in a string
	filename = f"seq_analysis{threading.get_ident()}.txt" # return the univoque number of the thread
	result=callblast(query,filename)  # really?
	end = time.time() 
	print(result)
	print("time occurred:", (end-start))


start = time.time()
consumer=KafkaConsumer("test6", group_id="test-consumer-group", bootstrap_servers = ["192.168.20.26:9092"])
ingresso=""
def callblast(incoming, filename, database= "human.1.protein.faa"):
	with open(filename, "w") as f:
		f.write(incoming)
	cmd=f"blastp -query {filename} -db {database} -evalue 1e-5 -outfmt 6 -max_target_seqs 1"
	s=subprocess.check_output(cmd.split(" "))
	return s.decode() 
K=200
num_threads=4
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor: #tells how may thread i want to use 
	futures=[] #create an empty list of objects required to know that the opertion of the executor is concluded
	finished = False 
	while 1 > 0:
		ingresso=[] 
		poll = consumer.poll(timeout_ms = 1000, max_records = K)
		if not poll:
			if not finished:
				finished = True
				print("Done! All the sequences have been analyzed") 
			continue
		finished = False
		for tp,records in poll.items():
			for record in records:
				record=(record.value).decode()
				ingresso.append(record.replace("<","\n"))
		future=executor.submit(worker,ingresso) #submit the job to the thread pool and obtain the future to know if it's completed
		futures.append(future) #append it to the list
		if len(futures) >= num_threads+1: #if we submitted enough elements (at least all threads are full) 
			done,notdone = concurrent.futures.wait(futures,return_when=concurrent.futures.FIRST_COMPLETED) #wait for a submitted element to be completed
			futures = list(notdone) #continue to monitor the unfinished elements
			
		#multithread(ingresso,4)








#### blastp -query cow.medium.faa -db human.1.protein.faa -evalue 1e-5 -outfmt 6 -max_target_seqs 1
