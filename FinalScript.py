#This Script is based on Python3 and is required to create Kafka-Consumers Calling BLAST and performing the analysis of the incoming data
from __future__ import absolute_import

import collections
import itertools
import logging

from kafka.vendor import six

from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment
from kafka.structs import TopicPartition


from kafka import KafkaConsumer
import subprocess
import time

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


start = time.time()
consumer=KafkaConsumer("test3", group_id="test-consumer-group",  bootstrap_servers = ["localhost:9092"])
ingresso=""
def callblast(incoming, filename, database= "human.1.protein.faa"):
        with open(filename, "w") as f:
                f.write(incoming)
        cmd=f"blastp -query {filename} -db {database} -evalue 1e-5 -outfmt 6 -max_target_seqs 1"
        s=subprocess.check_output(cmd.split(" "))
        end = time.time()
        print(s.decode())
        print("time occurred:", (end-start))
K=500
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
