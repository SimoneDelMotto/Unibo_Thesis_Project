# This script is made in Python3, it is used as launch command for the Flume configuartion file, it splits the input FASTA file in a way that when Kafka performs a Round Robin 
# the whole protein' sequence is sent to the consume instead of a single line

import sys

if len(sys.argv) < 2: #check what has been sent to the tool
        print ("protein file missing")
        exit()

path=sys.argv[1] #takes the element 1 (not 0!) of the argument given
with open(path,"r") as r:
        readfile=r.read() #read the file
        elements=readfile.split(">")[1:] #create an array for each protein starting from the first > (there is nothing before it)
for element in elements:
        element=element.replace("\n","<")
        print(">"+element)
