# checks if ruleset detects pcaps
# requires iscan, pcap folder and rule file
# usage: python scan_pcaps2.py rulefile [rulefile] ...

import subprocess
import multiprocessing
import time
from subprocess import PIPE, Popen
import os
from multiprocessing.pool import ThreadPool
import sys

#--------------Settings----------------

numThreads = 10
iscan_path = "iscan"
testDir = "pcaps_ios/mingxiao1998-outlook/20150914/"

#--------------------------------------


def processPcaps(jobList, rules, testDir):
	
	ret = []
	cmd = "./" + iscan_path  + " -apkv"
	for r in rules:
		cmd = cmd + " -r '" + r + "'"

	for f in jobList:

		correct_detect = False
		high_weight = 0
		detection = ""

		appId = f.split(".")[0]
		p1 = Popen([cmd + " '" + testDir + f + "'"], shell = True, stdout=PIPE)
		resp = p1.communicate()[0]
		for line in resp.split("\n"):
			if line[:8] == "[attack]":

				weight = line.split("ios_app: [")[1].split("][")[0]
				if (weight > high_weight):

					detection = line.split("ios_app: ")[1].split(" ")[0]
					high_weight = weight
	
		if ("[" + appId + "]" in detection):

			correct_detect = True
			ret.append("[" + f + "] Correct_Detection")

		elif (detection == ""):

			detection = "No prune triggered"
			ret.append("[" + f + "] Failed_Detection: No prune triggered")

		else:
			ret.append("[" + f + "] Failed_Detection: " + detection)

	return ret


# get list of rule files
if (len(sys.argv) < 2):
	
	print "Usage: python scan_pcaps.py rulefile [rulefile] ..."
	sys.exit()


ruleFiles = []
for i in range(1, len(sys.argv)):
	ruleFiles.append(sys.argv[i])


# get file list and split into threads
threads = [];
fileList = os.listdir(testDir)
numSplit = int((len(fileList) + 1)/(numThreads * 1.0)) + 1

pool = ThreadPool(processes=numThreads)
for i in range(0, numThreads):
	threads.append(pool.apply_async(processPcaps, (fileList[i * numSplit: (i * numSplit) + numSplit], ruleFiles, testDir)))
	

# get results and generate statistics
correct = 0
failed = 0
wrong = 0
totalPcaps = 0
for i in range(0, numThreads):
	
	res = threads[i].get()
	
	for r in res:
	
		if ("Correct_Detection" in r):
			correct = correct + 1
		elif ("No prune triggered" in r):
			failed = failed + 1
		else:
			wrong = wrong + 1
		print r
		totalPcaps = totalPcaps + 1


# generate stats
print "Rule set: " + str(ruleFiles)
print "Directory: " + testDir
print "Total Pcaps: " + str(totalPcaps)
print "Correct detection: " + str((correct * 100.0/totalPcaps))[:4] + "%"
print "Failed to detect: " + str((failed * 100.0/totalPcaps))[:4] + "%"
print "Wrong detection: " + str((wrong * 100.0/totalPcaps))[:4] + "%"
if ((wrong + correct) != 0):
	print "Wrong/(Wrong + Correct): " + str((wrong * 100.0/(wrong + correct)))[:4] + "%"
else:
	print "Wrong/(Wrong + Correct): NaN%"
 

