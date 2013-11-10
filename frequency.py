# Kevin Mangan
# kmm2256@columbia.edu

# ASSUMPTIONS
# input file is called input.txt and the program will generate results.tsv
# words are defined as things separated by a whitespace in the input file

# To get around python's GIL, each subtask is run in a separate process. This is sort of a map/reduce approach, as the
# script breaks the large file into manageable chunks and then gives it to a worker process, and then aggregates the data at the end.
# Newer versions of python have disallowed marshaling defaultdicts, so I opted to use pickling instead.


import sys
from collections import defaultdict

import threading, Queue, subprocess
import pickle, struct
import re, csv

CPUS = 4

executable = [sys.executable]

def process(file, chunk):
    f = open(file, "rb")
    f.seek(chunk[0])
    d = defaultdict(int)
    for line in f.read(chunk[1]).splitlines():
        for word in line.split(' '):
            #re.sub(r'[^a-zA-Z0-9]','', word)
            d[word] += 1
    return d

def getchunks(file, size=1024*1024*1024):
    # yield sequence of (start, size) chunk descriptors
    f = open(file, "rb")
    while 1:
        start = f.tell()
        f.seek(size, 1)
        s = f.readline() # skip forward to next line ending
        yield start, f.tell() - start
        if not s:
            break

def putobject(file, object):
    data = pickle.dumps(object)
    file.write(struct.pack("I", len(data)))
    file.write(data)
    file.flush()

def getobject(file):
    try:
        n = struct.unpack("I", file.read(4))[0]
    except struct.error:
        return None
    return pickle.loads(file.read(n))

class Worker(threading.Thread):
    def run(self):
        # we could save some overhead by forking, but this is more portable
        process = subprocess.Popen(
            executable + [sys.argv[0], "--process"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE
            )
        stdin = process.stdin
        stdout = process.stdout
        while 1:
            cmd = queue.get()
            if cmd is None:
                stdin.write("0\n")
                break
            putobject(stdin, cmd)
            result.append(getobject(stdout))
            queue.task_done()

# --------------------------------------------------------------------

import time, sys

timer = time.time

t0, t1 = timer(), time.clock()

if "--process" in sys.argv:

    stdin = sys.stdin
    stdout = sys.stdout
    while 1:
        args = getobject(stdin)
        if args is None:
            sys.exit(0) # terminate
        result = process(*args)
        putobject(stdout, result)

else:

    FILE = "input.txt"

    queue = Queue.Queue()
    result = []

    # fire up a bunch of workers (typically one per core)
    for i in range(CPUS):
        w = Worker()
        w.setDaemon(1)
        w.start()

    chunksize = 1 # megabyte

    # distribute the chunks
    for chunk in getchunks(FILE, chunksize*1024*1024):
        queue.put((FILE, chunk))

    # wait for the tasks to finish
    queue.join()

    # merge the incoming data
    count = defaultdict(int)
    for item in result:
        for key, value in item.items():
            count[key] += value


    print timer() - t0, time.clock() - t1

# --------------------------------------------------------------------

    writer = csv.writer(open('results.tsv', 'wb'), delimiter='\t')
    for key, value in count.items():
        writer.writerow([key, value])

