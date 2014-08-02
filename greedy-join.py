#!/usr/bin/python

import sqlite3
conn = sqlite3.connect("wordcounts.db")

output = open("phrasecounts.list", "w+")
import struct

# Looking for changes in para_id avoids the n+1 query problem
last_para_id = -1
cache = {}
wordgroup = set()
phrasegroup = set()

def dump_cache(cache, disk):
    block = bytearray(disk.read(4096))
    while block:
        for i in range(len(block)):
            length = block[i]; i += 1
            word = str(block[i:i+length]); i += length
            count = struct.unpack("I", block[i, i+4]); # No i += 4
            
            if word in cache:
                block[i:i+4] = struct.pack("I", count + cache[word])
                del cache[word]
            
        disk.seek(-len(block), 1)
        disk.write(block)
        block = bytearray(disk.read(4096))

    block = []
    block_length = 0
    while cache:
        word, count = cache.popitem()
        if len(word) > 255:
            # Delete 256+ character phrases
            continue
        item = chr(len(word)) + word + struct.pack("I", count)
        if block_length + len(item) > 4096:
            disk.write(''.join(block))
            block = []
            block_length = 0
        block.append(item)
        block_length += len(item)
    disk.write(''.join(block))
    disk.flush()

# This makes the (usually invalid) assumption that the data will be returned sorted by para_id
# We populated the DB in a way that we know this is the case but normally you would use "order by para_id"
for word, count, para_id in conn.execute("select word, count, para_id from word_counts;"):
    word = word.encode('utf8')
    if para_id == last_para_id:
        wordgroup.add(word)
    else:
        for word in wordgroup:
            if word not in cache:
                cache[word] = 1
            else:
                cache[word] += 1
                phrasegroup.add(word)
        for word1 in phrasegroup:
            for word2 in phrasegroup:
                if word1 < word2:
                    phrase = word1 + " " + word2
                    if phrase not in cache:
                        cache[phrase] = 1
                    else:
                        cache[phrase] += 1
                        
        if len(cache) > 50000000:
            dump_cache(cache, output)
        phrasegroup.clear()
        wordgroup.clear()
        last_para_id = para_id
        if (para_id % 1000) == 0:
            print para_id

dump_cache(cache, output)

""" Phrases intro:

        while len(wordgroup):
            next_wordgroup = set()
            for word in wordgroup:
                if word not in examined:
                    output[word] = "1"
                else:
                    output[word] = str(int(output[word]) + 1)
                    for word in 
            wordgroup = next_wordgroup
"""
