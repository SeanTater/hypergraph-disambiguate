#!/usr/bin/python

import sqlite3
conn = sqlite3.connect("wordcounts.db")

import leveldb
output = leveldb.LevelDB("phrasecounts-ldb")

# Looking for changes in para_id avoids the n+1 query problem
last_para_id = -1
cache = {}
wordgroup = set()
phrasegroup = set()

def dump_cache(cache, output):
    update = leveldb.WriteBatch()
    for phrase, count in cache.iteritems():
        try:
            update.Put(phrase, str(int(output.Get(phrase)) + cache[phrase]))
        except KeyError:
            update.Put(phrase, str(cache[phrase]))
    output.Write(update)
    cache.clear()

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
                        
        if len(cache) > 10000000:
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