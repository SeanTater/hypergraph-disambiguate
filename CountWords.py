import sys
from collections import Counter

lines = []
words = []
# Get the command line arguments
args = sys.argv

# Get the name of the file to count the words in
filename = args[1]
freqDist={}
# Get the words to not count
#skipwords = args[2].split(' ')

# Loop through the file and read each line into our lines list

for line in open(filename):
    lines.append(line)

# Get the number of lines read
numlines = len(lines)

counter = Counter()
# Loop through the lines list and place the words into the word list
for line in lines:
    # Split each line into individual words
    for word in line.split(' '):
   # Make sure the word is not in our list of words to skip
       # if word not in skipwords:
	# If the word is not in our skip list, add it to our word list
        counter.update(word)
             			
    result = dict(counter)
# Show the number of words and the number of lines
    print (result)