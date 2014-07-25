import sys
from collections import Counter
import code


words = []
# Get the command line arguments
args = sys.argv

# Get the name of the file to count the words in
filename = args[1]

# Loop through the file and read each line into our lines list
f = open(filename,encoding="utf8")
data = f.read()
paragraph = data.split('\n\n')
#for line in open(filename):
# 	lines.append(line)

code.interact(local=vars())

# Get the number of lines read
#numlines = len(lines)
for line in paragraph:
	counter = Counter()
	# Loop through the lines list and place the words into the word list
    # Split each line into individual words
    #for word in line.split(' '):
	counter += Counter(line.split(' '))
	result = dict(counter)
	# Show the number of words and the number of lines
	sys.stdout.write(str(result))
	