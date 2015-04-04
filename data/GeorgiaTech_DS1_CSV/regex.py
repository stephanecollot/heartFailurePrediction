# Regex to clean encounter.csv
# Use: python27 regex.py encounter.csv
# Then replace encounter.csv with regexOut.csv

import fileinput
import re

f = open('regexOut.csv','w')

for line in fileinput.input():
  match = re.match(r'^(?!Exact)', line.rstrip())
  line = re.sub('\s', '', line)
  if match:
    f.write(line)
  else:
    f.write('\n'+line)

f.close()