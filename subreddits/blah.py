
import csv
import os

for file in os.listdir("."):
	if file.endswith(".csv"):
		print(file)
		csvfile = open(file, encoding='utf-8')
		datareader = csv.DictReader(csvfile, delimiter = ',')
		print(datareader.fieldnames)