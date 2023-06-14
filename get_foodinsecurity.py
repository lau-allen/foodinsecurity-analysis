#importing libraries 
import pyspark 
import sys
import json
import os 

#function to extract csv data for rdd
def extract_products_data(index,rows):
  #skip header
  if index == 0:
    next(rows)
  #read csv data yielding entire row of data 
  import csv
  reader = csv.reader(rows)
  for row in reader:
    #checking if index 2 of the row is N/A
    if row[2] != 'N/A':
      yield tuple(row)

#function to extract csv data for rdd
def extract_items_data(index,rows):
  #skip header
  if index == 0:
    next(rows)
  #read csv data yielding entire row of data
  import csv
  reader = csv.reader(rows)
  for row in reader:
      yield tuple(row)

#define generator function for extracting data from keyfood_nyc_stores.json
def extract_json_data(json_f):
  #opening file 
  with open(json_f) as f:
    #loading json file using json.load
    data = json.load(f)
    #looping through all the keys
    for key in data.keys():
      #for each record, extract the needed info 
      record = data[key]
      yield (record['name'],(record['communityDistrict'],record['foodInsecurity']))

#function to open csv file to read in rdd 
def open_csv(file):
  import csv
  with open(file) as f:
    data = csv.reader(f)
    for i,row in enumerate(data):
      if i == 0:
        continue
      yield tuple(row)

#main
def main():
    #spark session
    sc = pyspark.SparkContext.getOrCreate()

    #convert products csv into rdd, while extracting needed data and manipulating data
    #get second part of UPC code and get float for price
    products = sc.textFile('gs://bdma/data/keyfood_products.csv',use_unicode=True).mapPartitionsWithIndex(extract_products_data).map(lambda x: (x[2].split('-')[1],(x[0],float(x[5].split(u'\xa0')[0].replace('$','')))))

    #convert samples csv into rdd, extracting the UPC code, product name
    sample_items = sc.parallelize(open_csv('keyfood_sample_items.csv')).map(lambda x: (x[0].split('-')[1],(x[1],))) 

    #convert into json into rdd
    nyc_stores = sc.parallelize(extract_json_data('keyfood_nyc_stores.json'))

    #generate output of product name, price, and food insecurity 
    #joining the sample_items, products, and nyc_stores rdd to get desired output 
    output = sample_items.join(products).mapValues(lambda x: x[0]+x[1]).map(lambda x: (x[1][1],(x[1][0],x[1][2]))).join(nyc_stores.mapValues(lambda x: (int(round(x[1]*100)),))).mapValues(lambda x: x[0]+x[1]).values()

    #printing count of output
    print(f'Count of Resultant RDD: {output.count()}')

    #saving output as text file
    output.saveAsTextFile(sys.argv[1])
     

if __name__ == '__main__':
    main()
