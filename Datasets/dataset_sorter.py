import sys, csv, operator

data = csv.reader(open('dataset_out.csv', 'r'),delimiter=';')

sortedlist = sorted(data, key=operator.itemgetter(0))    # 0 specifies according to first column we want to sort
with open("dataset_OrderByUser.csv", "w", newline='') as f:
    fileWriter = csv.writer(f, delimiter=';')
    for row in sortedlist:
        fileWriter.writerow(row)

data = csv.reader(open('dataset_out.csv', 'r'),delimiter=';')
sortedlist2 = sorted(data, key=operator.itemgetter(1))
with open("dataset_OrderByItem.csv", "w", newline='') as f2:
    fileWriter2 = csv.writer(f2, delimiter=';')
    for row in sortedlist2:
        fileWriter2.writerow(row)
