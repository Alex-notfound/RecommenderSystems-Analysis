import sys, csv, operator, random

def sort_by_user():
    data = csv.reader(open('dataset_out.csv', 'r'),delimiter=';')
    sortedlist = sorted(data, key=operator.itemgetter(0))    # 0 specifies according to first column we want to sort
    with open("dataset_OrderByUser.csv", "w", newline='') as f:
        fileWriter = csv.writer(f, delimiter=';')
        for row in sortedlist:
            fileWriter.writerow(row)

def sort_by_item():
    data = csv.reader(open('dataset_out.csv', 'r'),delimiter=';')
    sortedlist = sorted(data, key=operator.itemgetter(1))
    with open("dataset_OrderByItem.csv", "w", newline='') as f:
        fileWriter = csv.writer(f, delimiter=';')
        for row in sortedlist:
            fileWriter.writerow(row)

def sort_random():
    fi = open('dataset_out.csv', 'r')
    r = fi.readlines()
    fi.close()

    random.shuffle(r)

    fo = open('dataset_shuffle.csv', 'w')
    fo.writelines(r)
    fo.close()

sort_by_user()
sort_by_item()
sort_random()
