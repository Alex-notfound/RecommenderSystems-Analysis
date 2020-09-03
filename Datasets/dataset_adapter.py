import sys, csv, operator, random
from itertools import islice

def sort_by_user(filename, outfileOrderByUser):
    data = csv.reader(open(filename, 'r'),delimiter='\t')
    sortedlist = sorted(data, key=operator.itemgetter(0))    # 0 specifies according to first column we want to sort
    with open(outfileOrderByUser, "w", newline='') as f:
        fileWriter = csv.writer(f, delimiter='\t')
        for row in sortedlist:
            fileWriter.writerow(row)

def sort_by_item(filename, outfileOrderByItem):
    data = csv.reader(open(filename, 'r'),delimiter='\t')
    sortedlist = sorted(data, key=operator.itemgetter(1))
    with open(outfileOrderByItem, "w", newline='') as f:
        fileWriter = csv.writer(f, delimiter='\t')
        for row in sortedlist:
            fileWriter.writerow(row)

def sort_random(filename, outfileShuffle):
    fi = open(filename, 'r')
    r = fi.readlines()
    fi.close()

    random.shuffle(r)

    fo = open(outfileShuffle, 'w')
    fo.writelines(r)
    fo.close()
   
def castToDat():
    tmpFile = "books_total.dat"
    filename = "books_total.csv"

    with open(filename, "r") as zf, open(tmpFile, "w", newline='') as f_out:
        reader = csv.reader(zf, delimiter=';')
        writer = csv.writer(f_out, delimiter='\t')
        writer.writerow(next(reader))
        n_items = 0
        n_users = 0
        items_dict = dict()
        users_dict = dict()
        for row in reader:   
            if row[1] not in items_dict:  
                n_items += 1
                items_dict[row[1]] = [n_items]            
            row[1] = items_dict[row[1]][0]
            if row[0] not in users_dict:
                n_users += 1
                users_dict[row[0]] = [n_users]
            writer.writerow(row)
        print("Total - n_items:", n_items)
        print("Total - n_users:", n_users)
        
def cutDataset(filename, outfile, n_rows):
            
    with open(filename, "r") as zf, open(outfile, "w", newline='') as f_out:
        reader = csv.reader(zf, delimiter='\t')
        writer = csv.writer(f_out, delimiter='\t')
        n_items = 0
        n_users = 0
        items_dict = dict()
        users_dict = dict()
        
        for row in islice(reader, n_rows):
            if row[1] not in items_dict:  
                n_items += 1
                items_dict[row[1]] = 1
            else:
                items_dict[row[1]] += 1 
            if row[0] not in users_dict:
                n_users += 1
                users_dict[row[0]] = 1
            else:
                users_dict[row[0]] += 1
            writer.writerow(row)
        
        print(outfile)
        print("Size:", n_rows," - n_items:", n_items)
        min_items = items_dict[min(items_dict, key=items_dict.get)]
        max_items = items_dict[max(items_dict, key=items_dict.get)]
        filtered_vals = [v for _, v in items_dict.items() if v != 0]
        avg_items = sum(filtered_vals) / len(filtered_vals)
        print("Cada ítem fue valorado mínimo {0} veces y máximo {1}. Una media de {2} veces puntuado.".format(min_items,max_items,avg_items))
            
        print("Size:", n_rows," - n_users:", n_users)
        min_users = users_dict[min(users_dict, key=users_dict.get)]
        max_users = users_dict[max(users_dict, key=users_dict.get)]
        filtered_vals = [v for _, v in users_dict.items() if v != 0]
        avg_users = sum(filtered_vals) / len(filtered_vals)
        print("Cada usuario valoró mínimo {0} veces y máximo {1}. Una media de {2} veces ha puntuado.".format(min_users,max_users,avg_users))

        print("Ratio = AVG de veces que items fueron valorados / (Total items - AVG de veces que items fueron valorados)")
        print("{0} / {1} = {2}".format(avg_items, n_items - avg_items, avg_items/(n_items-avg_items)))
        print("################################################")

word_key = 'books'
filename = word_key + '_total.dat'
outfileOrderByUser = word_key + '_OrderByUser.dat'
outfileOrderByItem = word_key + '_OrderByItem.dat'
outfileShuffle = word_key + '_shuffle.dat'

#castToDat()
sort_by_user(filename, outfileOrderByUser)
sort_by_item(filename, outfileOrderByItem)
sort_random(filename, outfileShuffle)

cutDataset(outfileOrderByUser, word_key + '_OrderByUser_100k.dat', 100000)
cutDataset(outfileOrderByItem, word_key + '_OrderByItem_100k.dat', 100000)
cutDataset(outfileShuffle, word_key + '_shuffle_100k.dat', 100000)

cutDataset(outfileOrderByUser, word_key + '_OrderByUser_50k.dat', 50000)
cutDataset(outfileOrderByItem, word_key + '_OrderByItem_50k.dat', 50000)
cutDataset(outfileShuffle, word_key + '_shuffle_50k.dat', 50000)

cutDataset(outfileOrderByUser, word_key + '_OrderByUser_10k.dat', 10000)
cutDataset(outfileOrderByItem, word_key + '_OrderByItem_10k.dat', 10000)
cutDataset(outfileShuffle, word_key + '_shuffle_10k.dat', 10000)