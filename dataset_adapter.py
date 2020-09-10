import sys, csv, operator, random
from itertools import islice

def sort_by_user(filename, outfileOrderByUser):
    data = csv.reader(open(filename, 'r'),delimiter='\t')
    sortedlist = sorted(data, key=lambda row: int(row[0]))    # 0 specifies according to first column we want to sort
    with open(outfileOrderByUser, "w", newline='') as f:
        fileWriter = csv.writer(f, delimiter='\t')
        for row in sortedlist:
            fileWriter.writerow(row)

def sort_by_item(filename, outfileOrderByItem):
    data = csv.reader(open(filename, 'r'),delimiter='\t')
    sortedlist = sorted(data, key=lambda row: int(row[1]))
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
   
def castCsvToDat(filename, wordkey):
    tmpFile = wordkey + ".dat"
    with open(filename, "r") as zf, open(tmpFile, "w", newline='') as f_out:
        reader = csv.reader(zf, delimiter=';')
        writer = csv.writer(f_out, delimiter='\t')
        for row in reader:   
            writer.writerow(row)
        
def castDatToCsv(filename, wordkey):
    tmpFile = wordkey + ".csv"
    with open(filename, "r") as zf, open(tmpFile, "w", newline='') as f_out:
        reader = csv.reader(zf, delimiter='\t')
        writer = csv.writer(f_out, delimiter=';')
        for row in reader:   
            writer.writerow(row)     
        
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
        
        #getInfo(outfile,n_rows, n_items, n_users, items_dict, users_dict)
      
def getInfo(filename, n_rows, n_items, n_users, items_dict, users_dict):
    min_items = items_dict[min(items_dict, key=items_dict.get)]
    max_items = items_dict[max(items_dict, key=items_dict.get)]
    filtered_vals = [v for _, v in items_dict.items() if v != 0]
    avg_items = sum(filtered_vals) / len(filtered_vals)
            
    min_users = users_dict[min(users_dict, key=users_dict.get)]
    max_users = users_dict[max(users_dict, key=users_dict.get)]
    filtered_vals = [v for _, v in users_dict.items() if v != 0]
    avg_users = sum(filtered_vals) / len(filtered_vals)
    
    print(filename, " - Tuplas:", n_rows)
    print("Nº usuarios:", n_users)
    print("Nº items:", n_items)
    print("Máx. de veces que un usuario ha valorado un ítem:", max_users)
    print("Mín. de veces que un usuario ha valorado un ítem:", min_users)
    print("Avg de veces que un usuario ha valorado un ítem:", avg_users)
    print("Máx. de veces que un ítem ha sido valorado:", max_items)
    print("Mín. de veces que un ítem ha sido valorado:", min_items)
    print("Avg de veces que un ítem ha sido valorado:", avg_items)

    print("Ratio = AVG de veces que items fueron valorados / (Total items - AVG de veces que items fueron valorados)")
    print("{0} / {1} = {2}".format(avg_items, n_items - avg_items, avg_items/(n_items-avg_items)))
    print("################################################\n\n")

word_key = 'book_original'
filenameDat = word_key + '.dat'
filenameCsv = word_key + '.csv'
outfileOrderByUser = word_key + '_OrderByUser.dat'
outfileOrderByItem = word_key + '_OrderByItem.dat'
outfileShuffle = word_key + '_shuffle.dat'

#castDatToCsv(filename, word_key)

#castToDat()
sort_by_user(filenameDat, outfileOrderByUser)
sort_by_item(filenameDat, outfileOrderByItem)
sort_random(filenameDat, outfileShuffle)

cutDataset(outfileOrderByUser, word_key + '_OrderByUser_100k.dat', 100000)
cutDataset(outfileOrderByItem, word_key + '_OrderByItem_100k.dat', 100000)
cutDataset(outfileShuffle, word_key + '_shuffle_100k.dat', 100000)

cutDataset(outfileOrderByUser, word_key + '_OrderByUser_50k.dat', 50000)
cutDataset(outfileOrderByItem, word_key + '_OrderByItem_50k.dat', 50000)
cutDataset(outfileShuffle, word_key + '_shuffle_50k.dat', 50000)

cutDataset(outfileOrderByUser, word_key + '_OrderByUser_10k.dat', 10000)
cutDataset(outfileOrderByItem, word_key + '_OrderByItem_10k.dat', 10000)
cutDataset(outfileShuffle, word_key + '_shuffle_10k.dat', 10000)
