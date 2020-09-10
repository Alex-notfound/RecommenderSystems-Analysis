import sys, csv, operator, random

def getInfo(filename):

    with open(filename, "r") as zf:
        reader = csv.reader(zf, delimiter='\t')
        n_items = 0
        n_users = 0
        n_rows = 0
        items_dict = dict()
        users_dict = dict()
        for row in reader:   
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
            n_rows +=1


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
    print("Máx. veces que un usuario ha valorado:", max_users)
    print("Mín. veces que un usuario ha valorado:", min_users)
    print("Avg veces que un usuario ha valorado:", avg_users)
    print("Máx. veces que un ítem ha sido valorado:", max_items)
    print("Mín. veces que un ítem ha sido valorado:", min_items)
    print("Avg veces que un ítem ha sido valorado:", avg_items)

    print("################################################\n")

wordkey = "book_original"
getInfo(wordkey + ".dat")

getInfo(wordkey + "_OrderByItem_100k.dat")
getInfo(wordkey + "_OrderByItem_50k.dat")
getInfo(wordkey + "_OrderByItem_10k.dat")

getInfo(wordkey + "_OrderByUser_100k.dat")
getInfo(wordkey + "_OrderByUser_50k.dat")
getInfo(wordkey + "_OrderByUser_10k.dat")

getInfo(wordkey + "_shuffle_100k.dat")
getInfo(wordkey + "_shuffle_50k.dat")
getInfo(wordkey + "_shuffle_10k.dat")
