import csv
tmpFile = "dataset_out.dat"
filename = "BX-Book-Ratings.csv"

with open(filename, "r") as zf, open(tmpFile, "w", newline='') as f_out:
    reader = csv.reader(zf, delimiter=';')
    writer = csv.writer(f_out, delimiter='\t')
    writer.writerow(next(reader))
    n_items = 0
    n_users = 0
    isbn = dict()
    users_dict = dict()
    for row in reader:   
        if row[1] not in isbn:  
            n_items += 1
            isbn[row[1]] = [n_items]            
        row[1] = isbn[row[1]][0]
        if row[0] not in users_dict:
            n_users += 1
            users_dict[row[0]] = [n_users]
        writer.writerow(row)
    print("n_items:", n_items)
    print("n_users:", n_users)
            
