import csv
tmpFile = "books_total.csv"
filename = "BX-Book-Ratings.csv"

with open(filename, "r") as zf, open(tmpFile, "w", newline='') as f_out:
    reader = csv.reader(zf, delimiter=';')
    writer = csv.writer(f_out, delimiter=';')
    next(reader)
    i = 0
    isbn = dict()
    for row in reader:   
        if row[1] not in isbn:  
            isbn[row[1]] = [i]
            i += 1
        row[1] = isbn[row[1]][0]
        writer.writerow(row)
            
