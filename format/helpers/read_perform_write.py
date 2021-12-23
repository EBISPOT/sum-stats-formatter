import os
from tqdm import tqdm
from format.helpers.utils import *


def open_close_perform(file, header_function, row_function=None, args=None):
    filename = get_filename(file)
    args = args or {}
    is_header = True
    
    with open(file) as csv_file, open('.tmp.tsv', 'w') as result_file:
        csv_reader = get_csv_reader(csv_file)
        writer = csv.writer(result_file, delimiter='\t')
        row_count = get_row_count(file)

        for row in tqdm(csv_reader, total=row_count, unit="rows"):
            if is_header:
                is_header = False
                header = header_function(header=row, **args)
                writer.writerows([header])
                
            else:
                if row_function is not None:
                    row = row_function(row=row, **args)
                writer.writerows([row])

    os.rename('.tmp.tsv', filename + ".tsv")
