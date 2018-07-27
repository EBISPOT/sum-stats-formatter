import os
from format.utils import *


def open_close_perform(file, header_function, row_function=None, args=None):
    filename = get_filename(file)
    args = args or {}
    is_header = True
    lines = []
    with open(file) as csv_file:
        csv_reader = get_csv_reader(csv_file)
        for row in csv_reader:
            if is_header:
                is_header = False
                header = header_function(header=row, **args)
                lines.append(header)
            else:
                if row_function is not None:
                    row = row_function(row=row, **args)
                lines.append(row)

    with open('.tmp.tsv', 'w') as result_file:
        writer = csv.writer(result_file, delimiter='\t')
        writer.writerows(lines)

    os.rename('.tmp.tsv', filename + ".tsv")