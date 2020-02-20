import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
import re
import pathlib
import format.tab_mani as tabmani
 
 
 
class Home:
    def __init__(self, master):
        self.master = master
        self.frame = tk.Frame(self.master)
        self.master.title("Table Manipulator")
        self.master.wm_iconbitmap('icon.ico')
 
        self.file_browse_lab = ttk.LabelFrame(self.frame, text = "Select a sumstats file")
        self.file_browse_lab.grid(column = 0, row = 1, padx = 2, pady = 2, sticky='W')

        self.file_button()

        self.outfile_prefix_lab = ttk.LabelFrame(self.frame, text = "Prefix to use for outfile")
        self.outfile_prefix_lab.grid(column = 0, row = 3, padx = 2, pady = 2, sticky='W')
        self.outfile_prefix()

        self.field_sep_lab = ttk.LabelFrame(self.frame, text = "Field separator")
        self.field_sep_lab.grid(column = 0, row = 4, padx = 2, pady = 2, sticky='W')
        self.field_sep()

        self.ignore_pattern_lab = ttk.LabelFrame(self.frame, text = "Comment character:")
        self.ignore_pattern_lab.grid(column = 0, row = 7, padx = 2, pady = 2, sticky='W')
        self.ignore_pattern()

        self.split_lab = ttk.LabelFrame(self.frame, text = "Column splits")
        self.split_lab.grid(column = 0, row = 8, padx = 2, pady = 2, sticky='W')
        self.frame.pack()
        self.split_col_tab = []
        self.config = {"outFilePrefix":"formatted_",
                       "md5":"True",
                       "fieldSeparator":"\t",
                       "removeLinesStarting":"#",
                       "splitColumns": [],
                       "keepColumns": [],
                       "findAndReplaceValue": [],
                       "headerRename": []}

    def outfile_prefix(self):
        self.outfile_prefix = tk.Entry(self.outfile_prefix_lab, width=20)
        self.outfile_prefix.grid(column = 1, row = 1, sticky="W")
        self.outfile_prefix.insert(0, "formatted_")

    def field_sep(self):
        self.field_sep_val = tk.StringVar(None, '\s+')
        rdioOne = tk.Radiobutton(self.field_sep_lab, text='Tab',
                             variable=self.field_sep_val, value='\t') 
        rdioTwo = tk.Radiobutton(self.field_sep_lab, text='Comma',
                             variable=self.field_sep_val, value=',') 
        rdioThree = tk.Radiobutton(self.field_sep_lab, text='space',
                             variable=self.field_sep_val, value='\s+')
        rdioOne.grid(column=0, row=4, sticky="W")
        rdioTwo.grid(column=0, row=5, sticky="W")
        rdioThree.grid(column=0, row=6, sticky="W")
        
    def ignore_pattern(self):
        self.ignore_pattern = tk.Entry(self.ignore_pattern_lab, width=1)
        self.ignore_pattern.grid(column = 0, row = 7, sticky="W")

        
    def file_button(self):
        self.file_button = ttk.Button(self.file_browse_lab, text = "Select a file", command = self.fileDialog)
        self.file_button.grid(column = 0, row = 2, sticky="W")

    def fileDialog(self):
        self.filename = filedialog.askopenfilename(initialdir =  "/", title = "Select a file")
        self.label = ttk.Label(self.file_browse_lab, text = "")
        self.label.grid(column = 1, row = 2, sticky="W")
        self.label.configure(text = pathlib.Path(self.filename).name)
        self.preview_button()
        self.table()
        self.split_cols_button()

    def set_table_params(self):
        self.prefix = self.outfile_prefix.get() if self.outfile_prefix.get() else "formatted_"
        regex = re.compile('[@ !#$%^&*()<>?/\|}{~:]')
        if regex.search(self.prefix) != None:
            print("Warning: prefix: '{}' isn't very friendly - make sure it has not special characters or spaces".format(self.prefix))
            return False
        self.comment = self.ignore_pattern.get() if self.ignore_pattern.get() else False
        if self.comment and len(self.comment) > 1:
            print("Warning: comment character can only be one character long: '{}'".format(self.comment))
            return False
        self.sep = self.field_sep_val.get()
        return True
    
    def preview_button(self):
        self.preview_button = ttk.Button(self.frame, text = "Check!", command = self.perform_peek)
        self.preview_button.grid(column = 3, row = 1, sticky="E")

    def perform_peek(self):
        if self.table():
            print("\n>>>>>>>>>>>>>>>>>>>>> File preview <<<<<<<<<<<<<<<<<<<<<")
            print(self.table_top.peek())
        self.get_split_data()
        print(self.config)
        

    def table(self):
        if self.set_table_params():
            self.table_top = tabmani.Table(self.filename, 
                               self.prefix, 
                               self.sep, 
                               self.comment)
            self.table_top.dask_df()
            return True
        return False

    def split_cols_button(self):
        self.split_window_button = ttk.Button(self.frame, text = "Split columns", command = self.split_cols)
        self.split_window_button.grid(column = 4, row = 1, sticky="E")

    def split_cols(self):
        self.header_lab = ttk.LabelFrame(self.frame, text = "File headers")
        self.header_lab.grid(column = 0, row =8, padx = 2, pady = 2, sticky='W')
        self.split_params = ["separator", "leftName", "rightName"] 

        for index, field in enumerate(self.split_params):
            split_col_label = tk.Label(self.header_lab, text=field)
            split_col_label.grid(row=0, column = index + 1)

        for index, field in enumerate(self.table_top.get_header()): #Rows
            split_row_label = tk.Label(self.header_lab, text=field)
            self.split_col_tab.append({"field": field, "separator": None, "leftName": None, "rightName": None})
            for j, item in enumerate(self.split_params): #Columns
                split_row_label.grid(row=index+1, column=0, sticky="E")
                split_col_data = tk.Entry(self.header_lab)
                split_col_data.grid(row=index+1, column=j+1)
                self.split_col_tab[index][item] = split_col_data

    def get_split_data(self):
        for field in self.split_col_tab:
            if all([field['separator'].get(), field['leftName'].get(), field['rightName'].get()]):
                if field['field'] not in [item['field'] for item in self.config['splitColumns']]:
                    self.config['splitColumns'].append(
                                            {"field": field['field'],
                                             "separator": field['separator'].get(),
                                             "leftName": field['leftName'].get(),
                                             "rightName": field['rightName'].get()})




        








        







def main(): 
    root = tk.Tk()
    app = Home(root)
    root.mainloop()

if __name__ == '__main__':
    main()
