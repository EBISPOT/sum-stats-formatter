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
        #self.title("Sumstats file formatter")
        #self.minsize(640, 400)
        #self.wm_iconbitmap('icon.ico')
 
        self.file_browse_lab = ttk.LabelFrame(self.frame, text = "Summary statistics file formatter")
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
        self.split_window_button()

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

    def table(self):
        if self.set_table_params():
            self.table_top = tabmani.Table(self.filename, 
                               self.prefix, 
                               self.sep, 
                               self.comment)
            self.table_top.dask_df()
            return True
        return False

    def split_window_button(self):
        self.split_window_button = ttk.Button(self.frame, text = "Split columns -->", command = self.split_window())
        self.split_window_button.grid(column = 4, row = 1, sticky="E")

    def split_window(self):
        print("split")



class SplitCols:
    def __init__(self, table):
        super(Root, self).__init__()
        self.title("Sumstats file formatter - split columns")
        self.minsize(640, 400)
        self.wm_iconbitmap('icon.ico')
        self.table = table

    def col_splits(self):
        self.table_top.get_header()
        







def main(): 
    root = tk.Tk()
    app = Home(root)
    root.mainloop()

if __name__ == '__main__':
    main()
