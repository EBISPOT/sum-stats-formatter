import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
import re
import pathlib
import format.tab_mani as tabmani
 
 

class ScrolledFrame(tk.Frame):

    def __init__(self, parent, vertical=True, horizontal=False):
        super().__init__(parent)

        # canvas for inner frame
        self._canvas = tk.Canvas(self)
        self._canvas.grid(row=0, column=0, sticky='news') # changed

        # create right scrollbar and connect to canvas Y
        self._vertical_bar = tk.Scrollbar(self, orient='vertical', command=self._canvas.yview)
        if vertical:
            self._vertical_bar.grid(row=0, column=1, sticky='ns')
        self._canvas.configure(yscrollcommand=self._vertical_bar.set)

        # create bottom scrollbar and connect to canvas X
        self._horizontal_bar = tk.Scrollbar(self, orient='horizontal', command=self._canvas.xview)
        if horizontal:
            self._horizontal_bar.grid(row=1, column=0, sticky='we')
        self._canvas.configure(xscrollcommand=self._horizontal_bar.set)

        # inner frame for widgets
        self.inner = tk.Frame(self._canvas, bg='red')
        self._window = self._canvas.create_window((0, 0), window=self.inner, anchor='nw')

        # autoresize inner frame
        self.columnconfigure(0, weight=1) # changed
        self.rowconfigure(0, weight=1) # changed

        # resize when configure changed
        self.inner.bind('<Configure>', self.resize)
        self._canvas.bind('<Configure>', self.frame_width)

    def frame_width(self, event):
        # resize inner frame to canvas size
        canvas_width = event.width
        self._canvas.itemconfig(self._window, width = canvas_width)

    def resize(self, event=None): 
        self._canvas.configure(scrollregion=self._canvas.bbox('all'))


class Home:
    def __init__(self, master):
        self.master = master
        self.frame = tk.Frame(self.master)
        self.frame2 = tk.Frame(self.master)
        #self.master.title("Table Manipulator")
        #self.master.wm_iconbitmap('icon.ico')
 
        self.file_browse_lab = ttk.LabelFrame(self.frame, text = "Select a sumstats file")
        self.file_browse_lab.grid(column = 0, row = 1, padx = 2, pady = 2, sticky='W')

        self.file_button()

        self.outfile_prefix_lab = ttk.LabelFrame(self.frame, text = "Prefix to use for outfile")
        self.outfile_prefix_lab.grid(column = 0, row = 3, padx = 2, pady = 2, sticky='W')
        self.outfile_prefix()

        self.field_sep_lab = ttk.LabelFrame(self.frame, text = "Field delimiter")
        self.field_sep_lab.grid(column = 0, row = 4, padx = 2, pady = 2, sticky='W')
        self.field_sep()

        self.ignore_pattern_lab = ttk.LabelFrame(self.frame, text = "Comment character:")
        self.ignore_pattern_lab.grid(column = 0, row = 7, padx = 2, pady = 2, sticky='W')
        self.ignore_pattern()

        self.split_lab = ttk.LabelFrame(self.frame, text = "Column splits")
        self.split_lab.grid(column = 0, row = 8, padx = 2, pady = 2, sticky='W')
        self.frame.pack(fill=tk.X)
        self.split_col_tab = []
        self.column_shuffle_tab =[]
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
        self.tablegen()
        self.split_cols()
        self.column_shuffle()

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
        self.get_split_data()
        print(self.config)
        self.tablegen()
        if self.table:
            self.table.field_names.extend(self.table.get_header())
            # check for splits request
            splits = set_var_from_dict(self.config, 'splitColumns', None)
            if splits:
                if self.table.check_split_name_clashes(splits):
                    self.table.perform_splits(splits)
            self.column_shuffle()
            print("\n>>>>>>>>>>>>>>>>>>>>> File preview <<<<<<<<<<<<<<<<<<<<<")
            print(self.table.peek())
        

    def tablegen(self):
        if self.set_table_params():
            self.table = tabmani.Table(self.filename, 
                               self.prefix, 
                               self.sep, 
                               self.comment)
            self.table.dask_df()
            return True
        return False

    
    def split_cols(self):
        header_lab = ttk.LabelFrame(self.frame, text = "Columns IN")
        header_lab.grid(column = 0, row =8, padx = 2, pady = 2, sticky='W')
        split_params = ["delimiter", "leftName", "rightName"] 

        for index, field in enumerate(split_params):
            split_col_label = tk.Label(header_lab, text=field)
            split_col_label.grid(row=0, column = index + 1)

        for index, field in enumerate(self.table.get_header()): #Rows
            split_row_label = tk.Label(header_lab, text=field)
            self.split_col_tab.append({"field": field, "delimiter": None, "leftName": None, "rightName": None})
            for j, item in enumerate(split_params): #Columns
                split_row_label.grid(row=index+1, column=0, sticky="E")
                split_col_data = tk.Entry(header_lab, width=12)
                split_col_data.grid(row=index+1, column=j+1)
                self.split_col_tab[index][item] = split_col_data


    def get_split_data(self):
        self.config["splitColumns"] = []
        for field in self.split_col_tab:
            if all([field['delimiter'].get(), field['leftName'].get(), field['rightName'].get()]):
                if field['field'] not in [item['field'] for item in self.config['splitColumns']]:
                    self.config['splitColumns'].append(
                                            {"field": field['field'],
                                             "delimiter": field['delimiter'].get(),
                                             "leftName": field['leftName'].get(),
                                             "rightName": field['rightName'].get()})
    
    def column_shuffle(self):
        for widget in self.frame2.winfo_children():
            widget.destroy()
        self.header_lab = ttk.LabelFrame(self.frame2, text = "Columns OUT")
        self.header_lab.grid(column = 0, row =9, padx = 2, pady = 2, sticky='W')
        split_params = ["find", "replace", "keep", "rename"] 

        for index, field in enumerate(split_params):
            col_label = tk.Label(self.header_lab, text=field)
            col_label.grid(row=0, column = index + 1)

        for index, field in enumerate(self.table.get_header()): #Rows
            row_label = tk.Label(self.header_lab, text=field)
            self.column_shuffle_tab.append({"field": field, "find": None, "replace": None})
            for j, item in enumerate(split_params): #Columns
                row_label.grid(row=index+1, column=0, sticky="E")
                col_data = tk.Entry(self.header_lab, width=9)
                col_data.grid(row=index+1, column=j+1)
                self.column_shuffle_tab[index][item] = col_data
        self.frame2.pack(fill=tk.X)



    def header_rename(self):
        self.header_lab = ttk.LabelFrame(self.frame, text = "Headers")
        self.header_lab.grid(column = 0, row =10, padx = 2, pady = 2, sticky='W')

    def keep_cols(self):
        pass

    def md5(self):
        pass


def set_var_from_dict(dictionary, var_name, default):
    return dictionary[var_name] if var_name in dictionary else default



def main(): 
    root = tk.Tk()
    window = ScrolledFrame(root)
    window.pack(expand=True, fill='both')
    Home(window.inner)
    root.mainloop() 
    


if __name__ == '__main__':
    main()
