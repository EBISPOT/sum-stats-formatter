import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
import sys
import re
import pathlib
import json
import format.tab_mani as tabmani
 

class ScrolledFrame(tk.Frame):

    def __init__(self, parent, vertical=True, horizontal=False):
        super().__init__(parent)

        # canvas for inner frame
        self._canvas = tk.Canvas(self, width=640, height=400)
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
        self.inner = tk.Frame(self._canvas)
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
 
        self.file_browse_lab = ttk.LabelFrame(self.frame, text = "Select a tabular file")
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
                       "removeComments":"#",
                       "splitColumns": [],
                       "columnConfig": []}


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
        self.filename = filedialog.askopenfilename(initialdir =  "/", title = "Specify a file")
        self.label = ttk.Label(self.file_browse_lab, text = "")
        self.label.grid(column = 1, row = 2, sticky="W")
        self.label.configure(text = pathlib.Path(self.filename).name)
        self.preview_button()
        self.tablegen()
        self.split_cols()
        self.column_shuffle()
        self.apply_config_button()

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
        self.get_options_data()
        self.get_split_data()
        self.get_col_shuffle_data()
        print("\n>>>>>>>>>>>>>>>>>>>>> config <<<<<<<<<<<<<<<<<<<<<")
        print(json.dumps(self.config, sort_keys=True, indent=4))
        self.tablegen()
        if self.table:
            self.table.field_names.extend(self.table.get_header())
            # check for splits request
            splits = set_var_from_dict(self.config, 'splitColumns', None)
            if splits:
                if self.table.check_split_name_clashes(splits):
                    self.table.perform_splits(splits)
            column_config = set_var_from_dict(self.config, 'columnConfig', None)
            if column_config:
                find_replace = []
                for field in column_config:
                    if "find" and "replace" in field:
                        find_replace.append(field)
                if find_replace:
                    self.table.perform_find_replacements(find_replace)
                header_rename = {}
                for field in column_config:
                    if "rename" in field:
                        if field["rename"]:
                            header_rename[field["field"]] = field["rename"]
                if header_rename:
                    self.table.perform_header_rename(header_rename)
                keep_cols = []
                for field in column_config:
                    if "keep" in field:
                        if field["keep"]:
                            field_name = field["rename"] if "rename" in field and len(field["rename"]) > 0 else field["field"]
                            keep_cols.append(field_name)

                print("\n>>>>>>>>>>>>>>>>>>>>> Table preview <<<<<<<<<<<<<<<<<<<<<")
                print(self.table.peek(keep_cols))
                # need to reset fields so they stay in view
                header_rename = {}
                for field in column_config:
                    if "rename" in field:
                        if field["rename"]:
                            header_rename[field["rename"]] = field["field"]
                if header_rename:
                    self.table.perform_header_rename(header_rename)

                self.table.perform_keep_cols(self.table.get_header())
            else:
                print("\n>>>>>>>>>>>>>>>>>>>>> Table preview <<<<<<<<<<<<<<<<<<<<<")
                print(self.table.peek())

            self.reset_column_shuffle()
        

    def tablegen(self):
        if self.set_table_params():
            self.table = tabmani.Table(self.filename, 
                               self.prefix, 
                               self.sep, 
                               self.comment)
            self.table.partial_df()
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

    def get_col_shuffle_data(self):
        self.config["columnConfig"] = []        
        for field in self.column_shuffle_tab:
            if field['field'] not in [item['field'] for item in self.config['columnConfig']]:
                self.config['columnConfig'].append(
                                            {"field": field['field'],
                                             "find": field['find'].get(),
                                             "replace": field['replace'].get(),
                                             "keep": field['keep'].var.get(),
                                             "rename": field['rename'].get()})

    def get_options_data(self):
        self.config["outFilePrefix"] = self.outfile_prefix.get() if self.outfile_prefix else "formatted_"
        #self.config["md5"] = self.md5.get() if self.md5 else True
        self.config["fieldSeparator"] = self.field_sep_val.get() if self.field_sep_val else "\s+"
        self.config["removeComments"] = self.ignore_pattern.get() if self.ignore_pattern else None


    def reset_column_shuffle(self):
        for widget in self.frame2.winfo_children():
            widget.destroy()
        self.column_shuffle_tab = []
        self.column_shuffle()


    def column_shuffle(self):
        self.header_lab = ttk.LabelFrame(self.frame2, text = "Columns OUT")
        self.header_lab.grid(column = 0, row =9, padx = 2, pady = 2, sticky='W')
        split_params = ["find", "replace", "keep", "rename"] 

        for index, field in enumerate(split_params):
            col_label = tk.Label(self.header_lab, text=field)
            col_label.grid(row=0, column = index + 1)

        for index, field in enumerate(self.table.get_header()): #Rows
            row_label = tk.Label(self.header_lab, text=field)
            self.column_shuffle_tab.append({"field": field, "find": None, "replace": None, "keep": None, "rename": None})
            # get existing values from config
            field_config = next((i for i in self.config["columnConfig"] if i["field"] == field), None)
            for j, item in enumerate(split_params): #Columns
                row_label.grid(row=index+1, column=0, sticky="E")
                if item == 'keep':
                    v = tk.BooleanVar()
                    col_data = tk.Checkbutton(self.header_lab, onvalue=True, offvalue=False, variable=v)
                    col_data.var = v
                    col_data.select()
                else:
                    col_data = tk.Entry(self.header_lab, width=9)
                col_data.grid(row=index+1, column=j+1)
                if field_config:
                    if item in field_config:
                        if item == 'keep':
                            col_data.deselect() if not field_config[item] else col_data.select()
                        else:
                            col_data.insert(0, field_config[item])
                self.column_shuffle_tab[index][item] = col_data
        self.frame2.pack(fill=tk.X)


    def header_rename(self):
        self.header_lab = ttk.LabelFrame(self.frame, text = "Headers")
        self.header_lab.grid(column = 0, row =10, padx = 2, pady = 2, sticky='W')


    def apply_config_button(self):
        self.apply_config_button = ttk.Button(self.frame, text = "Apply config", command = self.apply_config)
        self.apply_config_button.grid(column = 3, row = 2, sticky="E")

    def apply_config(self):
        self.get_options_data()
        self.get_split_data()
        self.get_col_shuffle_data()

        print("File to format: {}\nConfig: {}\n>>>> formatting...".format(str(self.table.file), str(json.dumps(self.config, sort_keys=True, indent=4))))
        tabmani.apply_config_to_file(self.filename, self.config)
        self.table.set_outfile_name()
        print("Formatted file written to >>>> {}".format(str(self.table.outfile_name)))
        sys.exit()
                
       

def set_var_from_dict(dictionary, var_name, default):
    return dictionary[var_name] if var_name in dictionary else default


def main(): 
    root = tk.Tk()
    root.title("Table Manipulator")
    window = ScrolledFrame(root)
    window.pack(expand=True, fill='both')
    Home(window.inner)
    root.mainloop() 
    


if __name__ == '__main__':
    main()
