# GWAS Summary Statistics Formatter

A general purpose formatter for tabular GWAS summary statistics files.

The formatter's goal is to enable a user to format a single file, or multiple files _of the same starting format_ to a desired output format.

The formatting process has three steps:
1. Generate a formatting configuration template from the sumstats file
2. Edit the formatting configuration as required
3. Apply the formatting configuration to the file(s)

The interface is a command line tool called `ss-format`. Configurations can be made with JSON or XLSX. There is an option for LSF cluster execution for bulk formatting.

Inputs sumstast files must be flat files containing tables of separated values, where each row has the same number of fields/columns. 

## Installation

### 1. With Docker
```
docker pull ebispot/gwas-sumstats-formatter:latest
docker run -v <path/to/your/sumstats/dir:/files -it ebispot/gwas-sumstats-formatter:latest /bin/bash
ss-format --help
```

### 2. With Conda
```
git clone https://github.com/EBISPOT/sum-stats-formatter.git
cd sum-stats-formatter
conda env create -f conda_env.yml
conda activate ss-format
pip install .
ss-format --help
```

### 3. With venv and pip
```
git clone https://github.com/EBISPOT/sum-stats-formatter.git
cd sum-stats-formatter
python3 -m venv .venv
source .venv/bin/activate
pip install .
ss-format --help
```

## Options

```
$ ss-format --help
usage: ss-format [-h] -f FILEPATH [FILEPATH ...] [-s {space,tab,comma,pipe}]
                 [-p] [-c CONFIG_NAME] [-t {json,xlsx}]
                 [-m {gen,apply,apply-cluster}] [-M CLUSTER_MEM]

optional arguments:
  -h, --help            show this help message and exit
  -f FILEPATH [FILEPATH ...], --filepath FILEPATH [FILEPATH ...]
                        Path to the file to be processed
  -s {space,tab,comma,pipe}, --separator {space,tab,comma,pipe}
                        The separator/delimiter of the input file used to
                        separate the fields
  -p, --preview         Show a preview (top 10 lines) of the input/output
                        file(s)
  -c CONFIG_NAME, --config_name CONFIG_NAME
                        Name configuration file. You can set a path to the
                        configuration file directory in the environment
                        variable SS_FORMAT_CONFIG_DIR
  -t {json,xlsx}, --config_type {json,xlsx}
                        Type of configuration file, default: "json"
  -m {gen,apply,apply-cluster}, --mode {gen,apply,apply-cluster}
                        "gen" to generate the configuration file, "apply" to
                        apply the configuration file
  -M CLUSTER_MEM, --cluster_mem CLUSTER_MEM
                        Option to specify cluster memory in MB when using
                        `--mode apply-cluster`, default: "3600"
```

## Usage
### Format a single sumstats file
1. Generate the configuration file (example input filename: `test_ss.tsv`):
   1. `ss-format -f test_ss.tsv -t json -c test.json -m gen`
2. Edit test.json configuration file (see [Defining the configuration](#defining-the-configuration))
3. Apply the configuration to the file:
   1. `ss-format -f test_ss.tsv -t json -c test.json -m apply`
   2. The output, by default, will be a formatted file called `formatted_test_ss.tsv`
   
### Format multiple sumstats files _in the same way_, i.e. apply the same formatting rules to multiple files
1. Generate the configuration file (example input files: `test_ss1.tsv`, `test_ss2.tsv`, `test_ss3.tsv`):
   1. `ss-format -f test_ss1.tsv -t json -c test.json -m gen`
2. Edit test.json (see wiki for info) configuration file # only one config file will be made
3. Apply the configuration to all the file:
   1. `ss-format -f test_ss*.tsv -t json -c test.json -m apply` # use '*' wildcard for globbing
   2. The outputs, will be formatted files called `formatted_test_ss1.tsv`, `formatted_test_ss2.tsv`, `formatted_test_ss3.tsv`

### Format multiple sumstats files on an LSF cluster (conda or venv install only)
Same as previous except step 3 is like this:
- `ss-format -f test_ss*.tsv -t json -c test.json -m apply-cluster`

You can configure the memory if needed, e.g. you need 5600M of memory:
- `ss-format -f test_ss*.tsv -t json -c test.json -m apply-cluster -M 5600`

### Other commands
Print the top 10 rows of an input to the console:
- `ss-format -f test_ss.tsv`

Print the top 10 rows of an input to the console, specifying the separator as a comma:
- `ss-format -f test_ss.tsv -s comma`

Preview the affect of applying your config by applying to only the first 10 rows:
- `ss-format -f test_ss.tsv -t json -c test.json -m apply --preview`

### Defining the configuration
Step 2 is where you define the configuration that will be applied to the file in step 3. There are currently two possible config file formats, JSON and XLSX. Both have the same features but some may prefer to edit an XLSX rather than a JSON file.

#### Option 1: Edit the JSON config 
When you run `ss-format --mode gen` with `--config_type json` a JSON configuration template will be generated.
Use a text editor to edit the JSON. The docker image has `vim` and `nano` installed for this reason.

There are three sections to edit.
1. File level
2. Column splitting
3. Column configuration

##### File level
Example:
```
{
    "outFilePrefix": "formatted_",      # set prefix for outfile 
    "md5": false,                       # calculate md5 checksum (writes checksum to file if True)
    "convertNegLog10Pvalue": false,     # convert pvalues from -log10 to scientific notation (converts values in the `p_value` field)
    "fieldSeparator": "tab",            # set the field separator. Choose from 'space', 'tab', 'comma', 'pipe'.
    "removeComments": ""                # set a single character as a flag to remove rows beginning with that character e.g. '#'
}
```
##### Column splitting
This is for *splitting a column into two*, based on a separator string. The result is that the column ('field') is split on the first instance of the 'separator' string. 
The portion to the left of the separator is stored in a new column called 'leftName' and the portion to the right is stored in 'rightName'. Note that these new columns are only 
present in the output if they are also added to the 'Column configuration'.

Example:
```
"splitColumns": [                       # An array of split column objects
    {                                   # Each split column object contains the pairs, 'field', 'leftName', 'rightName' and 'separator'
        "field": "variant_id",          # If all values except 'field' are empty strings, no split will occur
        "leftName": "",
        "rightName": "",
        "separator": ""
    },
    {
        "field": "chromosome",
        "leftName": "",
        "rightName": "",
        "separator": ""
    },
    {
        "field": "base_pair_location",
        "leftName": "",
        "rightName": "",
        "separator": ""
    },
    {
        "field": "effect_allele",
        "leftName": "",
        "rightName": "",
        "separator": ""
    },
    {
        "field": "other_allele",
        "leftName": "",
        "rightName": "",
        "separator": ""
    },
    {
        "field": "p_value",             # Here the template is modified to split the 'p_value' column on the string 'e-'
        "leftName": "mantissa",         # The comppnent to the left of 'e-' is stored under a new column called 'mantissa'
        "rightName": "exp",             # The comppnent to the right of 'e-' is stored under a new column called 'exp'
        "separator": "e-"               # The separator can be any string, here it is the string 'e-'
    }
]
```

##### Column configuration
This is the section to edit for the following operations:
1. *Find and replace* - Within a 'field', replace all instances of 'find' with 'replace' (regular expressions are accepted)
2. *Extract* - Within a 'field', any string not matching 'extract' will be replaced with 'NA' (regular expressions are accepted)
3. *Rename column* - Rename 'field' with 'rename'
4. *Drop column* - Drop a 'field' altogether by removing that field's object from the config or setting 'rename' to `null`
5. *Add column* - Add a field, by adding a new object for that field. Values will be set to 'NA' unless the field name matches the 'leftName' or 'rightName' of a column split. 

Example:
```
"columnConfig": [                       # An array of column configuration objects
    {
        "field": "variant_id",
        "rename": "variant_id",         # The tool will automatically try to suggest the standard header (even if it's the same as the input)
        "find": "",
        "replace": "",
        "extract": "^rs[0-9]+$"         # Extract values that match the regex for an rsID, '^rs[0-9]+$'. All other values in the column will be set to 'NA'
    },
    {
        "field": "chromosome",
        "rename": "chromosome",
        "find": "chr|CHR",              # Find instances of 'chr' or 'CHR' and replace with empty strings
        "replace": "",
        "extract": ""
    },
    {
        "field": "base_pair_location",
        "rename": "base_pair_location",
        "find": "",
        "replace": "",
        "extract": ""
    },
    {
        "field": "effect_allele",
        "rename": "effect_allele",
        "find": "",
        "replace": "",
        "extract": ""
    },
    {
        "field": "other_allele",
        "rename": "other_allele",
        "find": "",
        "replace": "",
        "extract": ""
    },
    {
        "field": "p_value",
        "rename": "p_value",
        "find": "",
        "replace": "",
        "extract": ""
    },
    {
        "field": "extra",                 # Add an addition column (populated with 'NA') called 'extra'
        "rename": "",
        "find": "",
        "replace": "",
        "extract": ""
    },
    {
        "field": "ci_lower",              # Remove the 'ci_lower' column (can also be done by reomving this entire object)
        "rename": null,
        "find": "",
        "replace": "",
        "extract": ""
    },
    {
        "field": "mant",                  # Rename the 'mant' field to 'mantissa'
        "rename": "mantissa",
        "find": "",
        "replace": "",
        "extract": ""
    }
]
```

#### Option 2: Edit the XLSX config 
When you run `ss-format --mode gen` with `--config_type xlsx` an XLSX configuration template will be generated. 

Follow the same configuration principles as the JSON, but edit the XLSX workbook instead.
