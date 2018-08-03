# Final formatting tools

Formatters for bringing files from a 'common' to a loadable format. 

The files must first be formatted to the 'common' format using the [sum-stats-formatter](../sum-stats-formatter).


# List of tools:

1) `sumstats_formatting.py` brings pre-formatted summary statistics files to a database-loadable format. It replaces headers with database-loadable headers and adds any required headers that are missing. Where a header has been added, the records are populated with 'NA' values.

- `$ python sumstats_formatting.py -f <filename>`


2) `harmonise_data.py` performs the following:
- maps the base-pair location from the originally specified genome to a desired genome build using [PyLiftover](https://pypi.org/project/pyliftover/).
- resolves any missing RSID's based on latest genome build by querying [Ensembl's REST API](https://rest.ensembl.org/).
- unresolvable base-pair locations and RSID's are assigned 'NA' and 'id:NA' respectively.

- `$ python harmonise_data.py -f <filename> -original <from genome build> -mapped <to genome build>`


3) `basic_qc.py` performs the following:
- checks for 'variant_id', 'p_value', 'chromosome' and 'base_pair_location' columns.
- removes rows where any of the above are blank
- check data types

- `$ python basic_qc.py -f <filename>`


4) [custom_formatters/](custom_formatters/) contains custom built formatters for specific file formats.
