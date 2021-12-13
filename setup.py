from distutils.core import setup
import os

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='ss-format',
    version='0.1-SNAPSHOT',
    packages=['format', 'format.helpers'],
    data_files=[('data_files',['format/formatter_config_template.xlsx'])],
    entry_points={
        "console_scripts": ['ss-peek = format.helpers.peek:main',
                            'ss-rename = format.helpers.rename_header:main',
                            'ss-merge = format.helpers.merge_columns:main',
                            'ss-clean = format.helpers.clean_column:main',
                            'ss-split = format.helpers.split_column:main',
                            'ss-swap = format.helpers.swap_columns:main',
                            'ss-allele-swap = format.helpers.allele_swap:main',
                            'ss-help-ss = format.helpers.help:main',
                            'ss-valid-headers = format.helpers.show_known_headers:main',
                            'ss-rename-file = format.helpers.rename_filename:main',
                            'ss-compress = format.helpers.compress_file:main',
                            'ss-delete = format.helpers.delete_columns:main',
                            'ss-concat = format.helpers.cat_files:main',
                            'ss-format = format.sumstats_formatter:main',
                            'vcf2tsv = format.helpers.gwasvcf2tsv:main']
    },
    url='https://github.com/EBISPOT/sum-stats-formatter',
    license='',
    install_requires=requirements
)
