from distutils.core import setup
import os

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='ss-format',
    version='0.1-SNAPSHOT',
    packages=['format'],
    data_files=[('data_files',['format/tab_man_template.xlsx'])],
    entry_points={
        "console_scripts": ['ss-peek = format.peek:main',
                            'ss-format = format.automatic_formatting:main',
                            'ss-rename = format.rename_header:main',
                            'ss-merge = format.merge_columns:main',
                            'ss-clean = format.clean_column:main',
                            'ss-split = format.split_column:main',
                            'ss-swap = format.swap_columns:main',
                            'ss-allele-swap = format.allele_swap:main',
                            'ss-help-ss = format.help:main',
                            'ss-valid-headers = format.show_known_headers:main',
                            'ss-rename-file = format.rename_filename:main',
                            'ss-compress = format.compress_file:main',
                            'ss-delete = format.delete_columns:main',
                            'ss-concat = format.cat_files:main',
                            'tabman = format.tabman:main',
                            'vcf2tsv = format.gwasvcf2tsv:main']
    },
    url='https://github.com/EBISPOT/sum-stats-formatter',
    license='',
    install_requires=requirements
)
