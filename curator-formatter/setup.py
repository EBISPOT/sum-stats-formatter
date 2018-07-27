from distutils.core import setup

setup(
    name='format',
    version='0.1-SNAPSHOT',
    packages=['format'],
    entry_points={
        "console_scripts": ['peek = format.peek:main',
                            'format = format.automatic_formatting:main',
                            'rename = format.rename_header:main',
                            'merge = format.merge_columns:main',
                            'clean = format.clean_column:main',
                            'split = format.split_column:main',
                            'swap = format.swap_columns:main',
                            'allele-swap = format.allele_swap:main',
                            'help-ss = format.help:main',
                            'valid-headers = format.show_known_headers:main',
                            'rename-file = format.rename_filename:main',
                            'compress = format.compress_file:main',
                            'delete = format.delete_columns:main']
    },
    url='https://github.com/EBISPOT/SumStats',
    license='',
    author='Olga Vrousgou',
    author_email='olgavrou@gmail.com',
)
