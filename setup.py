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
        "console_scripts": ['ss-format = format.sumstats_formatter:main',
                            'vcf2tsv = format.helpers.gwasvcf2tsv:main']
    },
    url='https://github.com/EBISPOT/sum-stats-formatter',
    license='',
    install_requires=requirements
)
