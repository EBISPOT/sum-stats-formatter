from distutils.core import setup

setup(
    name='sumstats_harmonisation',
    version='0.1',
    packages=['formatting_tools'],
    entry_points={
        "console_scripts": ['ss-format = formatting_tools.sumstats_formatting:main',
                            'ss-update-locations = formatting_tools.update_locations:main',
                            'ss-chrom-check = formatting_tools.chrom_check:main',
                            'ss-chrom-split = formatting_tools.split_by_chromosome:main',
                            'ss-strand-count = formatting_tools.sum_strand_counts:main',
                            'ss-qc = formatting_tools.basic_qc.py:main']
    },
    url='https://github.com/EBISPOT/sum-stats-formatter/',
    license='',
    author='James Hayhurst',
    author_email='hayhurst.jd@gmail.com',
    description='Package for harmonising summary statistics',
    install_requires=['h5py', 'snakemake', 'pyliftover', 'requests']
)
