from distutils.core import setup

setup(
    name='ss-validate',
    version='0.1-SNAPSHOT',
    packages=['validate'],
    entry_points={
        "console_scripts": ['ss-validate = validate.validator:main']
    },
    url='https://github.com/EBISPOT/sum-stats-formatter',
    license='',
    install_requires=['pandas_schema']
)
