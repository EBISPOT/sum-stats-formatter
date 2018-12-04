from gwas_properties import *

def get_db_properties(db_properties):
    '''
    Return database connection properties for spotrel (release snapshot database).
    '''
    # Curation app database
    spotpro_properties_file = SPOTPRO_PROPERTIES
    
    # DEV3 database properties - for testing script
    cttv_properties_file = 'cttv_database.properties'

    if db_properties == 'SPOTPRO':
        properties_file = spotpro_properties_file
    elif db_properties == 'CTTV':
        properties_file = cttv_properties_file
    else:
        pass

    try:
        file = open(properties_file, 'r')
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
    else:
        for line in  file:
            if 'spring.datasource.url' in line:
                dsn_tns_property = line.split()
                conn, dsn_tns = dsn_tns_property[1].split('@')
                ip, port, sid = dsn_tns.split(':')
            
            if 'spring.datasource.username' in line:
                username_property = line.split()
                username = username_property[1]
            
            if 'spring.datasource.password' in line:
                password_property = line.split()
                password = password_property[1]
        file.close()

        return ip, port, sid, username, password


if __name__ == '__main__':
    db_config = 'CTTV'
    get_db_properties(db_config)
