import itertools
import os
import pandas as pd
import psycopg2
import requests

from scrapy.selector import Selector

ATTRIBUTES = [
    'handle',
    'jel_code',
    'author_shortid',
    'date',
    'citation_title',
    'citation_authors',
    'citation_abstract',
    'citation_keywords',
    'citation_year',
    'citation_volume',
    'citation_issue',
    'citation_firstpage',
    'citation_lastpage'
]

if __name__ == '__main__':

    # Open connection against postgres db.
    host_ip, port_number, db_name, user_name, password = (
        'localhost', '5432', 'postgres', 'postgres', 'postman_in_postgres')
    conn = psycopg2.connect(
        host=host_ip,
        port=port_number,
        database=db_name,
        user=user_name,
        password=password
    )
    cursor = conn.cursor()

    # query articles to be processed.
    qry = 'select * from PK.VIEW_QUEUE_AREF'
    cursor.execute(qry)
    a_links = cursor.fetchall()
    a_links = list(itertools.chain(*a_links))

    furl = 'https://ideas.repec.org{}.html'
    xpath_fqry = '//meta[@name="{}"]/@content'

    global_dict = dict()
    for link in a_links:
        page = requests.get(furl.format(link))
        slct = Selector(text=page.text)

        # initialize page:
        local_dict = dict()
        for attr in ATTRIBUTES:
            local_dict[attr] = slct.xpath(xpath_fqry.format(attr)).get()

        # lift information into top dict with db identifier.
        local_dict['aref'] = link
        global_dict[link] = local_dict.copy()

    # generate and store DataFrame from dictionary.
    cols = ['aref'] + ATTRIBUTES
    df = pd.DataFrame.from_dict(global_dict, orient='index', columns=cols)
    mount_import = '/home/jan/Docker/volumes/postgres/imports/pap_info.csv'
    df.to_csv(mount_import, sep='\t', index=False, header=False)

    # insert updates.
    docker_pth = '/var/lib/postgresql/data/imports/pap_info.csv'
    qry = "copy PK.LZ_pap_info from '{}' (delimiter '\t');".format(docker_pth)
    cursor.execute(qry)
    conn.commit()

    # close connections and tidy-up.
    cursor.close()
    conn.close()
    os.remove(mount_import)
