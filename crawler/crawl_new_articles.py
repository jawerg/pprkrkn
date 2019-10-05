import itertools
import os
import pandas as pd
import psycopg2
import requests

from scrapy.selector import Selector

if __name__ == '__main__':

    # initialize database connection.
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

    # receive journals to be taken as baseline.
    qry = 'select * from PK.VIEW_QUEUE_JOURNALS'
    cursor.execute(qry)

    journals = cursor.fetchall()
    journals = list(itertools.chain(*journals))

    # initialize data structure to store results.
    jpaps = list()  # paps from all journals

    # crawl newest papers of each journal.
    for journal in journals:
        url = 'https://ideas.repec.org/s/{}.html'.format(journal)
        page = requests.get(url)
        slct = Selector(text=page.text)  #

        paplist = [url[:-5] for url in slct.xpath('//li/b/a/@href').getall()]
        jpaps.extend(paplist)

    # store results to mounted folder.
    mount_import = '/home/jan/Docker/volumes/postgres/imports/aref_inbox.csv'
    pd.DataFrame(jpaps).to_csv(mount_import, index=False, header=False)

    # clean current landing zone.
    qry = 'truncate table pk.lz_aref_inbox'
    cursor.execute(qry)
    conn.commit()

    # insert updates.
    docker_pth = '/var/lib/postgresql/data/imports/aref_inbox.csv'
    qry = "copy PK.LZ_AREF_INBOX (aref) from '{}';".format(docker_pth)
    cursor.execute(qry)
    conn.commit()

    # close connections and tidy-up.
    cursor.close()
    conn.close()
    #os.remove(mount_import)
