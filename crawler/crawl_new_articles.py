import psycopg2
import sys
import itertools

import pandas
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

    # crawl newest papers of each journal.
    for journal in journals:
        url = 'https://ideas.repec.org/s/{}.html'.format(journal)
        page = requests.get(url)
        slct = Selector(text=page.text)  #

        paplist = [url[:-5] for url in slct.xpath('//li/b/a/@href').getall()]
        fqry = '''insert into pk.lz_aref_inbox values ('{}');'''

        for pap in paplist:
            qry = fqry.format(pap)
            cursor.execute(qry)

        conn.commit()

    # close connections.
    cursor.close()
    conn.close()
