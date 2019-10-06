import itertools
import os
import pandas as pd
import requests

from scrapy.selector import Selector
from pypg import Connector as cnnc


def add_paps(collector, jlink, page=0):
    # interpret given page counter and generate next page switch.
    if page == 0:
        pager, pplus = '', 2  # next page from no suffix is 2.
    else:
        pager, pplus = str(page), page + 1

    # construct url.
    url = 'https://ideas.repec.org/s/{}.html'.format(jlink + pager)
    response = requests.get(url)

    # get all references from the current page.
    slct = Selector(text=response.text)
    paplist = [url[:-5] for url in slct.xpath('//li/b/a/@href').getall()]
    collector.extend(paplist)

    i_switches = set(slct.xpath('//li[@class="page-item inactive"]/a/text()').getall())  # inactive switches
    a_switches = slct.xpath('//li[@class="page-item"]/a/text()').getall()  # active switches

    conditions = [
        '»' in i_switches,  # only the final page doesn't have a "next page" button.
        len(a_switches) == 0,  # there's nowhere to go if there are no switches at all.
        page > 100  # hard code force breakpoint for mis-aligned cron job
    ]

    if not any(conditions):
        collector = add_paps(collector=collector, jlink=jlink, page=pplus)

    return collector


if __name__ == '__main__':

    # connect to the postgres container with stored credentials via function.
    conn = cnnc.connect_to_pg_container()
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
        jpaps = add_paps(jpaps, journal)

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
    os.remove(mount_import)
