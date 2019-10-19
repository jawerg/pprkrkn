import itertools
import os
import pandas as pd
import requests

from scrapy.selector import Selector
from pypg import Connector as cnnc

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


def get_paps_to_query(crsr):
    crsr.execute('select * from PK.VIEW_QUEUE_AREF')
    arefs = crsr.fetchall()
    return list(itertools.chain(*arefs))


def run_qry(cnn, crsr, qry):
    crsr.execute(qry)
    cnn.commit()


def push_data(cnn, crsr, loc, dat):
    mount_loc = '/home/jan/Docker/volumes/postgres/imports/{}.csv'
    dat.to_csv(mount_loc.format(loc), sep='\t', index=False, header=False)

    # clear current landing zone (delete-query).
    d_qry = 'truncate table PK.LZ_{}'.format(loc.upper())
    run_qry(cnn, crsr, d_qry)

    # insert updates (insert-query).
    docker_pth = '/var/lib/postgresql/data/imports/{}.csv'.format(loc)
    i_qry = "copy PK.LZ_{} from '{}' (delimiter '\t');".format(loc, docker_pth)
    run_qry(cnn, crsr, i_qry)

    # clean-up
    os.remove(mount_loc.format(loc))


if __name__ == '__main__':

    # connect to the postgres container with stored credentials via function.
    conn = cnnc.connect_to_pg_container()
    cursor = conn.cursor()

    # query articles to be processed.
    a_links = get_paps_to_query(crsr=cursor)

    # prepare general form of the page und meta-information.
    furl = 'https://ideas.repec.org{}.html'
    xpath_fqry = '//meta[@name="{}"]/@content'

    # Store meta-information, references, and citations in data structures
    # for all links included in the current run. In a later step, insert those
    # en-block into the database.
    global_dict, references, citations = dict(), list(), list()
    for link in a_links:
        page = requests.get(furl.format(link))
        slct = Selector(text=page.text)

        # store all relevant information in a local dict:
        local_dict = dict()
        for attr in ATTRIBUTES:
            local_dict[attr] = slct.xpath(xpath_fqry.format(attr)).get()

        # lift information into top dict with db identifier.
        local_dict['aref'] = link
        global_dict[link] = local_dict.copy()

        # store references
        refs = slct.xpath('//div[@id="refs"]//@href').getall()
        refs = [(link, ref[:-5]) for ref in refs if ref[0] == '/' and len(ref) > 19]
        references.extend(refs)

        # store citations
        cites = slct.xpath('//div[@id="cites"]//@href').getall()
        cites = [(link, cit) for cit in cites if cit[0] == '/' and len(cit) > 19]
        citations.extend(cites)

    # generate and store DataFrames
    cols = ['aref'] + ATTRIBUTES
    mdf = pd.DataFrame.from_dict(global_dict, orient='index', columns=cols)
    rdf = pd.DataFrame(references)
    cdf = pd.DataFrame(citations)

    # define locations and according tables.
    mdict = {
        'pap_info': mdf,
        'references': rdf,
        'citations': cdf
    }

    for table, data in mdict.items():
        push_data(cnn=conn, crsr=cursor, loc=table, dat=data)

    # close connections and tidy-up.
    cursor.close()
    conn.close()
