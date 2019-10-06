import os
import pandas as pd
import requests

from scrapy.selector import Selector
from pypg import Connector as cnnc

if __name__ == '__main__':

    # Get page response.
    url = 'https://ideas.repec.org/top/top.journals.simple10.html'
    page = requests.get(url)

    # Transform response into text, which can be fed to a CSS Selector
    slct = Selector(text=page.text)
    toplist = slct.css('tr[bgcolor*="#e6e6e6"]').extract()

    # Initialize a datastructure to store the ranking.
    ranking_table = list()

    # Iterate over each position of the ranking.
    for p, pos in enumerate(toplist):

        slct = Selector(text=pos)

        # Extract main information.
        journal = slct.css('a::text').extract_first()
        kuerzel = slct.css('a').css('a::attr(href)').extract_first()[3:-5]

        # Extract td field info.
        td_fields = [val for val in slct.css('td::text').extract() if val[0].isdigit()]
        rank, ifac, adj_cites, n_items, cites = td_fields

        # journal and publisher are separated by comma.
        indi = journal.index(',')
        publisher = journal[indi + 2:]
        journal = journal[:indi]

        ranking_table.append((kuerzel, rank, journal, publisher, ifac, adj_cites, n_items, cites))

        # Check if there is a second journal mentioned.
        if 'also covers' in ''.join(slct.css('::text').extract()):
            journal = slct.css('a::text').extract()[1]
            kuerzel = slct.css('a').css('a::attr(href)').extract()[1][3:-5]

            # journal and publisher are separated by comma.
            indi = journal.index(',')
            publisher = journal[indi + 2:]
            journal = journal[:indi]

            ranking_table.append((kuerzel, rank, journal, publisher, ifac, adj_cites, n_items, cites))

    # Export csv to postgres mounted imports folder.
    cols = ['kuerzel', 'rank', 'journal', 'publisher', 'ifac', 'adj_cites', 'n_items', 'cites']
    df = pd.DataFrame(ranking_table, columns=cols)

    # remove duplicates that shouldn't exist (and aren't of any interest).
    df.drop_duplicates(subset='kuerzel', keep='first', inplace=True)

    mount_import = '/home/jan/Docker/volumes/postgres/imports/journal_info.csv'
    df.to_csv(mount_import, index=False, header=False, sep='|')

    # connect to the postgres container with stored credentials via function.
    conn = cnnc.connect_to_pg_container()
    cursor = conn.cursor()

    # clean current landing zone.
    qry = 'truncate table pk.lz_journal_info'
    cursor.execute(qry)
    conn.commit()

    # insert updates.
    docker_pth = '/var/lib/postgresql/data/imports/journal_info.csv'
    qry = """copy PK.LZ_JOURNAL_INFO from '{}' (delimiter '|');""".format(docker_pth)
    cursor.execute(qry)
    conn.commit()

    # finish.
    cursor.close()
    conn.close()
    os.remove(mount_import)  # avoid duplicate imports due to empty updates.
