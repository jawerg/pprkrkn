import psycopg2
import requests
from scrapy.selector import Selector

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

    # clean current landing zone.
    qry = 'truncate table pk.lz_journal_info'
    cursor.execute(qry)
    conn.commit()

    # Insert each row into the journal ranking table.
    fqry = '''insert into pk.lz_journal_info values ({}) on conflict (kuerzel) do nothing;'''
    for tup in ranking_table:
        qry = fqry.format(""", """.join(["'" + val.replace("'", "''") + "'" for val in tup]))
        cursor.execute(qry)
        conn.commit()

    # finish.
    cursor.close()
    conn.close()
