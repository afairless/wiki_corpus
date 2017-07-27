#! /usr/bin/env python3


def decompress_bz2_file(filename, decompressed_filename):
    '''
    Decompresses 'bz2' file and saves it to a new file
    taken from:
    https://stackoverflow.com/questions/16963352/decompress-bz2-files
    '''
    from bz2 import BZ2File as bz2_file
    with open(decompressed_filename, 'wb') as new_file, bz2_file(filename, 'rb') as file:
        for data in iter(lambda : file.read(100 * 1024), b''):
            new_file.write(data)


def print_intermittent_status_message_in_loop(iteration, every_xth_iteration,
                                              total_iterations):
    if iteration % every_xth_iteration == 0:
        print('Processing file {0} of {1}, which is {2:.0f}%'
            .format(iteration + 1, total_iterations,
                    100 * (iteration + 1) / total_iterations))
    return()


def hms_string(sec_elapsed):
    '''
    # downloaded from:
    # http://www.heatonresearch.com/2017/03/03/python-basic-wikipedia-parsing.html
    # https://github.com/jeffheaton/article-code/blob/master/python/wikipedia/wiki-basic-stream.py
    # Simple example of streaming a Wikipedia
    # Copyright 2017 by Jeff Heaton, released under the The GNU Lesser General Public License (LGPL).
    # http://www.heatonresearch.com
    '''
    # Nicely formatted time string
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60
    return "{}:{:>02}:{:>05.2f}".format(h, m, s)


def strip_tag_name(t):
    '''
    # downloaded from:
    # http://www.heatonresearch.com/2017/03/03/python-basic-wikipedia-parsing.html
    # https://github.com/jeffheaton/article-code/blob/master/python/wikipedia/wiki-basic-stream.py
    # Simple example of streaming a Wikipedia
    # Copyright 2017 by Jeff Heaton, released under the The GNU Lesser General Public License (LGPL).
    # http://www.heatonresearch.com
    '''
    t = t.tag
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


def modify_text(a_string):
    '''
    Processes Wikipedia text for analysis:  removes HTML tags, removes  newline
        indicators, converts to lowercase, removes references, removes URLs,
        tokenizes, removes punctuations, removes stop words, removes numbers,
        stems
    Wikipedia text is input as a single string; each string is an article
    Returns a list of processed tokens
    '''

    import html2text
    import nltk
    import string
    import re

    #nltk.download('punkt')
    #nltk.download('all')

    a_string = html2text.html2text(a_string).lower()            # remove HTML tags, convert to lowercase
    a_string = a_string.split('=references=')[0]                # remove references and everything afterwards
    a_string = re.sub(r'https?:\/\/.*?[\s]', '', a_string)      # remove URLs
    a_string = a_string.replace('|', ' ').replace('\n', ' ')    # tokenizer doesn't divide by '|' and '\n'

    tokens = nltk.tokenize.word_tokenize(a_string)
    #tokens = nltk.tokenize.RegexpTokenizer(r'\w+').tokenize(a_string)

    stop_words = nltk.corpus.stopwords.words('english')
    string_punctuation = set(string.punctuation)
    punctuation = [p for p in string_punctuation]
    #miscellaneous = ['url']
    remove_items_list = stop_words + punctuation #+ miscellaneous

    tokens = [w for w in tokens if w not in remove_items_list]
    tokens = [w for w in tokens if '=' not in w]                        # remove remaining tags and the like
    tokens = [w for w in tokens if not                                  # remove tokens that are all digits or punctuation
              all(x.isdigit() or x in string_punctuation for x in w)]
    tokens = [w.strip(string.punctuation) for w in tokens]              # remove stray punctuation attached to words
    tokens = [w for w in tokens if len(w) > 1]                          # remove single characters
    tokens = [w for w in tokens if not any(x.isdigit() for x in w)]     # remove everything with a digit in it

    stemmer = nltk.stem.PorterStemmer()
    stemmed = [stemmer.stem(w) for w in tokens]

    return(stemmed)


def insert_row_sqlite(database_name, table_name, values_list):
    '''
    Inserts row into table of SQLite database
    'database_name' - name of SQLite database
    'table_name' - name of table to insert row into
    'values_list' - list of the row's values to insert
    WARNING:  do not use this function with an unsecured database; it is
        vulnerable to SQL injection attacks
    '''
    import sqlite3

    con = sqlite3.connect(database_name)
    cur = con.cursor()

    placeholders = ', '.join('?' * len(values_list))
    cur.execute('INSERT INTO {t} VALUES ({p})'
                .format(t=table_name, p=placeholders),
                (values_list))

    con.commit()
    con.close()

    return


def count_wiki_articles(wiki_path, print_status_interval, num_documents):
    '''
    Counts and returns the number of Wikipedia articles in a dumped Wikipedia
        XML file
    Counts are divided into template pages, redirect pages, pages with articles,
        and a total number of pages; only the number of article pages is
        returned

    'wiki_path' - file path to the Wikipedia XML file
    'print_status_interval' - the number of articles to process before providing
        a status update message (e.g., print the message every 50 articles)
    'num_documents' - estimated number of pages with articles in the Wikipedia
        XML file; this is used only to provide the status update message, so an
        accurate estimate is not necessary to provide accurate counts

    # adapted from:
    # http://www.heatonresearch.com/2017/03/03/python-basic-wikipedia-parsing.html
    # https://github.com/jeffheaton/article-code/blob/master/python/wikipedia/wiki-basic-stream.py
    # Simple example of streaming a Wikipedia
    # Copyright 2017 by Jeff Heaton, released under the The GNU Lesser General Public License (LGPL).
    # http://www.heatonresearch.com
    '''

    import xml.etree.ElementTree as etree

    # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/

    totalCount = 0
    articleCount = 0
    redirectCount = 0
    templateCount = 0
    title = None

    for event, elem in etree.iterparse(wiki_path, events=('start', 'end')):
        tname = strip_tag_name(elem)

        if event == 'start':
            if tname == 'page':
                title = ''
                wiki_id = -1
                redirect = ''
                inrevision = False
                ns = 0
            elif tname == 'revision':
                # Do not pick up on revision id's
                inrevision = True

        else:
            if tname == 'title':
                pass
            elif tname == 'id' and not inrevision:
                pass
            elif tname == 'redirect':
                redirect = elem.attrib['title']
            elif tname == 'ns':
                ns = int(elem.text)
            elif tname == 'text':
                pass
            elif tname == 'page':
                totalCount += 1

                if ns == 10:
                    templateCount += 1
                elif len(redirect) > 0:
                    redirectCount += 1
                else:
                    articleCount += 1
                    print_intermittent_status_message_in_loop(articleCount,
                                    print_status_interval, num_documents)

            elem.clear()

            #if articleCount > 120:     # limit number of articles for testing
            #    break

    print('Total pages: {:,}'.format(totalCount))
    print('Template pages: {:,}'.format(templateCount))
    print('Article pages: {:,}'.format(articleCount))
    print('Redirect pages: {:,}'.format(redirectCount))

    return(articleCount)


def process_save_wiki_to_sql(wiki_path, database_name, key_list,
                             print_status_interval, num_documents):
    '''
    Processes a dumped Wikipedia XML file and stores the results in a SQLite
        database

    'wiki_path' - file path to the Wikipedia XML file
    'database_name' - name of the database to create
    'key_list' - list of integers to become the primary key for the table of
        articles in the database
    'print_status_interval' - the number of articles to process before providing
        a status update message (e.g., print the message every 50 articles)
    'num_documents' - the number of pages with articles in the Wikipedia XML
        file

    Wikipedia periodically provides its entire website in a 'dump': further
        information here:
            https://meta.wikimedia.org/wiki/Data_dumps
            https://dumps.wikimedia.org/enwiki/
    This function divides the Wikipedia pages into template pages, redirect
        pages, and pages with articles; information on each is stored in its own
        table in the database
    Information on a template page includes its Wikipedia ID number and title
    Information on a redirect page includes its Wikipedia ID number, title, and
        and the title of the page that the user is redirected to
    Information on an article page includes its Wikipedia ID number, title, and
        the text of the page
    The text of an article page is processed for analysis by a call to the
        function 'modify_text' before being stored in the database
    The function returns the name of the articles table, the name of the column
        with the processed article text, and the name of the column with the
        primary key

    # adapted from:
    # http://www.heatonresearch.com/2017/03/03/python-basic-wikipedia-parsing.html
    # https://github.com/jeffheaton/article-code/blob/master/python/wikipedia/wiki-basic-stream.py
    # Simple example of streaming a Wikipedia
    # Copyright 2017 by Jeff Heaton, released under the The GNU Lesser General Public License (LGPL).
    # http://www.heatonresearch.com
    '''
    import xml.etree.ElementTree as etree
    import time
    import sqlite3

    con = sqlite3.connect(database_name)
    cur = con.cursor()

    template_table_name = 'template'
    cur.execute('CREATE TABLE {t} (wiki_id INTEGER, title TEXT)'
                .format(t=template_table_name))

    redirect_table_name = 'redirect'
    cur.execute('CREATE TABLE {t} (wiki_id INTEGER, title TEXT, redirect TEXT)'
                .format(t=redirect_table_name))

    articles_table_name = 'articles'
    articles_text_col_name = 'text'
    key_col_name = 'key'
    cur.execute('CREATE TABLE {t} ({k} INTEGER PRIMARY KEY, wiki_id INTEGER, '
                'title TEXT, {te} TEXT)'
                .format(t=articles_table_name, k=key_col_name,
                        te=articles_text_col_name))

    con.commit()
    con.close()

    # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/

    totalCount = 0
    articleCount = 0
    redirectCount = 0
    templateCount = 0
    title = None
    start_time = time.time()

    for event, elem in etree.iterparse(wiki_path, events=('start', 'end')):
        tname = strip_tag_name(elem)

        if event == 'start':
            if tname == 'page':
                title = ''
                wiki_id = -1
                redirect = ''
                inrevision = False
                ns = 0
            elif tname == 'revision':
                # Do not pick up on revision id's
                inrevision = True

        else:
            if tname == 'title':
                title = elem.text
            elif tname == 'id' and not inrevision:
                wiki_id = int(elem.text)
            elif tname == 'redirect':
                redirect = elem.attrib['title']
            elif tname == 'ns':
                ns = int(elem.text)
            elif tname == 'text':
                page_text = modify_text(elem.text)
            elif tname == 'page':
                totalCount += 1

                if ns == 10:
                    templateCount += 1
                    insert_row_sqlite(database_name, template_table_name,
                                      [wiki_id, title])
                elif len(redirect) > 0:
                    redirectCount += 1
                    insert_row_sqlite(database_name, redirect_table_name,
                                      [wiki_id, title, redirect])
                else:
                    articleCount += 1
                    print_intermittent_status_message_in_loop(articleCount,
                                    print_status_interval, num_documents)
                    page_text = ' '.join(page_text)
                    insert_row_sqlite(database_name, articles_table_name,
                                      [key_list[articleCount-1], wiki_id, title,
                                       page_text])
            elem.clear()

            #if articleCount > 120:     # limit number of articles for testing
            #    break

    elapsed_time = time.time() - start_time

    print('Total pages: {:,}'.format(totalCount))
    print('Template pages: {:,}'.format(templateCount))
    print('Article pages: {:,}'.format(articleCount))
    print('Redirect pages: {:,}'.format(redirectCount))
    print('Elapsed time: {}'.format(hms_string(elapsed_time)))

    return(articles_table_name, articles_text_col_name, key_col_name)


def iter_documents_sqlite(database_name, table_name, col_name, key_col_name):
    '''
    '''
    import sqlite3

    con = sqlite3.connect(database_name)
    cur = con.cursor()

    cur.execute('SELECT COUNT(*) FROM {t}'.format(c=key_col_name, t=table_name))
    response = cur.fetchall()
    num_documents = response[0][0]

    try:
        for i in range(num_documents):
            cur.execute('SELECT ({c1}) FROM {t} WHERE {c2}=?'
                        .format(c1=col_name, t=table_name, c2=key_col_name),
                        (i, ))
            response = cur.fetchall()
            document = response[0][0]
            yield(document.split())

    finally:
        con.close()


class TheCorpusFromSql(object):
    '''
    Iterates through each document (a list of tokens) and creates a corpus and
        dictionary in accordance with the Gensim text analysis package
    '''

    def __init__(self, database_name, table_name, col_name, key_col_name):
        from gensim.corpora import Dictionary
        self.database_name = database_name
        self.table_name = table_name
        self.col_name = col_name
        self.key_col_name = key_col_name
        self.dictionary = Dictionary(iter_documents_sqlite(
            database_name, table_name, col_name, key_col_name))

    def __iter__(self):
        for document_tokens_list in iter_documents_sqlite(
            self.database_name, self.table_name, self.col_name, self.key_col_name):
            yield self.dictionary.doc2bow(document_tokens_list)


def main():
    '''
    Creates and saves corpus and dictionary from Wikipedia XML file dump

    Wikipedia periodically provides its entire website in a 'dump': further
        information here:
            https://meta.wikimedia.org/wiki/Data_dumps
            https://dumps.wikimedia.org/enwiki/


    Wikipedia XML dump download information:

    Main site version:
    2017-07-02 19:52:45 done Recombine articles, templates, media/file descriptions, and primary meta-pages.
        enwiki-20170701-pages-articles.xml.bz2 13.1 GB
    2017-07-02 16:48:31 done Articles, templates, media/file descriptions, and primary meta-pages.
        enwiki-20170701-pages-articles1.xml-p10p30302.bz2 156.8 MB


    Illinois mirror:
    ftp://ftpmirror.your.org/pub/wikimedia/dumps/enwiki/20170701/

    Illinois mirror version:
    File:enwiki-20170701-pages-articles.xml.bz2
        13784309 KB 	7/2/17 	7:52:00 PM
    File:enwiki-20170701-pages-articles1.xml-p10p30302.bz2
        160604 KB 	7/2/17 	3:46:00 PM


    Downloads, July 13, 2017:

    Entire Wikipedia dump file:
    ftp://ftpmirror.your.org/pub/wikimedia/dumps/enwiki/20170701/enwiki-20170701-pages-articles.xml.bz2

    Part of Wikipedia dump file, to use for smaller-scale testing:
    ftp://ftpmirror.your.org/pub/wikimedia/dumps/enwiki/20170701/enwiki-20170701-pages-articles1.xml-p10p30302.bz2

    '''

    import os
    import random
    import gensim

    # prepare, decompress Wikipedia dump file
    filepath = os.getcwd()
    filename = 'enwiki-20170701-pages-articles1.xml-p10p30302.bz2'  # small part of Wikipedia
    #filename = 'enwiki-20170701-pages-articles.xml.bz2'             # all of Wikipedia
    compressed_path = os.path.join(filepath, filename)
    wiki_path = compressed_path.rsplit('.', 1)[0]
    decompress_bz2_file(compressed_path, wiki_path)

    # process, save Wikipedia to SQLite database
    message_interval = 200                              # print status update message every 'x' documents
    est_num_docs = 15151                                # estimated number of documents
    num_documents = count_wiki_articles(wiki_path, message_interval, est_num_docs)
    database_name = 'wiki_token_docs.sqlite'
    random.seed(513598)
    key_list = random.sample(range(num_documents), num_documents)   # randomizes order of Wikipedia articles
    wiki_table_name, wiki_col_name, key_col_name = process_save_wiki_to_sql(
        wiki_path, database_name, key_list, message_interval, num_documents)

    # delete de-compressed Wikipedia dump file
    os.remove(wiki_path)

    # create and save Gensim corpus and dictionary
    wiki_corpus = TheCorpusFromSql(database_name, wiki_table_name,
                                   wiki_col_name, key_col_name)
    wiki_dictionary = wiki_corpus.dictionary
    wiki_corpus.dictionary.save('wiki_dictionary.dict')
    wiki_corpus.dictionary.save_as_text('wiki_dictionary.txt')
    gensim.corpora.MmCorpus.serialize('wiki_corpus.mm', wiki_corpus)

    return()


if __name__ == '__main__':
    main()
