'''
Methods/script for parsing PubMed Baseline .xml.gz files
and extracting titles/abstracts from each article.
'''
import multiprocessing as mp
import queue
import gzip
import codecs
import os
import glob
import time
import sys
from bs4 import BeautifulSoup
from drgriffis.common import log

class _SIGNALS:
    HALT = -1
    COMPLETED_FILE = 0
    FAILURE = 2

def _t_getArticles(f_q, article_q, n_halts):
    result = f_q.get()
    while result != _SIGNALS.HALT:
        with gzip.open(result, 'r') as stream:
            contents = stream.read()
        soup = BeautifulSoup(contents, 'lxml-xml')
        articles = soup.find_all('Article')
        for article in articles:
            article_q.put(str(article))
        article_q.put(_SIGNALS.COMPLETED_FILE)
        while article_q.qsize() > 5000:
            time.sleep(1)
        result = f_q.get()
    for _ in range(n_halts):
        article_q.put(_SIGNALS.HALT)

def _t_getTitleAndAbstract(article_q, corpus_q):
    result = article_q.get()
    while result != _SIGNALS.HALT:
        if result == _SIGNALS.COMPLETED_FILE:
            corpus_q.put(result)
        else:
            article = BeautifulSoup(result, 'lxml-xml')
            title = article.find('ArticleTitle').text
            abstract = article.find('Abstract')
            if abstract:
                abstract = abstract.find('AbstractText')
                if abstract:
                    abstract = abstract.text.replace('\n', ' ')
            corpus_q.put((title, abstract))
        result = article_q.get()

def _t_writeCorpus(corpus_q, num_files, outf):
    completed_files, success, failure = 0, 0, 0
    log.track(message='  >> Article progress -- Success: {1}  Errors: {2}  GZs Completed: {3}/%d' % num_files, writeInterval=10)
    with codecs.open(outf, 'w', 'utf-8') as stream:
        result = corpus_q.get()
        while result != _SIGNALS.HALT:
            if result == _SIGNALS.COMPLETED_FILE:
                completed_files += 1
            elif result == _SIGNALS.FAILURE:
                failure += 1
            else:
                (title, abstract) = result
                stream.write('%s\n' % title)
                if abstract:
                    stream.write('%s\n' % abstract)
                success += 1
            log.tick(success, failure, completed_files)
            result = corpus_q.get()
    log.flushTracker()

    log.writeln('\nDone processing!')
    log.writeln('Final statistics:')
    log.writeln('  XML files: %d/%d' % (completed_files, num_files))
    log.writeln('  Successful abstracts: %d' % success)
    log.writeln('  Error abstracts: %d' % failure)
    log.writeln('\nOutput written to %s' % outf)

def generateCorpus(dirpath, outf, gz_threads=2, extract_threads=4):
    success, errors = 0, 0
    gzs = glob.glob(os.path.join(dirpath, 'medline*.xml.gz'))

    log.writeln('Extracting records from %d .gz files' % len(gzs))

    f_q, article_q, corpus_q = mp.Queue(), mp.Queue(), mp.Queue()
    gz_processes = []
    for i in range(gz_threads):
        n_halts = extract_threads if i == 0 else 0
        gz_processes.append(mp.Process(target=_t_getArticles, args=(f_q, article_q, n_halts)))
    extract_processes = [
        mp.Process(target=_t_getTitleAndAbstract, args=(article_q, corpus_q))
            for _ in range(extract_threads)
    ]
    write_process = mp.Process(target=_t_writeCorpus, args=(corpus_q, len(gzs), outf))

    for gzf in gzs:
        f_q.put(gzf)

    for t in gz_processes:
        t.start()
    for t in extract_processes:
        t.start()
    write_process.start()

    for _ in gz_processes:
        f_q.put(_SIGNALS.HALT)

    for t in gz_processes:
        t.join()
    for t in extract_processes:
        t.join()

    corpus_q.put(_SIGNALS.HALT)
    write_process.join()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog GZ_DIR FILEPATH',
                description='Extractes titles and abstracts from the PubMed'
                            ' Baseline .xml.gz files in GZ_DIR and writes them'
                            ' to FILEPATH.')
        parser.add_option('--read-threads', dest='read_threads',
                type='int', default=1,
                help='number of threads to use for loading .xml.gz files and extracting'
                     ' articles (default: %default)')
        parser.add_option('--extract-threads', dest='extract_threads',
                type='int', default=3,
                help='number of threads to use for extracting titles and abstracts from'
                     ' individual articles (default: %default)')
        parser.add_option('-l', '--logfile', dest='logfile',
                help='name of file to write log contents to (empty for stdout)',
                default=None)
        (options, args) = parser.parse_args()
        if len(args) != 2:
            parser.print_help()
            exit()
        return args, options

    (gz_dir, outf), options = _cli()
    generateCorpus(
        gz_dir, 
        outf,
        gz_threads=options.read_threads,
        extract_threads=options.extract_threads
    )
