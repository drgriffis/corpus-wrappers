'''
Creates a single plaintext file containing the NYT portion of English Gigaword.
@author Denis Newman-Griffis (newman-griffis.1@osu.edu)
@uses Python 3.5.1
'''

import glob
import gzip
import codecs
from bs4 import BeautifulSoup
from denis.common.logging import log
from denis.common import preprocessing

def getNextDocument(hook):
    '''Returns the next chunk of text between <DOC>...</DOC> tags.

    Returns None if at the end of the set of docs.
    '''
    lns = []

    ln = hook.readline().decode('utf-8')
    if ln == '': return None
    elif ln[:4] != '<DOC': raise Exception('Expected start of a document!')

    while ln[:5] != '</DOC':
        lns.append(ln)
        ln = hook.readline().decode('utf-8')

    return '\n'.join(lns)

def extractFromGZipFile(gzn, outhook):
    log.writeln('Extracting documents from %s' % gzn)
    log.track(message='  >> Extracted {0} documents...', writeInterval=1)
    hook = gzip.open(gzn)
    while True:
        doc = getNextDocument(hook)
        if doc == None: break

        soup = BeautifulSoup(doc, 'lxml-xml')
        tokens = preprocessing.tokenize(soup.get_text(), splitwords=True)
        outhook.write('%s\n' % ' '.join(tokens))
        log.tick()
    hook.close()
    log.writeln()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog --output=OUTPUT GZIPFILES',
                description='Extracts Gigaword document texts from GZIPFILES and writes them as plaintext to single OUTPUT file')
        parser.add_option('--output', dest='output',
                help='name of file to write artcile texts to (REQUIRED)',
                default=None)
        (options, args) = parser.parse_args()
        if len(args) == 0 or options.output == None:
            parser.print_help()
            exit()
        if len(args) == 1: tarfs = glob.glob(args[0])
        else: gzfs = args
        return gzfs, options.output
    gzfs, outfn = _cli()

    t_main = log.startTimer('Document texts will be written to %s.' % outfn)

    outf = codecs.open(outfn, 'w', 'utf-8')
    for gzf in gzfs:
        extractFromGZipFile(gzf, outf)
    outf.close()

    log.stopTimer(t_main, message='Processing complete in {0:.2f}s.')
