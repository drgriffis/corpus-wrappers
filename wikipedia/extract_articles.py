'''
Script to extract only article pages from a Wikipedia data dump
'''

import codecs
import parser
from denis.common.logging import log

def extractAllArticles(infile, outfile):
    log.track(message='  >> Extracted {0} articles...', writeInterval=5)
    inhook = codecs.open(infile, 'r', 'utf-8')
    outhook = codecs.open(outfile, 'w', 'utf-8')
    while True:
        next_page = parser.getNextPage(inhook, articles_only=True)
        if next_page is None: break
        outhook.write('%s\n' % str(next_page))
        log.tick()
    outhook.close()
    inhook.close()
    log.flushTracker()

if __name__=='__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog INFILE OUTFILE',
                description='Extracts article-only subset of Wikipedia dump in INFILE and saves to OUTFILE')
        (options, args) = parser.parse_args()
        if len(args) != 2:
            parser.print_help()
            exit()
        (infile, outfile) = args
        return infile, outfile

    infile, outfile = _cli()

    t_main = log.startTimer('Extracting article-only subset of Wikipedia dump %s' % infile)
    extractAllArticles(infile, outfile)
    log.stopTimer(t_main, message='Wrote subset to %s.\nProcessing time: {0:.2f}s')
