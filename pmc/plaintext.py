'''
Script to extract single plaintext corpus from .tar.gz PubMed PMC Open Access Subset files
@author Denis Newman-Griffis (newman-griffis.1@osu.edu)
@uses Python 3.5.1
'''

import glob
import codecs
import tarfile
from denis.common import preprocessing
from denis.common.logging import log

def extractArticleTexts(tarf, outf, mode='r:gz'):
    log.writeln('--- Processing %s ---' % tarf)
    f = tarfile.open(tarf, mode=mode)

    t_sub = log.startTimer('Reading member files...', newline=False)
    members = f.getmembers()
    log.stopTimer(t_sub, message='%d members ({0:.2f}s)' % len(members))

    log.track(message='  >> Extracted {0} articles...', writeInterval=1)
    for m in members:
        if m.isfile():
            # write each file on a separate line
            hook = f.extractfile(m)
            # pull all lines that don't start with "===="
            # (not skipping any front matter, including the title line, as this sometimes
            # includes the abstract)
            for line in hook:
                decoded = line.decode('utf-8')
                if decoded.strip()[:4] == '====': continue
                tokens = preprocessing.tokenize(decoded)
                outf.write(' '.join(tokens))
            outf.write('\n')
            hook.close()
            log.tick()
    f.close()
    log.writeln()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog --output=OUTPUT TARFILES',
                description='Extracts PMC article texts from TARFILES and writes them to single OUTPUT file')
        parser.add_option('--output', dest='output',
                help='name of file to write artcile texts to (REQUIRED)',
                default=None)
        (options, args) = parser.parse_args()
        if len(args) == 0 or options.output == None:
            parser.print_help()
            exit()
        if len(args) == 1: tarfs = glob.glob(args[0])
        else: tarfs = args
        return tarfs, options.output
    tarfs, outfn = _cli()

    t_main = log.startTimer('Article texts will be written to %s.' % outfn)

    outf = codecs.open(outfn, 'w', 'utf-8')
    for tarf in tarfs:
        extractArticleTexts(tarf, outf)
    outf.close()

    log.stopTimer(t_main, message='Processing complete in {0:.2f}s.')
