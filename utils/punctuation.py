import codecs
from drgriffis.common import log

PUNCTUATION = set([
    ',',
    '.',
    ':',
    ';',
    '!',
    '?',
    '"',
    "'",
    '-lrb-',
    '-rrb-',
    '+',
    '-',
    '>',
    '<',
    '--',
    '%',
    '-lsb-',
    '-rsb-',
    '/',
    '|',
    '`',
    '``',
    "''",
    '=',
    '*',
    '...'
])

def punctuationList():
    punc = list(PUNCTUATION)
    punc.sort()
    return tuple(punc)

def filterTokens(tokens):
    new_tokens = []
    for t in tokens:
        if not t.lower() in PUNCTUATION:
            new_tokens.append(t)
    return new_tokens

def cleanPreTokenizedCorpus(inf, outf):
    log.track(message='  >> Processed {0:,} lines', writeInterval=10000)
    with codecs.open(inf, 'r', 'utf-8') as in_stream:
        with codecs.open(outf, 'w', 'utf-8') as out_stream:
            for line in in_stream:
                tokens = [s.strip() for s in line.split()]
                tokens = filterTokens(tokens)
                out_stream.write(' '.join(tokens))
                out_stream.write('\n')
                log.tick()
    log.flushTracker()

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog IN OUT')
        (options, args) = parser.parse_args()
        if len(args) != 2:
            parser.print_help()
            exit()
        return args
    inf, outf = _cli()
    cleanPreTokenizedCorpus(inf, outf)
