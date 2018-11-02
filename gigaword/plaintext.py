'''
Creates a single plaintext file containing English Gigaword.
@author Denis Newman-Griffis (newman-griffis.1@osu.edu)
@uses Python 3.5.1
'''

import glob
import gzip
import codecs
import os
import multiprocessing as mp
from bs4 import BeautifulSoup
from utils import corenlp
import configlogger
from drgriffis.common import log

datadirs = [
    'afp_eng',
    'apw_eng',
    'cna_eng',
    'ltw_eng',
    'nyt_eng',
    'wpb_eng',
    'xin_eng'
]

class _SIGNALS:
    HALT = -1
    FILE_COMPLETE = 0
    DOC_COMPLETE = 1
    DOC_SKIPPED = 2

def listAllFiles(gigaword_dir, skip_dirs, skip_files):
    all_files = []
    for datadir in datadirs:
        if datadir in skip_dirs: continue
        files = os.listdir(os.path.join(gigaword_dir, datadir))
        for fname in files:
            if fname in skip_files: continue
            all_files.append(os.path.join(gigaword_dir, datadir, fname))
    return all_files

def getNextDocument(hook, delay_decode_errors=False):
    '''Returns the next chunk of text between <DOC>...</DOC> tags.

    Returns None if at the end of the set of docs.

    Params:
        delay_decode_errors :: if a UnicodeDecodeError occurs, wait until
                               the end of the document is found before raising
                               the exception
    '''
    lns = []
    decode_error = None

    ln = hook.readline()
    if ln == b'': return None
    elif ln[:4] != b'<DOC': raise Exception('Expected start of a document!')

    while ln[:5] != b'</DOC':
        try:
            ln = ln.decode('utf-8')
            lns.append(ln)
        except UnicodeDecodeError as e:
            if not delay_decode_errors: raise e
            elif decode_error is None: decode_error = e

        ln = hook.readline()

    # if we were delaying decode errors until the end and we found one
    # while extracting the document, raise it now
    if delay_decode_errors and (not decode_error is None):
        raise decode_error

    return '\n'.join(lns)

def extractFromGZipFiles(gzns, input_q, output_q, split_sentences=False, ignore_decode_errors=False):
    for gzn in gzns:
        log.writeln('Extracting from %s...' % gzn)
        with gzip.open(gzn, 'r') as hook:
            while True:
                try:
                    doc = getNextDocument(hook, delay_decode_errors=ignore_decode_errors)
                    if doc == None: break

                    soup = BeautifulSoup(doc, 'lxml-xml')
                    text = soup.get_text()
                    paragraphs = [
                        p.replace('\n', ' ')
                            for p in text.split('\n\n\n')
                    ]
                    if split_sentences:
                        for p in paragraphs:
                            if len(p) > 0:
                                input_q.put(p.strip())
                    else:
                        input_q.put(' '.join(paragraphs))
                    output_q.put(_SIGNALS.DOC_COMPLETE)

                except UnicodeDecodeError as e:
                    if ignore_decode_errors: output_q.put(_SIGNALS.DOC_SKIPPED)
                    else: raise e
        output_q.put(_SIGNALS.FILE_COMPLETE)

def _threadedWriter(outf, output_q, n_threads):
    halts_seen = 0
    lines_written, files_complete, docs_complete, docs_skipped = 0, 0, 0, 0

    log.track(message='  >> Written {1:,} lines (processed {2:,} GZip files -- {3:,} good documents; {4:,} skipped for decode error)', writeInterval=100)
    with codecs.open(outf, 'w', 'utf-8') as stream:
        result = output_q.get()
        while True:
            if result == _SIGNALS.HALT:
                halts_seen += 1
                if halts_seen == n_threads:
                    break
            elif result == _SIGNALS.FILE_COMPLETE:
                files_complete += 1
            elif result == _SIGNALS.DOC_COMPLETE:
                docs_complete += 1
            elif result == _SIGNALS.DOC_SKIPPED:
                docs_skipped += 1
            else:
                stream.write(result)
                stream.write('\n')
                lines_written += 1
            log.tick(lines_written, files_complete, docs_complete, docs_skipped)
            result = output_q.get()
    log.flushTracker(lines_written, files_complete, docs_complete, docs_skipped)

if __name__ == '__main__':
    def _cli():
        import optparse
        parser = optparse.OptionParser(usage='Usage: %prog --output=OUTPUT GIGAWORDDIR',
                description='Extracts Gigaword document texts from GZIPFILES and writes them as plaintext to single OUTPUT file')
        parser.add_option('--output', dest='output',
                help='name of file to write artcile texts to (REQUIRED)',
                default=None)
        parser.add_option('--skip-dirs', dest='skip_dirs',
                help='comma-separated list of directories to skip (e.g., nyt_eng,ltw_eng to skip the NY Times and LA Times portions of the corpus)',
                default='')
        parser.add_option('--skip-files', dest='skip_files',
                help='comma-separated list of filenames to skip (e.g., apw_eng_200104.gz,apw_eng_200306.gz to skip two months of AP data)'
                     '; note these are NOT relative paths, just the filenames',
                default='')
        parser.add_option('--split-sentences', dest='split_sentences',
                action='store_true', default=False,
                help='use Stanford CoreNLP sentence splitter and write one sentence per line; by default, one full document is written per line')
        parser.add_option('--lower', dest='to_lower',
                action='store_true', default=False,
                help='lowercase all output text')
        parser.add_option('--remove-punctuation', dest='remove_punctuation',
                action='store_true', default=False,
                help='remove punctuation tokens in output text')
        parser.add_option('--threads', dest='threads',
                type='int', default=2,
                help='number of threads for tokenization')
        parser.add_option('-l', '--logfile', dest='logfile',
                help='file to log configuration and stdout output to')
        (options, args) = parser.parse_args()
        if len(args) == 0 or options.output == None:
            parser.print_help()
            exit()

        if len(options.skip_dirs) > 0:
            options.skip_dirs = set(options.skip_dirs.split(','))
        else:
            options.skip_dirs = set()

        if len(options.skip_files) > 0:
            options.skip_files = set(options.skip_files.split(','))
        else:
            options.skip_files = set()

        return args, options

    (gigaword_dir,), options = _cli()
    log.start(logfile=options.logfile)
    configlogger.writeConfig(log, [
        ('Gigaword directory', gigaword_dir),
        ('Subdirectories to skip', '--none--' if len(options.skip_dirs) == 0 else '[%s]' % ', '.join(options.skip_dirs)),
        ('Specific files to skip', '--none--' if len(options.skip_files) == 0 else '[%s]' % ', '.join(options.skip_files)),
        ('Output format settings', [
            ('Number of tokenization threads', options.threads),
            ('Splitting sentences', options.split_sentences),
            ('Lowercasing', options.to_lower),
            ('Removing punctuation', options.remove_punctuation),
        ])
    ], title='Gigaword plaintext corpus extraction')
    
    gzfs = listAllFiles(gigaword_dir, options.skip_dirs, options.skip_files)
    log.writeln('Found %d gzip files.' % len(gzfs))

    t_main = log.startTimer('Document texts will be written to %s.' % options.output)

    input_q, output_q = mp.Queue(), mp.Queue()
    tokenize_threads = corenlp.createTokenizerThreads(
        n_threads=options.threads,
        input_q=input_q,
        output_q=output_q,
        halt_signal=_SIGNALS.HALT,
        complete_op=lambda q:q.put(_SIGNALS.HALT),
        complete_op_args=(output_q,),
        start_port=9000,
        sentence_split=options.split_sentences,
        to_lower=options.to_lower,
        remove_punctuation=options.remove_punctuation
    )
    write_thread = mp.Process(
        target=_threadedWriter,
        args=(options.output, output_q, options.threads)
    )

    for t in tokenize_threads:
        t.start()
    write_thread.start()

    extractFromGZipFiles(gzfs, input_q, output_q, split_sentences=options.split_sentences, ignore_decode_errors=True)
    for t in tokenize_threads:
        input_q.put(_SIGNALS.HALT)

    for t in tokenize_threads:
        t.join()
    write_thread.join()

    log.stopTimer(t_main, message='Processing complete in {0:.2f}s.')
    log.stop()
