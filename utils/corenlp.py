'''
Wrappers for using Stanford CoreNLP to sentence split and tokenize.

Requires python-stanford-corenlp Python package.
  Github: https://github.com/stanfordnlp/python-stanford-corenlp (Requires some dependencies)
  Via pip: pip install stanford-corenlp
  Via pip from Github: pip install -U https://github.com/stanfordnlp/python-stanford-corenlp/archive/master.zip

Expects environment variable CORENLP_HOME to be defined.
Requires Java SE 8 (minimum).
'''

import multiprocessing as mp
import corenlp
from drgriffis.common import preprocessing
from drgriffis.common import log
from . import punctuation

def _threadedTokenizer(input_q, output_q, port, sentence_split, to_lower, remove_punctuation, extra_ops, halt_signal, complete_op, complete_op_args):
    #log.writeln('[THREAD INIT] sentence_split: %s' % str(sentence_split))
    #log.writeln('[THREAD INIT] to_lower: %s' % str(to_lower))
    #log.writeln('[THREAD INIT] remove_punctuation: %s' % str(remove_punctuation))
    try:
        i = 0
        with corenlp.client.CoreNLPClient(
                    start_server=True,
                    endpoint='http://localhost:%d' % port,
                    annotators=['tokenize','ssplit'],
                    stdout=open('/dev/null', 'w'),
                    stderr=open('/dev/null', 'w')
                ) as client:
            result = input_q.get()
            while result != halt_signal:
                normed_line = preprocessing.digitsToZero(result)
                line_sentences = client.annotate(normed_line)

                # extract strings from the data objects
                line_sentences = [
                    [token.word for token in sentence.token]
                        for sentence in line_sentences.sentence
                ]

                # if we're not splitting sentences, squash them all to one line here
                if not sentence_split:
                    line_tokens = []
                    for sentence in line_sentences:
                        line_tokens.extend(sentence)
                    line_sentences = [line_tokens]

                for sentence in line_sentences:
                    # execute any added operations here
                    if extra_ops:
                        for op in extra_ops:
                            sentence = op(sentence)

                    if remove_punctuation:
                        sentence = punctuation.filterTokens(sentence)
                    if to_lower:
                        sentence =  [t.lower() for t in sentence]

                    output_q.put(' '.join(sentence))

                result = input_q.get()
                i += 1
    finally:
        complete_op(*complete_op_args)

def createTokenizerThreads(n_threads, input_q, output_q, halt_signal, complete_op, complete_op_args,
        start_port=9000, sentence_split=False, to_lower=False, remove_punctuation=False, extra_ops=None):
    '''Creates tokenization threads with multiprocessing module, and returns as list (unstarted).

    Required arguments
      n_threads        :: number of tokenization threads to create (each
                          creates its own instance of CoreNLP server)
      input_q          :: multiprocessing.Queue object for input chunks of
                          text. Each item in the queue will be fed to CoreNLP
                          through ssplit and tokenizer ops.
      output_q         :: multiprocessing.Queue object for output lines
      halt_signal      :: stop processing when this comes through the input_q
      complete_op      :: function to execute on completion of each tokenization
                          thread
      complete_op_args :: list of arguments to pass to complete_op

    Optional arguments
      sentence_split     :: add each sentence output by ssplit module to the
                            output_q individually; by default, all sentences
                            are collapsed and each item in input_q generates
                            one put to output_q
      to_lower           :: lowercase all text
      remove_punctuation :: skip punctuation tokens in output
      extra_ops          :: list of lambda functions to execute on each list of
                            tokens (called BEFORE lowercasing and punctuation
                            is removed)
    '''
    threads = [
        mp.Process(
            target=_threadedTokenizer,
            args=(
                input_q,
                output_q,
                start_port+i,
                sentence_split,
                to_lower,
                remove_punctuation,
                extra_ops,
                halt_signal,
                complete_op,
                complete_op_args
            )
        )
            for i in range(n_threads)
    ]
    return threads
