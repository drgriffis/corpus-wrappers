'''
Parser for Wikipedia XML dumps
@author Denis Newman-Griffis (newman-griffis.1@osu.edu)
@uses Python >= 3.5
'''

from bs4 import BeautifulSoup

_ARTICLE_NAMESPACE_ID = 0

def getNextPage(hook, articles_only=True):
    keep_looking, next_page = True, None
    while keep_looking:
        in_page, page_lines, page_closed = False, [], False
        # start iterating through the rest of the lines in the file
        for line in hook:
            if not in_page:
                soup = BeautifulSoup(line, 'lxml-xml')
                if not soup.page is None: in_page = True
            if in_page:
                page_lines.append(line)
                # if hit end of <page></page>, stop iterating
                if (line.strip().lower() == '</page>'):
                    page_closed = True
                    break

        if page_closed:
            soup = BeautifulSoup('\n'.join(page_lines), 'lxml-xml')
            # if looking for articles only, verify that this page is an article
            if articles_only:
                try:
                    ns = int(soup.ns.text)
                except:
                    ns = -1
                if ns == _ARTICLE_NAMESPACE_ID: keep_looking = False
            # otherwise, return it anyway
            else:
                keep_looking = False

            # if we found a valid page, save it as the return value
            if not keep_looking: next_page = soup

        # if we didn't hit the end of a page, then we hit the end of the file,
        # so no more pages to look through
        else:
            keep_looking = False

    return next_page
