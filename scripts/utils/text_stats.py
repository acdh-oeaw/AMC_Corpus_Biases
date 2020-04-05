### from textacy library



def flesch_reading_ease(n_syllables, n_words, n_sents, lang=None):
    """
    Readability score usually in the range [0, 100], related (inversely) to
    :func:`flesch_kincaid_grade_level()`. Higher value => easier text.
    Note:
        Constant weights in this formula are language-dependent;
        if ``lang`` is null, the English-language formulation is used.
    References:
        English: https://en.wikipedia.org/wiki/Flesch%E2%80%93Kincaid_readability_tests#Flesch_reading_ease
        German: https://de.wikipedia.org/wiki/Lesbarkeitsindex#Flesch-Reading-Ease
        Spanish: ?
        French: ?
        Italian: https://it.wikipedia.org/wiki/Formula_di_Flesch
        Dutch: ?
        Russian: https://ru.wikipedia.org/wiki/%D0%98%D0%BD%D0%B4%D0%B5%D0%BA%D1%81_%D1%83%D0%B4%D0%BE%D0%B1%D0%BE%D1%87%D0%B8%D1%82%D0%B0%D0%B5%D0%BC%D0%BE%D1%81%D1%82%D0%B8
    """
    if lang is None or lang == 'en':
        return 206.835 - (1.015 * n_words / n_sents) - (84.6 * n_syllables / n_words)
    elif lang == 'de':
        return 180.0 - (n_words / n_sents) - (58.5 * n_syllables / n_words)
    elif lang == 'es':
        return 206.84 - (1.02 * n_words / n_sents) - (60.0 * n_syllables / n_words)
    elif lang == 'fr':
        return 207.0 - (1.015 * n_words / n_sents) - (73.6 * n_syllables / n_words)
    elif lang == 'it':
        return 217.0 - (1.3 * n_words / n_sents) - (60.0 * n_syllables / n_words)
    elif lang == 'nl':
        return 206.84 - (0.93 * n_words / n_sents) - (77.0 * n_syllables / n_words)
    elif lang == 'ru':
        return 206.835 - (1.3 * n_words / n_sents) - (60.1 * n_syllables / n_words)
    else:
        langs = ['en', 'de', 'es', 'fr', 'it', 'nl', 'ru']
        raise ValueError(
            'Flesch Reading Ease is only implemented for these languages: {}. '
            'Passing `lang=None` falls back to "en" (English)'.format(langs))

def wiener_sachtextformel(n_words, n_polysyllable_words, n_monosyllable_words,
                          n_long_words, n_sents,
                          variant=1):
    """
    Readability score for German-language texts, whose value estimates the grade
    level required to understand a text. Higher value => more difficult text.
    References:
        https://de.wikipedia.org/wiki/Lesbarkeitsindex#Wiener_Sachtextformel
    """
    ms = 100 * n_polysyllable_words / n_words
    sl = n_words / n_sents
    iw = 100 * n_long_words / n_words
    es = 100 * n_monosyllable_words / n_words
    if variant == 1:
        return (0.1935 * ms) + (0.1672 * sl) + (0.1297 * iw) - (0.0327 * es) - 0.875
    elif variant == 2:
        return (0.2007 * ms) + (0.1682 * sl) + (0.1373 * iw) - 2.779
    elif variant == 3:
        return (0.2963 * ms) + (0.1905 * sl) - 1.1144
    elif variant == 4:
        return (0.2744 * ms) + (0.2656 * sl) - 1.693
    else:
        raise ValueError('``variant`` value invalid; must be 1, 2, 3, or 4')
        
def lix(n_words, n_long_words, n_sents):
    """
    Readability score commonly used in Sweden, whose value estimates the
    difficulty of reading a foreign text. Higher value => more difficult text.
    References:
        https://en.wikipedia.org/wiki/LIX
    """
    return (n_words / n_sents) + (100 * n_long_words / n_words)

def automated_readability_index(n_chars, n_words, n_sents):
    """
    Readability score whose value estimates the U.S. grade level required to
    understand a text, most similarly to :func:`flesch_kincaid_grade_level()`,
    but using characters instead of syllables like :func:`coleman_liau_index()`.
    Higher value => more difficult text.
    References:
        https://en.wikipedia.org/wiki/Automated_readability_index
    """
    return (4.71 * n_chars / n_words) + (0.5 * n_words / n_sents) - 21.43