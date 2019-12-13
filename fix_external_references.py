"""
Evaluate externalReferences in BTS documents.

Usage:
    fix_external_references.py [<collection>... | --list-collections]
    fix_external_references.py [--stat-file=<fn>]

    Collected information gets saved into the JSON file at the path specified
    via --stat-file parameter as a nested dictionary structured like this:

        stats[collection_name][ref_provider][ref_type][doc_id] = [ref_id]

Options:
    --list-collections  Print names of all relevant collections found in
                        connected DB (empty collections are omitted)
    --stat-file=<fn>    Path to a JSON file where collected statistics are
                        saved [default: ext_refs.json].

Arguments:
    <collection>    Any number of couchDB collection names which are to be
                    processed

"""
import json
import types
from collections import defaultdict
from functools import lru_cache, wraps, reduce

from tqdm import tqdm
import docopt
from aaew_etl import storage


a64 = storage.get_couchdb_server()

_stats = defaultdict(
    lambda: defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(list)
        )
    )
)

_view = "function(doc){if(doc.state=='active'&&doc.eClass){emit(doc.id,doc);}}"

_fix_functions = []


def fix(func):
    # TODO: maybe not
    _fix_functions.append(func)

    @wraps(func)
    def wrapper(ID: str, ref: dict):
        yield from func(ID, ref)

    return wrapper


def fix_provider_cfeetk(ID: str, ref: dict) -> dict:
    """

    >>> fix_provider_cfeetk('', {'reference': 'http://sith.huma-num.fr/vocable/287', 'type': 'aaew_wcn'})
    {'reference': '287'}


    """
    ref['reference'] = ref.get('reference').split('/')[-1]
    if 'type' in ref:
        ref.pop('type')
    return ref


def aaew_type(ID: str):
    return 'demotic' if ID.lower().startswith('d')\
        else 'hieratic_hieroglyphic'


def fix_type_aaew_wcn(ID: str, ref: dict) -> dict:
    """

    >>> fix_type_aaew_wcn('d1000', {})
    {'provider': 'aaew', 'type': 'demotic'}

    """
    ref['provider'] = 'aaew'
    ref['type'] = aaew_type(ID)
    return ref


def fix_provider_trismegistos_type_null(ID: str, ref: dict) -> dict:
    """
    >>> fix_provider_trismegistos_type_null('', {'reference': 'www.tm.org/text/653740'})
    {'reference': '653740', 'type': 'text'}

    """
    tm_type, tm_id = ref.get('reference').split('/')[-2:]
    ref['reference'] = tm_id
    ref['type'] = tm_type
    return ref


def fix_provider_aaew_wcn(ID: str, ref: dict):
    yield ref
    yield {
        'provider': 'dza',
        'type': aaew_type(ID),
        'reference': ref.get('reference')
    }


def exclude(*collection_names):
    """ decorator for fix functions that are not supposed to be executed in
    certain collections.

    Functions decorated with this will not be executed by
    :func:`apply_defined_fixes`.
    """
    def decorator(func):
        func._excluded_collections = collection_names

        @wraps(func)
        def wrapper(ID: str, ref: dict):
            yield from func(ID, ref)


def parse_fix_name(name: str) -> dict:
    """
    >>> parse_fix_name('fix_provider_thot')
    {'provider': 'thot'}

    >>> parse_fix_name('fix_provider_aaew_wcn')
    {'provider': 'aaew_wcn'}

    >>> parse_fix_name('fix_provider_trismegistos_type_null')
    {'provider': 'trismegistos', 'type': None}

    """
    def stash_conf():
        if conf_field:
            val = '_'.join(conf_values)
            conf[conf_field] = val if val != 'null' else None
            conf_values.clear()

    segments = name.split('_')
    if len(segments) < 2 or segments[0] != 'fix':
        return False
    conf = {}
    conf_field = None
    conf_values = []
    for segm in segments[1:]:
        if segm in ['provider', 'type']:
            stash_conf()
            conf_field = segm
        else:
            conf_values.append(segm)
    stash_conf()
    return conf


def is_fix_applicable(fix: types.FunctionType, ref: dict) -> bool:
    """ looks at a function name and decides whether it should be applied
    to the given reference.

    >>> is_fix_applicable(fix_provider_aaew_wcn, {'provider': 'aaew_wcn'})
    True

    >>> is_fix_applicable(fix_provider_cfeetk, {'provider': 'aaew_wcn'})
    False

    >>> is_fix_applicable(fix_provider_trismegistos_type_null, {'provider': 'trismegistos'})
    True

    >>> is_fix_applicable(fix_provider_trismegistos_type_null, {'provider': 'trismegistos', 'type': 'text'})
    False

    """
    conf = parse_fix_name(fix.__name__)
    for key in ['type', 'provider']:
        if key in conf:
            if ref.get(key) != conf.get(key):
                return False
    return True


def apply_defined_fixes(collection_name: str, ID: str, ref: dict):
    """ applies those ``fix_...`` functions to the external reference at hand,
    which have a ``@fix`` decorator and scenario definitions matching the
    reference configuration, i.e. for a reference with provider ``thot``, the
    function ``fix_provider_thot`` will be called, and if it has a type
    ``foo``, then the function ``fix_provider_thot_type_foo`` will be called as
    well, applying its changes to the result of the first fix.

    The returned result(s) contain(s) the changes made to the original external
    reference made by all matching fix functions

    Returns zero, one or more external reference objects depending on the fixes
    applied

    """
    fixes = [
        f for f in globals().values()
        if isinstance(f, types.FunctionType) and f.__name__.startswith('fix_')
    ]
    
    results = [ref]
    fixes_applied = True
    while fixes_applied:
        for fix in fixes:
            if '_excluded_collections' in fix.__dict__:
                if collection_name in fix.__dict__['_excluded_collections']:
                    continue
            results = reduce(
                lambda a, b: a + b,
                [list(fix(ID, r)) for r in results]
            )
    for ref in results:
        yield ref


def process_external_references(collection_name: str, doc: dict):
    """ does only collect external references information for now and saves
    it to --stat-file.
    """
    for ref in doc.get('externalReferences', []):
        provider = ref.get('provider')
        ref_type = ref.get('type')
        ref_val = ref.get('reference')
        _stats[collection_name][provider][ref_type][doc['_id']] += [ref_val]


def all_docs_in_collection(collection_name: str):
    """
    Returns iterator producing every non-deleted BTS document in a collection,
    i.e. all ``active`` documents that have an ``eClass`` value. Displays a
    progress bar until fully consumed.

    :returns: generator
    """
    if collection_name in a64:
        collection = a64[collection_name]
        with tqdm(
            total=query_bts_doc_count(collection_name),
            ncols=100,
            desc=collection_name
        ) as pb:
            for doc in storage.couch.apply_temp_view(collection, _view):
                yield doc
                pb.update(1)


@lru_cache(maxsize=512)
def query_bts_doc_count(collection_name: str) -> int:
    """ returns number of BTS documents in specified
    collection

    >>> query_bts_doc_count('aaew_wlist') > 50000
    True

    """
    return storage.couch.view_result_count(
        a64[collection_name],
        _view,
    )


def gather_collection_stats(collection_name: str):
    """ go through every BTS document in collection and collect external
    references"""
    doc_count = query_bts_doc_count(collection_name)
    if doc_count > 0:
        for doc in all_docs_in_collection(collection_name):
            process_external_references(
                collection_name,
                doc,
            )


def list_collections():
    print(f'Non-empty collections in DB at {a64}:\n')
    collection_names = [cn for cn in a64]
    column_width = max(map(len, collection_names))
    for cn in collection_names:
        if len(a64[cn]) > 0:
            print(f'\t{cn:<{column_width}}\t {len(a64[cn])}')


def main(args: dict):
    if len(args['<collection>']) < 1:
        if args['--list-collections']:
            list_collections()
            return
        else:
            print('no collection names given. go through all collections..')
            collection_names = [cn for cn in a64]
    else:
        if args['--list-collections']:
            print(
                'you can either specify <collection> or call '
                '--list-collections but not both'
            )
            return
        else:
            collection_names = [cn for cn in args['<collection>'] if cn in a64]
    # go through specified collections
    for collection_name in collection_names:
        gather_collection_stats(collection_name)
    # save collected stats
    try:
        with open(args['--stat-file'], 'w+') as f:
            print(f'writing gathered reference information to file '
                  f'{args["--stat-file"]}.')
            json.dump(_stats, f, indent=2)
    except Exception as e:
        print(f'cannot write statistics to file {args["--stat-file"]}!')
        print(e)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    print(args)
    main(args)
