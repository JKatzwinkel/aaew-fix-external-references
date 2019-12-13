"""
Evaluate externalReferences in BTS documents.

Usage:
    fix_external_references.py [<collection>...] [--stat-file=<fn>] [--fixable-only|--non-fixable-only]
    fix_external_references.py --list-fixes
    fix_external_references.py --list-collections

    Collected information gets saved into the JSON file at the path specified
    via --stat-file parameter as a nested dictionary structured like this:

        stats[collection_name][ref_provider][ref_type][doc_id] = [ref_id]

Options:
    --list-collections  Print names of all relevant collections found in
                        connected DB (empty collections are omitted)
    --stat-file=<fn>    Path to a JSON file where collected statistics are
                        saved [default: ext_refs.json].
    --list-fixes        Print names and defined scenarios of all fix functions
    --fixable-only      Only references to which fixes apply get saved to
                        `--stat-file`
    --non-fixable-only  Same as `--fixable-only` but the opposite

Arguments:
    <collection>    Any number of couchDB collection names which are to be
                    processed

"""
import json
import types
from collections import defaultdict
from functools import lru_cache, wraps

from tqdm import tqdm
import docopt
from aaew_etl import storage, util, log


a64 = storage.get_couchdb_server()

_stats = defaultdict(
    lambda: defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(list)
        )
    )
)
_stats_config = {
    'fixable': True,
    'non-fixable': True,
}

_view = "function(doc){if(doc.state=='active'&&doc.eClass){emit(doc.id,doc);}}"


def cp_ref(ref: dict, *keys) -> dict:
    res = {}
    if len(keys) < 1:
        keys = ref.keys()
    for key in keys:
        if key in ref:
            res[key] = ref[key]
    return res


def aaew_type(ID: str):
    return 'demotic' if ID.lower().startswith('d')\
        else 'hieratic_hieroglyphic'


def fix_provider_null_type_null(ID: str, ref: dict):
    """
    >>> fix_provider_null_type_null('', {})

    >>> fix_provider_null_type_null('', {'reference': 'foo'})
    {'reference': 'foo'}

    """
    if ref.get('reference') is not None:
        return cp_ref(ref)
    return None


def fix_type_vega(ID: str, ref: dict):
    """
    >>> fix_type_vega('', {'type': 'vega', 'reference': '1'})
    {'reference': '1', 'provider': 'vega'}

    """
    ref = cp_ref(ref, '_id', 'eClass', 'reference')
    ref['provider'] = 'vega'
    return ref


def fix_provider_cfeetk(ID: str, ref: dict) -> dict:
    """
    >>> fix_provider_cfeetk('', {'reference': 'http://sith.huma-num.fr/vocable/287', 'type': 'aaew_wcn'})
    {'reference': '287'}
    """
    ref = cp_ref(ref)
    ref['reference'] = ref.get('reference').split('/')[-1]
    if 'type' in ref:
        ref.pop('type')
    return ref


def fix_type_aaew_wcn(ID: str, ref: dict) -> dict:
    """
    >>> fix_type_aaew_wcn('d1000', {})
    {'provider': 'aaew', 'type': 'demotic'}
    """
    ref = cp_ref(ref, '_id', 'eClass', 'reference')
    ref['provider'] = 'aaew_copy'
    ref['type'] = aaew_type(ID)
    return ref


def fix_provider_trismegistos_type_null(ID: str, ref: dict) -> dict:
    """
    >>> fix_provider_trismegistos_type_null('', {'reference': 'www.tm.org/text/653740'})
    {'reference': '653740', 'type': 'text'}

    """
    ref = cp_ref(ref)
    try:
        tm_type, tm_id = ref.get('reference').split('/')[-2:]
        ref['reference'] = tm_id
        ref['type'] = tm_type
    except:
        log.info(f'got unfixable trismegistos ref in doc {ID}: {ref}')
    return ref


def fix_provider_aaew_copy(ID: str, ref: dict):
    ref = cp_ref(ref)
    ref['provider'] = 'aaew'
    yield ref
    yield {
        'provider': 'dza',
        'type': aaew_type(ID),
        'reference': ref.get('reference')
    }


def fix_reference_null(ID: str, ref: dict):
    return None


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
        if segm in ['provider', 'type', 'reference']:
            stash_conf()
            conf_field = segm
        else:
            conf_values.append(segm)
    stash_conf()
    return conf


def is_fix_applicable(fix: types.FunctionType, ref: dict) -> bool:
    """ looks at a function name and decides whether it should be applied
    to the given reference.

    >>> is_fix_applicable(fix_provider_aaew, {'provider': 'aaew'})
    True

    >>> is_fix_applicable(fix_provider_cfeetk, {'provider': 'aaew_wcn'})
    False

    >>> is_fix_applicable(fix_provider_trismegistos_type_null, {'provider': 'trismegistos'})
    True

    >>> is_fix_applicable(fix_provider_trismegistos_type_null, {'provider': 'trismegistos', 'type': 'text'})
    False

    >>> is_fix_applicable(fix_reference_null, {'type': 'foo', 'provider': 'bar'})
    True

    """
    conf = parse_fix_name(fix.__name__)
    for key in ['type', 'provider', 'reference']:
        if key in conf:
            if ref.get(key) != conf.get(key):
                return False
    return True


def get_fixes() -> list:
    """ return all defined fix functions"""
    return [
        f for f in globals().values()
        if isinstance(f, types.FunctionType) and f.__name__.startswith('fix_')
    ]


def get_applicable_fixes(collection_name: str, ref: dict) -> list:
    """
    >>> [f.__name__ for f in get_applicable_fixes('', {'provider': 'trismegistos'})]
    ['fix_provider_trismegistos_type_null', 'fix_reference_null']

    """
    assert isinstance(ref, dict)
    return [
        f for f in get_fixes()
        if is_fix_applicable(f, ref) and not(
            '_excluded_collections' in f.__dict__ and
            collection_name in f.__dict__['excluded_collections']
        )
    ]


def apply_single_fix(ID: str, ref: dict, fix: types.FunctionType) -> types.GeneratorType:
    """ yields all results of the applied fix

    >>> list(apply_single_fix('', {'reference': 'domain/path/ID'}, fix_provider_cfeetk))
    [{'reference': 'ID'}]

    >>> list(apply_single_fix('', {'type': 'foo'}, fix_reference_null))
    []

    """
    res = fix(ID, ref)
    if isinstance(res, types.GeneratorType):
        for e in res:
            if e:
                yield e
    elif res:
        yield res


def apply_all_fixes(collection_name: str, ID: str, ref: dict) -> types.GeneratorType:
    """
    >>> list(apply_all_fixes('', '', {'provider': 'foo', 'type': 'bar', 'reference': 'id'}))
    [{'provider': 'foo', 'type': 'bar', 'reference': 'id'}]
    """
    fixes = get_applicable_fixes(collection_name, ref)
    if len(fixes) > 0:
        while len(fixes) > 0:
            fix = fixes.pop(0)
            log.info(f'apply fix {fix.__name__} to {ref}.')
            yield from apply_single_fix(ID, ref, fix)
    else:
        yield cp_ref(ref)


def apply_defined_fixes(collection_name: str, ID: str, refs: list) -> list:
    """ applies those ``fix_...`` functions to the external references at hand,
    which have a ``@fix`` decorator and scenario definitions matching the
    reference configuration, i.e. for a reference with provider ``thot``, the
    function ``fix_provider_thot`` will be called, and if it has a type
    ``foo``, then the function ``fix_provider_thot_type_foo`` will be called as
    well, applying its changes to the result of the first fix.

    The returned results contain the changes made to the original external
    references made by all matching fix functions

    """
    fixed_refs = []
    for ref in refs:
        assert isinstance(ref, dict)
        for res in apply_all_fixes(
            collection_name, 
            ID, 
            ref,
        ):
            fixed_refs.append(res)
            assert isinstance(res, dict)
    return fixed_refs


def apply_fixes_until_cows_come_home(
    collection_name: str,
    ID: str,
    refs: list
) -> list:
    """ calls :func:`apply_defined_fixes` on the input until this results in
    no further changes
    """
    if len(refs) < 1:
        return []
    assert isinstance(refs[0], dict)
    fixed_refs = refs
    hashes = []
    while len(hashes) < 2 or hashes[-2] != hashes[-1]:
        hashes.append(util.md5(fixed_refs))
        fixed_refs = apply_defined_fixes(
            collection_name,
            ID,
            fixed_refs
        )
        hashes.append(util.md5(fixed_refs))
    return fixed_refs


def save_stats(collection_name: str, ID: str, ref: dict):
    """ does just that (later this is being written to ``--stat-file``)
    """
    provider = ref.get('provider')
    ref_type = ref.get('type')
    ref_val = ref.get('reference')
    _stats[collection_name][provider][ref_type][ID] += [ref_val]


def do_fixes_apply(collection_name: str, ID: str, ref: dict) -> bool:
    """ apply fixes and see whether the result is different from the original
    ref. If so, this returns True.

    >>> do_fixes_apply('', '', {'provider': 'foo', 'type': 'bar', 'reference': 'id'})
    False

    """
    fixed_hash = util.md5(
        [
            fixed
            for fixed in apply_all_fixes(collection_name, ID, ref)
        ]
    )
    return util.md5([ref]) != fixed_hash


def process_external_references(collection_name: str, doc: dict):
    """ does only collect external references information for now and saves
    it to --stat-file.
    """
    refs = doc.get('externalReferences', [])
    for ref in refs:
        if all(_stats_config.values()):
            should_be_saved = True
        else:
            should_be_saved = do_fixes_apply(
                collection_name,
                doc['_id'],
                ref,
            )
            if _stats_config['non-fixable']:
                should_be_saved = not(should_be_saved)
        if should_be_saved:
            save_stats(collection_name, doc['_id'], ref)
    fixed_refs = apply_fixes_until_cows_come_home(
        collection_name,
        doc['_id'],
        refs,
    )
    if util.md5(refs) != util.md5(fixed_refs):
        log.info(refs)
        log.info(fixed_refs)
        log.info('')


def print_all_scenarios() -> list:
    for fix in get_fixes():
        name = fix.__name__
        print(f'{parse_fix_name(name)} -> {name}')


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
    if args.get('--list-fixes'):
        print_all_scenarios()
        return
    _stats_config['fixable'] = not(args.get('--non-fixable-only', False))
    _stats_config['non-fixable'] = not(args.get('--fixable-only', False))
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
    main(args)
