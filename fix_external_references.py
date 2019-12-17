"""
Evaluate externalReferences in BTS documents.

Usage:
    fix_external_references.py [<collection>...] [--stat-file=<fn>] [--fixable-only|--non-fixable-only]
    fix_external_references.py [<collection>...] [--stat-file=<fn>] [--fix [--upload]]
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
    --fix               Apply fixes without uploading the results
    --upload            Upload fixed refs to CouchDB

Arguments:
    <collection>    Any number of couchDB collection names which are to be
                    processed

"""
import re
import json
import uuid
import base64
import types
from datetime import datetime as dt
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

_rex = {
    k: re.compile(r)
    for k, r in {
        'thot': r'^http://thot\.philo\.ulg\.ac\.be/concept/thot-[0-9]*$',
        'topbib': r'http://thot\.philo\.ulg\.ac\.be/concept/topbib-[0-9-]*[a-z]?$',
        'trismegistos': r'^(https?://)?www\.trismegistos\.org/[^/]*/[0-9]*$',
    }.items()
}


def pp(obj):
    """ pretty print

    >>> pp({'c': 4, 'a': 1, 'b': [2, 3]})
    {
      "a": 1,
      "b": [
        2,
        3
      ],
      "c": 4
    }
    """
    print(json.dumps(obj, indent=2, sort_keys=True))


def generate_id() -> str:
    """ generate a new ID based on hostname and date

    >>> len(generate_id())
    27
    """
    return base64.urlsafe_b64encode(
        bytes.fromhex(
            (dt.now().strftime('%Y0%j') + str(uuid.uuid4()).replace('-',''))[:40])
        ).decode().replace('-', 'Q').replace('_', 'W')[:-1]


def cp_ref(ref: dict, *keys) -> dict:
    """ erstellt eine kopie der uebergebenen external reference mit entweder den
    angegebenen feldern oder allen feldern.

    Wenn das ``_id`` feld NICHT unter den zu kopierenden keys ist, ABER im
    original objekt enthalten, wird eine neue ID auf Basis der aktuellen
    uhrzeit und der MAC adresse des ausfuehrenden hosts erzeugt.

    >>> cp_ref({'type': 'text', 'provider': 'trismegistos', 'reference': 'x'})
    {'type': 'text', 'provider': 'trismegistos', 'reference': 'x'}

    >>> cp_ref({'_id': 'xxx'}, '_id')['_id']
    'xxx'

    >>> len(cp_ref({'type': 't', '_id': 'xxx'}, 'type')['_id'])
    27

    """
    res = {}
    if len(keys) < 1:
        keys = ref.keys()
    for key in keys:
        if key in ref:
            res[key] = ref[key]
    if '_id' not in res and '_id' in ref:
        res['_id'] = generate_id()
    return res


def add_revision(doc: dict, t: dt = None) -> dict:
    """ erzeugt eine neue revision und haengt sie an die revision history des
    dokuments an.

    >>> add_revision({}, t=dt(2017,12,12))
    {'revisions': ['1@2017-12-12T00:00:00@74cb6b70ab6b58566bfadc664b00282d']}


    >>> doc={}
    >>> x=add_revision(doc, t=dt(2017,12,12))
    >>> doc
    {'revisions': ['1@2017-12-12T00:00:00@74cb6b70ab6b58566bfadc664b00282d']}

    """
    revs = doc.get('revisions', [])
    t = t or dt.now()
    revs.append(
        f'{len(revs)+1}@{t:%Y-%m-%dT%H:%M:%S}@74cb6b70ab6b58566bfadc664b00282d'
    )
    doc['revisions'] = revs
    return doc


def aaew_type(ID: str):
    """ weist einer ID den type ``hieratic_hieroglyphic`` oder ``demotic`` zu,
    je nachdem ob sie mit einem ``d`` beginnt oder nicht.

    >>> aaew_type('10070')
    'hieratic_hieroglyphic'

    >>> aaew_type('dm2356')
    'demotic'
    """
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
    >>> pp(fix_type_vega('', {'type': 'vega', 'reference': '1'}))
    {
      "provider": "vega",
      "reference": "1"
    }

    """
    ref = cp_ref(ref, 'eClass', 'reference')
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
    {'provider': 'aaew_copy', 'type': 'demotic'}
    """
    ref = cp_ref(ref, '_id', 'eClass', 'reference')
    ref['provider'] = 'aaew_copy'
    ref['type'] = aaew_type(ID)
    return ref


def fix_type_null_reference_trismegistos(
    ID: str,
    ref: dict
) -> dict:
    """
    >>> fix_type_null_reference_trismegistos('', {'reference': 'www.trismegistos.org/text/653740'})
    {'reference': '653740', 'provider': 'trismegistos', 'type': 'text'}

    """
    ref = cp_ref(ref)
    try:
        tm_type, tm_id = ref.get('reference').split('/')[-2:]
        ref['reference'] = tm_id
        ref['provider'] = ref.get('provider') or 'trismegistos'
        ref['type'] = tm_type
    except Exception:
        log.info(f'got unfixable trismegistos ref in doc {ID}: {ref}')
    return ref


def fix_provider_thot_reference_thot(ID: str, ref: dict) -> dict:
    """
    >>> fix_provider_thot_reference_thot('', {'reference': 'http://thot.philo.ulg.ac.be/concept/thot-4845'})
    {'reference': 'thot-4845'}

    """
    ref = cp_ref(ref)
    try:
        thot_id = ref.get('reference').split('/')[-1]
        ref['reference'] = thot_id
    except Exception:
        log.info(f'got unfixable thot ref in doc {ID}: '
                 f'{ref.get("reference")}')
    return ref


def fix_provider_thot_reference_topbib(ID: str, ref: dict) -> dict:
    """
    >>> list(fix_provider_thot_reference_topbib('', {'reference': 'http://thot.philo.ulg.ac.be/concept/topbib-407-070'}))
    [{'reference': 'topbib-407-070', 'provider': 'topbib', 'type': 'thot'}, {'reference': '407-070', 'provider': 'topbib', 'type': 'griffith'}]
    """
    ref = cp_ref(ref)
    try:
        topbib_id = ref.get('reference').split('/')[-1]
        ref['reference'] = topbib_id
        ref['provider'] = 'topbib'
        ref['type'] = 'thot'
        yield ref
        ref = cp_ref(ref)
        topbib_id = '-'.join(topbib_id.split('-')[1:])
        ref['reference'] = topbib_id
        ref['type'] = 'griffith'
        yield ref
    except Exception:
        log.info(f'got unfixable thot topbib ref in doc {ID}: '
                 f'{ref.get("reference")}')
    return ref


def fix_provider_aaew_copy(ID: str, ref: dict):
    ref = cp_ref(ref)
    ref['provider'] = 'aaew'
    yield ref
    ref = cp_ref(ref)
    ref['provider'] = 'dza'
    ref['type'] = aaew_type(ID)
    yield ref


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

    >>> is_fix_applicable(fix_provider_aaew_copy, {'provider': 'aaew_copy'})
    True

    >>> is_fix_applicable(fix_provider_cfeetk, {'provider': 'aaew_wcn'})
    False

    >>> is_fix_applicable(fix_reference_null, {'type': 'foo', 'provider': 'bar'})
    True

    >>> is_fix_applicable(fix_provider_thot_reference_thot, {'provider': 'thot', 'reference': 'http://thot.philo.ulg.ac.be/concept/thot-1000'})
    True

    >>> is_fix_applicable(fix_provider_thot_reference_thot, {'provider': 'thot', 'reference': 'www.foo.bar'})
    False

    """
    conf = parse_fix_name(fix.__name__)
    for key in conf.keys():
        if ref.get(key) != conf.get(key):
            if key == 'reference':
                if conf.get(key):
                    if not _rex[conf.get(key)].match(ref.get(key) or ''):
                        return False
                else:
                    return False
            else:
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
    >>> [f.__name__ for f in get_applicable_fixes('', {'provider': 'trismegistos', 'reference': "www.trismegistos.org/text/128543"})]
    ['fix_type_null_reference_trismegistos']

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

    >>> pp(list(apply_single_fix('', {'provider': 'aaew_copy', 'reference': '1'}, fix_provider_aaew_copy)))
    [
      {
        "provider": "aaew",
        "reference": "1"
      },
      {
        "provider": "dza",
        "reference": "1",
        "type": "hieratic_hieroglyphic"
      }
    ]

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

    >>> list(apply_all_fixes('', '', {'reference': 'www.trismegistos.org/text/45106'}))
    [{'reference': '45106', 'provider': 'trismegistos', 'type': 'text'}]

    """
    fixes = get_applicable_fixes(collection_name, ref)
    res = [ref]
    if len(fixes) > 0:
        while len(fixes) > 0:
            fix = fixes.pop(0)
            log.debug(f'apply fix {fix.__name__} to {res}.')
            res = [
                fixed_ref
                for r in res
                for fixed_ref in apply_single_fix(ID, r, fix)
            ]
        for r in res:
            yield r
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
        if len(hashes) > 20:
            log.info(f'infinite loop in doc {ID} in {collection_name}:')
            log.info(f'started with {refs}')
            log.info(f'endet up with {fixed_refs}')
            break
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


def save_to_stats(collection_name: str, ID: str, refs: list):
    """ save multiple refs to stats file """
    for ref in refs:
        if all(_stats_config.values()):
            should_be_saved = True
        else:
            should_be_saved = do_fixes_apply(
                collection_name,
                ID,
                ref,
            )
            if _stats_config['non-fixable']:
                should_be_saved = not(should_be_saved)
        if should_be_saved:
            save_stats(collection_name, ID, ref)


def is_ref_in_list(ref: dict, refs: list) -> bool:
    """ determines if a ref occurs within the list of refs by comparing md5 sums"""
    h = util.md5(ref)
    for r in refs:
        if util.md5(r) == h:
            return True
    return False


def update_and_upload_document(collection_name: str, doc: dict, refs: list):
    """ replaces the document's ``externalReferences`` array with the passed refs,
    appends a newly created revision to the revision history, and uploads it
    into the specified collection on the database server.
    """
    doc['externalReferences'] = refs
    add_revision(doc)
    a64[collection_name][doc['_id']] = doc


def process_external_references(
        collection_name: str,
        doc: dict,
        apply_fixes: bool = False
) -> bool:
    """ does only collect external references information for now and saves
    it to --stat-file.

    If ``--fix`` flag is set, fixes will be applied to fixable refs and the
    document's ``externalReferences`` will be overwritten with the results.

    :returns: whether fixes have been applied and written to the document
    """
    refs = doc.get('externalReferences', [])
    fixed_refs = apply_fixes_until_cows_come_home(
        collection_name,
        doc['_id'],
        refs,
    )
    if not apply_fixes:
        save_to_stats(
            collection_name,
            doc['_id'],
            refs,
        )
    if util.md5(refs) != util.md5(fixed_refs):
        log.info(refs)
        log.info(fixed_refs)
        if apply_fixes:
            for ref in fixed_refs:
                if not is_ref_in_list(ref, refs):
                    save_stats(collection_name, doc['_id'], ref)
            doc['externalReferences'] = fixed_refs
            return True
    return False


def print_all_scenarios() -> list:
    for fix in get_fixes():
        name = fix.__name__
        print(f'{parse_fix_name(name)} -> {name}')


def count_refs_in_stats() -> int:
    """ how many refs have been stored in the stats dict """
    return sum(
        [
            len(ref_ids)
            for collection_name, providers in _stats.items()
            for provider, ref_types in providers.items()
            for ref_type, doc_ids in ref_types.items()
            for doc_id, ref_ids in doc_ids.items()
        ]
    )


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


def gather_collection_stats(collection_name: str, apply_fixes: bool = False):
    """ go through every BTS document in collection and collect external
    references"""
    doc_count = query_bts_doc_count(collection_name)
    if doc_count > 0:
        for doc in all_docs_in_collection(collection_name):
            process_external_references(
                collection_name,
                doc,
                apply_fixes=apply_fixes
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
        gather_collection_stats(
            collection_name,
            apply_fixes=args.get('--fix', False)
        )
    # save collected stats
    try:
        with open(args['--stat-file'], 'w+') as f:
            print(f'writing {count_refs_in_stats()} gathered references to '
                  f'file {args["--stat-file"]}.')
            json.dump(_stats, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f'cannot write statistics to file {args["--stat-file"]}!')
        print(e)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
