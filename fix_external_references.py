"""
Evaluate externalReferences in BTS documents.

Usage:
    fix_external_references.py inspect [--stat-file=<fn>] [--fixable-only | --non-fixable-only] [--corpus <collection> ...]
    fix_external_references.py apply-fixes [upload] [--stat-file=<fn>] [--corpus <collection> ...]
    fix_external_references.py list-fixes
    fix_external_references.py list-collections
    fix_external_references.py generate-id

    Collected information gets saved into the JSON file at the path specified
    via --stat-file parameter as a nested dictionary structured like this:

        stats[collection_name][ref_provider][ref_type][doc_id] = [ref_id]

Commands:
    apply-fixes         Apply fixes (dump fixed documents in `fixed_documents` folder)

    upload              Upload fixed refs to CouchDB

    list-fixes          Print names and defined scenarios of all fix functions

    generate-id         Generate and print a single BTS contained object ID and
                        stop.

    list-collections    Print names of all relevant collections found in
                        connected DB (empty collections are omitted)


Options:
    --stat-file=<fn>    Path to a JSON file where collected statistics are
                        saved [default: ext_refs.json].

    --fixable-only      Only references to which fixes apply get saved to
                        `--stat-file`

    --non-fixable-only  Same as `--fixable-only` but the opposite

    --corpus            specify one or more collections to process


Arguments:
    <collection>        Any number of couchDB collection names which are to be
                        processed

"""
import re
import json
import time
import uuid
import base64
import types
from copy import deepcopy
from datetime import datetime as dt
from collections import defaultdict
from functools import lru_cache, wraps

from tqdm import tqdm
import docopt
from aaew_etl import storage, util, log, filing


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
        'thot': r'^http://thot\.philo\.ulg\.ac\.be/concept/thot-[0-9]+$',
        'topbib': r'^http://thot\.philo\.ulg\.ac\.be/concept/topbib-[0-9-]+[a-z]?$',
        'trismegistos': r'^(https?://)?www\.trismegistos\.org/[^/]+/[0-9]+$',
        'griffith': r'^https?://topbib\.griffith\.ox\.ac\.uk//?dtb.html\?topbib=[0-9-]+[a-z]?\s*$',
        'cfeetk': r'^https?://sith\.huma-num\.fr/vocable/[0-9]+$',
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
    {'revisions': ['0@2017-12-12T00:00:00@74cb6b70ab6b58566bfadc664b00282d']}


    >>> doc={}
    >>> x=add_revision(doc, t=dt(2017,12,12))
    >>> doc
    {'revisions': ['0@2017-12-12T00:00:00@74cb6b70ab6b58566bfadc664b00282d']}

    """
    revs = doc.get('revisions', [])
    t = t or dt.now()
    revs.append(
        f'{len(revs)}@{t:%Y-%m-%dT%H:%M:%S}@74cb6b70ab6b58566bfadc664b00282d'
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


def empty_ref():
    """ returns a new external reference with an ``eClass`` field and a
    generated ``_id`` value.
    """
    return {
        "eClass": "http://btsmodel/1.0#//BTSExternalReference",
        "_id": generate_id(),
    }


def generate_topbib_thot_and_griffith(ref: dict):
    """
    >>> list(generate_topbib_thot_and_griffith({'reference': '407-070'}))
    [{'reference': 'topbib-407-070', 'provider': 'topbib', 'type': 'thot'}, {'reference': '407-070', 'provider': 'topbib', 'type': 'griffith'}]
    """
    topbib_id = ref.get('reference')
    if topbib_id is None:
        raise ValueError
    else:
        ref = cp_ref(ref)
        yield {
            **cp_ref(ref),
            'reference': f'topbib-{topbib_id}',
            'provider': 'topbib',
            'type': 'thot'
        }
        if '_id' in ref:
            ref['_id'] = generate_id()
        yield {
            **cp_ref(ref),
            'reference': topbib_id,
            'provider': 'topbib',
            'type': 'griffith'
        }


def fix_provider_null_type_null(ID: str, ref: dict):
    """
    >>> fix_provider_null_type_null('', {'reference': 'foo'})
    {'reference': 'foo'}

    >>> fix_provider_null_type_null('', {})
    """
    if ref.get('reference') is not None:
        return cp_ref(ref)
    return None


def fix_type_aaew_1(ID: str, ref: dict):
    """
    >>> pp(fix_type_aaew_1('', {'type': 'aaew_1', 'reference': '2/1312'}))
    {
      "provider": "aaew",
      "reference": "2/1312"
    }

    >>> pp(fix_type_aaew_1('', {'provider': 'aaew', 'type': 'aaew_1', 'reference': '11'}))
    {
      "provider": "aaew",
      "reference": "11"
    }
    """
    ref = cp_ref(ref)
    ref['provider'] = 'aaew'
    ref.pop('type')
    return ref


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


def fix_provider_cfeetk_reference_cfeetk(ID: str, ref: dict) -> dict:
    """
    >>> fix_provider_cfeetk_reference_cfeetk('', {'reference': 'http://sith.huma-num.fr/vocable/287', 'type': 'aaew_wcn'})
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
        topbib_id = '-'.join(topbib_id.split('-')[1:])
        ref['reference'] = topbib_id
        yield from generate_topbib_thot_and_griffith(ref)
    except Exception:
        log.info(f'got unfixable thot topbib ref in doc {ID}: '
                 f'{ref.get("reference")}')
    return ref


def fix_provider_topographical_bibliography_reference_griffith(ID: str, ref: dict) -> dict:
    """ fix ``topographical bibliography`` provider containing griffith links

    >>> list(fix_provider_topographical_bibliography_reference_griffith('', {'reference': 'http://topbib.griffith.ox.ac.uk//dtb.html?topbib=704-020-010-260'}))
    [{'reference': 'topbib-704-020-010-260', 'provider': 'topbib', 'type': 'thot'}, {'reference': '704-020-010-260', 'provider': 'topbib', 'type': 'griffith'}]
    """
    ref = cp_ref(ref)
    try:
        topbib_id = ref.get('reference').strip().split('=')[-1]
        ref['reference'] = topbib_id
        yield from generate_topbib_thot_and_griffith(ref)
    except Exception:
        log.info(f'got unfixable griffith link in doc {ID}: '
                 f'{ref.get("reference")}')
    return ref


def fix_provider_topographical_bibliography_reference_topbib(ID: str, ref: dict) -> dict:
    """ applies :func:`fix_provider_thot_reference_topbib`.

    >>> list(fix_provider_topographical_bibliography_reference_topbib('', {'reference': 'http://thot.philo.ulg.ac.be/concept/topbib-407-070'}))
    [{'reference': 'topbib-407-070', 'provider': 'topbib', 'type': 'thot'}, {'reference': '407-070', 'provider': 'topbib', 'type': 'griffith'}]
    """
    return fix_provider_thot_reference_topbib(ID, ref)


def fix_provider_topographical_bibliography_reference_thot(ID: str, ref: dict) -> dict:
    """ applies :func:`fix_provider_thot_reference_thot`

    >>> fix_provider_topographical_bibliography_reference_thot('', {'reference': 'http://thot.philo.ulg.ac.be/concept/thot-4845'})
    {'reference': 'thot-4845'}
    """
    return fix_provider_thot_reference_thot(ID, ref)


def fix_provider_aaew_copy(ID: str, ref: dict):
    """ fixes the dummy provider ``aaew_copy`` which is used as an intermediate
    during DZA link creation

    >>> list(fix_provider_aaew_copy('10070', {'provider': 'aaew_copy', 'type': 'hieratic_hieroglyphic'}))
    [{'provider': 'aaew', 'type': 'hieratic_hieroglyphic'}, {'provider': 'dza', 'type': 'hieratic_hieroglyphic'}]

    >>> list(fix_provider_aaew_copy('d2765', {'provider': 'aaew_copy', 'type': 'demotic'}))
    [{'provider': 'aaew', 'type': 'demotic'}]
    """
    ref = cp_ref(ref)
    ref['provider'] = 'aaew'
    yield ref
    ref = cp_ref(ref)
    if '_id' in ref:
        ref['_id'] = generate_id()
    ref['provider'] = 'dza'
    ref['type'] = aaew_type(ID)
    if ref['type'] == 'hieratic_hieroglyphic':
        yield ref


def fix_reference_null(ID: str, ref: dict):
    """ delete references with no reference """
    return None


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


def normalize_identifier(s: str) -> str:
    """ omits spaces and underscores

    >>> normalize_identifier('topographical bibliography') == normalize_identifier('topographical_bibliography')
    True

    >>> normalize_identifier(None)

    """
    if s:
        return s.replace(' ', '').replace('_', '')


def is_fix_applicable(fix: types.FunctionType, ref: dict) -> bool:
    """ looks at a function name and decides whether it should be applied
    to the given reference.

    >>> is_fix_applicable(fix_provider_aaew_copy, {'provider': 'aaew_copy'})
    True

    >>> is_fix_applicable(fix_provider_cfeetk_reference_cfeetk, {'provider': 'aaew_wcn'})
    False

    >>> is_fix_applicable(fix_reference_null, {'type': 'foo', 'provider': 'bar'})
    True

    >>> is_fix_applicable(fix_provider_thot_reference_thot, {'provider': 'thot', 'reference': 'http://thot.philo.ulg.ac.be/concept/thot-1000'})
    True

    >>> is_fix_applicable(fix_provider_thot_reference_thot, {'provider': 'thot', 'reference': 'www.foo.bar'})
    False

    >>> is_fix_applicable(fix_provider_topographical_bibliography_reference_topbib, {'provider': 'topographical bibliography', 'reference': 'http://thot.philo.ulg.ac.be/concept/topbib-100-100-10'})
    True

    """
    conf = parse_fix_name(fix.__name__)
    for key in conf.keys():
        if normalize_identifier(ref.get(key)) != normalize_identifier(conf.get(key)):
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

    >>> list(apply_single_fix('', {'reference': 'domain/path/ID'}, fix_provider_cfeetk_reference_cfeetk))
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

    >>> list(apply_all_fixes('', '', {'type': 'a', 'provider': 'b'}))
    []

    """
    fixes = get_applicable_fixes(collection_name, ref)
    res = [ref]
    if len(fixes) > 0:
        while len(fixes) > 0:
            fix = fixes.pop(0)
            log.debug(f'{ID}: apply {fix.__name__} to {res}.')
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

    >>> apply_defined_fixes('', '', [{'provider': 'a', 'type': 'b'}, {'type': 'vega', 'reference': 'x'}])
    [{'reference': 'x', 'provider': 'vega'}]

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


def save_ref(collection_name: str, ID: str, ref: dict):
    """ does just that (later this is being written to ``--stat-file``)
    """
    provider = ref.get('provider', 'null')
    ref_type = ref.get('type', 'null')
    ref_val = ref.get('reference', 'null')
    _stats[collection_name][provider][ref_type][ID] += [ref_val]


@filing(path='fixed_documents/before')
def _save_document(doc: dict):
    """ save document to path in decoration """
    return doc


@filing(path='fixed_documents/after')
def save_side_by_side_comparison_to_file(doc: dict, fixed_refs: list):
    """ saves a before and after version of an updated document to file for
    investigation with a diff tool.

    :returns: updated document
    """
    _save_document(doc)
    doc = update_document(doc, fixed_refs)
    log.debug(f'save {doc["eClass"]} doc {doc["_id"]} to file..')
    return doc


def do_fixes_apply(collection_name: str, ID: str, ref: dict) -> bool:
    """ apply fixes and see whether the result is different from the original
    ref. If so, this returns True.

    >>> do_fixes_apply('', '', {'provider': 'foo', 'type': 'bar', 'reference': 'id'})
    False

    >>> do_fixes_apply('', '', {'provider': 'cfeetk', 'reference': '287'})
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
            save_ref(collection_name, ID, ref)


def is_ref_in_list(ref: dict, refs: list) -> bool:
    """ determines if a ref occurs within the list of refs by comparing md5 sums

    >>> is_ref_in_list({'a': '1', 'b': '2'}, [{'b': '2', 'a': '1'}, {'c': '1'}])
    True

    """
    h = util.md5(ref)
    for r in refs:
        if util.md5(r) == h:
            return True
    return False


def update_document(doc: dict, refs: list) -> dict:
    """ replaces the document's ``externalReferences`` array with the passed refs,
    appends a newly created revision to the revision history

    :returns: updated doc
    """
    doc['externalReferences'] = refs
    add_revision(doc)
    return doc


def process_external_references(
    collection_name: str,
    doc: dict,
    apply_fixes: bool = False,
) -> list:
    """ does only collect external references information for now and saves
    it to --stat-file.

    With ``apply-fixes`` command in use, fixes will be applied to fixable refs and the
    document's ``externalReferences`` will be overwritten with the results.

    :returns: updated reference list in case any fixes have been applied,
              None otherwise
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
        log.debug(f'refs in document {doc["_id"]} changed.')
        if apply_fixes:
            log.debug(f'about to log changes to {doc["_id"]}..')
            for ref in fixed_refs:
                if not is_ref_in_list(ref, refs):
                    # TODO: log deletions as well!
                    log.debug(f' ref not in orig doc: {ref}')
                    save_ref(collection_name, doc['_id'], ref)
            return fixed_refs
    return None


def print_all_scenarios() -> list:
    for fix in get_fixes():
        name = fix.__name__
        print(f'{parse_fix_name(name)} -> {name}')


def count_refs_in_stats(*collection_names) -> int:
    """ how many refs have been stored in the stats dict

    >>> count_refs_in_stats()
    0

    >>> _stats['c1']['provider']['type']['doc_id'] += ['ref1', 'ref2']
    >>> count_refs_in_stats('c1')
    2
    """
    collection_names = collection_names if len(collection_names) > 0 \
        else _stats.keys()
    return sum(
        [
            len(ref_ids)
            for collection_name, providers in _stats.items()
            for provider, ref_types in providers.items()
            for ref_type, doc_ids in ref_types.items()
            for doc_id, ref_ids in doc_ids.items()
            if collection_name in collection_names
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


def upload_document(collection_name: str, doc: dict) -> bool:
    """ upload document to collection and indicate how that went.
    """
    collection = a64[collection_name]
    doc_id = doc['_id']
    local_rev = doc['_rev']
    remote_rev = collection[doc_id]['_rev']
    if remote_rev != local_rev:
        log.warning(f'doc {collection_name}/{doc_id} rev mismatch: '
                    f'l:{local_rev} vs. r:{remote_rev}. possible conflict')
    try:
        ID, new_rev = collection.save(doc)
        log.debug(f'successfully uploaded {doc_id}. rev: {local_rev} -> {new_rev}')
        return True
    except Exception:
        log.warning(f'failed to upload {doc_id}! remote: {remote_rev}, local: {local_rev}')
        return False



def gather_collection_stats(collection_name: str, apply_fixes: bool = False, upload: bool = False):
    """ go through every BTS document in collection and collect external
    references"""
    doc_count = query_bts_doc_count(collection_name)
    if doc_count > 0:
        for doc in all_docs_in_collection(collection_name):
            fixed_refs = process_external_references(
                collection_name,
                doc,
                apply_fixes=apply_fixes,
            )
            if fixed_refs is not None and apply_fixes is True:
                fixed_doc = deepcopy(doc)
                fixed_doc = save_side_by_side_comparison_to_file(fixed_doc, fixed_refs)
                if upload:
                    try:
                        if upload_document(collection_name, fixed_doc):
                            log.debug(f'successfully uploaded document {doc.get("_id")} to'
                                      f'collection {collection_name}.')
                        else:
                            log.info(f'problem updating couchdb: {doc.get("_id")}!')
                    except:
                        log.warning(f'could not upload document {doc.get("_id")} '
                                    f'to {collection_name}')


def execute_fixes_and_upload(collection_name: str, doc: dict):
    """ same as process_external_references, but updates documents and uploads them """
    # TODO
    pass


def list_collections():
    print(f'Non-empty collections in DB at {a64}:\n')
    collection_names = [cn for cn in a64]
    column_width = max(map(len, collection_names))
    for cn in collection_names:
        if len(a64[cn]) > 0:
            print(f'\t{cn:<{column_width}}\t {len(a64[cn])}')


def list_collection_stats(*collection_names):
    """
    >>> _stats['corpus']['p']['t1']['d1'].extend(['r1', 'r2', 'r3'])
    >>> _stats['corpus']['p']['t2']['d3'].extend(['r6'])
    >>> _stats['a']['p']['t1']['d2'].extend(['r4', 'r5'])
    >>> list_collection_stats('corpus', 'a', 'b')
     a        2
     b        
     corpus   4
    """
    if len(collection_names) < 0:
        collection_names = [cn for cn in a64]
    column_width = max(map(len, collection_names))
    for cn in sorted(collection_names):
        count = count_refs_in_stats(cn)
        if count < 1:
            count = ""
        print(f' {cn:<{column_width}}   {count}')


def main(args: dict):
    if args.get('list-fixes'):
        print_all_scenarios()
        return
    if args.get('generate-id'):
        print(
            generate_id()
        )
        return
    _stats_config['fixable'] = not(args.get('--non-fixable-only', False))
    _stats_config['non-fixable'] = not(args.get('--fixable-only', False))
    if len(args['<collection>']) < 1:
        if args['list-collections']:
            list_collections()
            return
        else:
            print('no collection names given. go through all collections..')
            collection_names = [cn for cn in a64]
    else:
        if args['list-collections']:
            print(
                'you can either specify <collection> or call '
                'list-collections but not both'
            )
            return
        else:
            collection_names = [cn for cn in args['<collection>'] if cn in a64]
    _completed = {c: False for c in collection_names}
    # go through specified collections
    if args.get('apply-fixes', False):
        log.info('will apply fixes..')
    if args.get('upload', False):
        log.info('will upload documents when fixed..')
    for collection_name in collection_names:
        while _completed[collection_name] is not True:
            try:
                gather_collection_stats(
                    collection_name,
                    apply_fixes=args.get('apply-fixes', False),
                    upload=args.get('upload', False),
                )
                _completed[collection_name] = True
            except ValueError:
                log.warning(f'server error during retrieval of {collection_name}. '
                            f'Trying again..')
                time.sleep(3)

    # save collected stats
    try:
        if all(_stats_config.values()):
            logged_refs_qualifier = ''
        else:
            logged_refs_qualifier = 'fixable' if _stats_config['fixable'] else 'non-fixable'
        with open(args['--stat-file'], 'w+') as f:
            print(f'writing {count_refs_in_stats()} {logged_refs_qualifier} references to '
                  f'file {args["--stat-file"]}.')
            json.dump(_stats, f, indent=2, sort_keys=True)
    except Exception as e:
        print(f'cannot write statistics to file {args["--stat-file"]}!')
        print(e)
    print(f'logged references per collection: ')
    list_collection_stats(*collection_names)


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    main(args)
