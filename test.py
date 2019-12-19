#!/usr/bin/env python
# -*- coding: utf-8 -*-

from aaew_etl import util

import fix_external_references as fixie

wlist_refs = [
    {
        'eClass': 'http://btsmodel/1.0#//BTSExternalReference',
        '_id': 'IBUBd6Zp86W4zklvsWJz28s4ajs',
        'reference': '114450',
        'type': 'aaew_wcn'
    }, 
    {
        'eClass': 'http://btsmodel/1.0#//BTSExternalReference', 
        '_id': 'IBgAg2YCWEGOQ0uJhkLv2TJBLcw', 
        'reference': 'http://sith.huma-num.fr/vocable/1365', 
        'provider': 'cfeetk'
    }
]

def test_have_fixes():
    assert len(fixie.get_fixes()) > 0

def test_wlist_whether_dza_ref_gets_created():
    refs = [ref for ref in wlist_refs]
    assert isinstance(refs[0], dict)
    fixed = fixie.apply_fixes_until_cows_come_home('', '', refs)
    providers = [fr.get('provider') for fr in fixed]
    assert 'dza' in providers
    assert 'aaew' in providers


def test_single_fix_aaew_wcn():
    ref = {'type': 'aaew_wcn', 'reference': '100'}
    fixed = fixie.apply_all_fixes('', '100', ref)
    ref = list(fixed)[0]
    assert 'provider' in ref
    assert ref['type'] != 'aaew_wcn'

def test_trismegistos():
    refs = [
        {'eClass': 'http://btsmodel/1.0#//BTSExternalReference',
         '_id': 'IBcBMSa03g80WUKkudaQnaBLBJc',
         'reference': 'www.trismegistos.org/text/52213',
         'provider': 'trismegistos'},
        {'eClass': 'http://btsmodel/1.0#//BTSExternalReference',
         '_id': 'IBcAhQMGHIAH9EElmzq8ipBQ2pk',
         'reference': 'www.trismegistos.org/text/88558',
         'provider': 'trismegistos'}
    ]
    fr = fixie.apply_defined_fixes('', '', refs)
    assert fr[0]['reference'] == '52213'
    assert fr[1]['reference'] == '88558'
    assert len(fr) == 2
    assert fr[0]['type'] == 'text'

