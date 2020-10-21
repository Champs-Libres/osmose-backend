# FILE FORM /qa-loader/ -> ne pas editer

from parsy import (
    regex,
    seq,
    string,
    whitespace,
    generate,
    seq,
    ParseError,
    match_item,
)

from collections import namedtuple

import xml.etree.ElementTree as ET

HasKey = namedtuple('HasKey', ['key'])
NotHasKey = namedtuple('NotHasKey', ['key'])
KeyEqVal = namedtuple('KeyEqVal', ['key', 'value'])
NotKeyEqVal = namedtuple('NotKeyEqVal', ['key', 'value'])
And = namedtuple('And', ['seq'])
Or = namedtuple('Or', ['seq'])
Intersects = namedtuple('Intersects', ['request_name']) # ou requête ?

whitespace1 = whitespace.at_least(1) # at least one ws
whitespace0 = whitespace.many() # 0 or n ws

# Lexemes
lex = {} # lexemes

for l in ['OR(', 'AND(', 'HAS_KEY', 'NOT_HAS_KEY', '=', '!=', 'INTERSECTS']:
    lex[l] = string(l)

lex[')'] = match_item(')')
lex[','] = match_item(',')

word = regex(r'[a-z_]+')


# Basic Rules
has = (lex['HAS_KEY'] >> whitespace1 >> word).map(lambda k: HasKey(k)).desc('HAS_KEY a_key')
not_has = (lex['NOT_HAS_KEY'] >> whitespace1 >> word).map(lambda k: NotHasKey(k)).desc('NOT_HAS_KEY a_key')
key_eq_val = seq(key=word, value=(whitespace0 >> lex['='] >> whitespace0 >> word)).combine_dict(KeyEqVal).desc('key = valye')
not_key_eq_val = seq(key=word, value=(whitespace0 >> lex['!='] >> whitespace0 >> word)).combine_dict(NotKeyEqVal).desc('key != value')
intersects = (lex['INTERSECTS'] >> whitespace1 >> word).map(lambda k: Intersects(k)).desc('INTERSECTS request_name')
basic_rules = has | not_has | key_eq_val | not_key_eq_val | intersects # todo simple rule ?

# Mix and & Or
@generate
def or_internal():
    return (yield lex[','] >> whitespace0 >> request)
    
@generate
def and_internal():
    return (yield lex[','] >> whitespace0 >> request)

@generate
def or_group():
    return (yield  (lex['OR('] >> whitespace0 >> (seq(request, or_internal.many()).combine(lambda e, next: Or([e] + next))) << whitespace0 << lex[')']))

@generate
def and_group():
    return (yield  (lex['AND('] >> whitespace0 >> (seq(request, and_internal.many()).combine(lambda e, next: And([e] + next))) << whitespace0 << lex[')']))


request = basic_rules | or_group | and_group

def parse_request(data):
    parsed = request.parse(data)
    return parsed

def xml_to_sql_fromstring(data, table_name=None):
    xml_analyser = ET.fromstring(data)
    return xml_to_sql(xml_analyser, table_name)

def xml_to_sql(node, table_name=None):
    ref_table = ''
    if table_name:
        ref_table = f'{table_name}.'

    if node.tag == 'and':
        eval_seq = [xml_to_sql(el, table_name) for el in node]
        ret = ') AND ('.join(eval_seq)
        return '(' + ret + ')'
    elif node.tag == 'or':
        eval_seq = [xml_to_sql(el, table_name) for el in node]
        ret = ') OR ('.join(eval_seq)
        return '(' + ret + ')'
    elif node.tag == 'has_tag':
        return f"{ref_table}tags?'{node.text}'"
    elif node.tag == 'key_value':
        if node.text.find("!=") > 0:
            k, v = node.text.split("!=")
            return f"{ref_table}tags->'{k}'!='{v}'"
        elif node.text.find("=") > 0:
            k, v = node.text.split("=")
            return f"{ref_table}tags->'{k}'='{v}'"
    elif node.tag == 'has_not_tag':
        return f"not {ref_table}tags?'{node.text}'"
    elif node.tag == 'intersects':
        return f"st_intersects({ref_table}geom, {node.text}.geom)"
    else:
        print(f'ERROR {node.tag}')


def evaluate(tuple):
    tuple_type = type(tuple).__name__

    if tuple_type == 'HasKey':
        return f"tags?'{tuple.key}'"

    elif tuple_type == 'NotHasKey':
        return f"not tags?'{tuple.key}'"

    elif tuple_type == 'KeyEqVal':
        return f"tags->'{tuple.key}'='{tuple.value}'"

    elif tuple_type == 'NotKeyEqVal':
        return f"tags->'{tuple.key}'!='{tuple.value}'"

    elif tuple_type == 'Intersects':
        return f"st_intersects(geom, {tuple.request_name}.geom)" # ou return f"st_intersects(el.geom, {tuple.request_name}.geom)"

    elif tuple_type == 'And':
        eval_seq = [evaluate(el) for el in tuple.seq]
        ret = ') AND ('.join(eval_seq)
        return '(' + ret + ')'

    elif tuple_type == 'Or':
        eval_seq = [evaluate(el) for el in tuple.seq]
        ret = ') OR ('.join(eval_seq)
        return '(' + ret + ')'


KEY_VALUE_TAG = 'key_value'
HAS_NOT_TAG_TAG = 'has_not_tag'
HAS_TAG_TAG  = 'has_tag'
AND_TAG  = 'and'
OR_TAG  = 'or'
INTERSECT_TAG = 'intersects'
 

def create_tag(tag, text=None):
    el = ET.Element(tag)
    if text:
        el.text = text
    return el

def ast_to_xml(tuple):
    # ne pas faire comme ça :
    # - générer à partir du xml créé 
    # - injecter sur le tag <insert></insert> ou <insert/
    tuple_type = type(tuple).__name__

    if tuple_type == 'HasKey':
        return create_tag(HAS_TAG_TAG, tuple.key)

    elif tuple_type == 'NotHasKey':
        return create_tag(HAS_NOT_TAG_TAG, tuple.key)

    elif tuple_type == 'KeyEqVal':
        return create_tag(KEY_VALUE_TAG, f"{tuple.key}={tuple.value}")

    elif tuple_type == 'NotKeyEqVal':
        return create_tag(KEY_VALUE_TAG, f"{tuple.key}!={tuple.value}")

    elif tuple_type == 'And':
        el = create_tag(AND_TAG)

        for child in tuple.seq:
            el.append(ast_to_xml(child))

        return el

    elif tuple_type == 'Or':
        el = create_tag(OR_TAG)

        for child in tuple.seq:
            el.append(ast_to_xml(child))

        return el

    elif tuple_type == 'Intersects':
        return create_tag(INTERSECT_TAG, tuple.request_name)


if __name__ == "__main__":
    print('Do the testing')

    print(lex['HAS_KEY'].parse('HAS_KEY'))

    try:
        print(word.parse('aza blop'))
    except ParseError as pex:
        print('ok')

    print(has.parse('HAS_KEY blop'))
    print(not_has.parse('NOT_HAS_KEY blop'))
    print(key_eq_val.parse('blop = true'))
    print(key_eq_val.parse('blop=true'))
    print(not_key_eq_val.parse('blop !=true'))

    # test basic_rules
    print(basic_rules.parse('blop= true'))

    print(or_internal.parse(', HAS_KEY blop'))

    # test Internal_or
    print(or_group.parse('OR(blop != true, HAS_KEY blop)'))
    print(request.parse('HAS_KEY blop'))

    test = request.parse('OR(blop != true, HAS_KEY blop,  AND(OR(blop != true, HAS_KEY blop,  AND(blop != true, HAS_KEY blop)), HAS_KEY blop))')
    print(test)
    print(evaluate(test))

    test = request.parse('AND(blop != true, HAS_KEY blop,  OR(AND(blop != true, HAS_KEY blop,  AND(blop != true, HAS_KEY blop)), HAS_KEY blop))')
    print(test)
    print(evaluate(test))

    test = request.parse('AND(HAS_KEY indoor, NOT_HAS_KEY level)')
    print(test)
    print(evaluate(test))

    test = request.parse('OR(NOT_HAS_KEY name,  shop=vacant)')

    test = request.parse('AND(HAS_KEY shop, OR(NOT_HAS_KEY name,  shop=vacant))')
    print(test)
    print(evaluate(test))

    test = request.parse('AND( OR(highway=bus_stop, highway=platform), NOT_HAS_KEY name)')
    print(test)
    print(evaluate(test))

    test = request.parse('AND( amenity=bicycle_parking, NOT_HAS_KEY capacity)')
    print(test)
    print(evaluate(test))

    test = request.parse("""AND(
OR(highway=bus_stop, highway=platform),
NOT_HAS_KEY name, INTERSECTS emprise_gare)""")
    print(test)
    print(evaluate(test))

    test = request.parse("""AND(
OR(highway=bus_stop, highway=platform),
OR(NOT_HAS_KEY name, HAS_KEY name),
INTERSECTS emprise_gare)""")
    print(test)
    print(evaluate(test))


    xml = """
<and>
    <has_tag>public_transport</has_tag>
    <key_value>public_transport=stop_area</key_value>
    <has_tag>network</has_tag>
    <or>
        <key_value>network=Transilien</key_value>
        <key_value>network=SNCF</key_value>
    </or>
</and>
"""
    print(xml_to_sql_fromstring(xml))


    xml2 = """
<and>
    <or>
        <key_value>highway=bus_stop</key_value>
        <key_value>highway=platform</key_value>
    </or>
    <or>
        <has_not_tag>name</has_not_tag>
        <has_tag>name</has_tag>
    </or>
    <intersects>emprise_gare</intersects>
</and>
"""

    print(xml_to_sql_fromstring(xml2))
