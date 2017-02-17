# -*- coding: utf-8 -*-

def columns_names(cur):
    names = []
    for desc in cur.description:
        name = desc[0]
        names.append(name)

    return names


def named_tuple(tuple, cur, to_lower=True):
    names = columns_names(cur)

    if to_lower:
        names = map(lambda x: x.lower(), names)

    return dict(zip(names, tuple))
