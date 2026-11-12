"""Разбор строк test_decoding с old-key / new-tuple: что и на сколько изменилось.

    python diff_update.py update_full.txt
"""
import re
import sys

# name[type]:value — type может содержать пробелы ("timestamp with time zone").
COL = re.compile(r"(\w+)\[([^\]]+)\]:('(?:[^']|'')*'|[^\s]+)")
SPLIT = re.compile(r" old-key: (.*?) new-tuple: (.*)$")


def parse_tuple(s):
    return {n: (t, v.strip("'")) for n, t, v in COL.findall(s)}


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def diff(line):
    m = SPLIT.search(line.strip())
    if not m:
        return None
    head = line.strip()[: m.start()]
    tbl = re.search(r"table (\S+?):", head)
    old, new = parse_tuple(m.group(1)), parse_tuple(m.group(2))
    out = []
    for col, (typ, nv) in new.items():
        ov = old.get(col, (typ, None))[1]
        if ov == nv:
            continue
        delta = ""
        a, b = _num(ov), _num(nv)
        if a is not None and b is not None:
            delta = f" (Δ={b - a:+g})"
        out.append(f"{col}: {ov} -> {nv}{delta}")
    return (tbl.group(1) if tbl else "?"), out


def main(argv):
    if len(argv) > 1:
        lines = open(argv[1], encoding="utf-8")
    else:
        lines = sys.stdin
    found = 0
    for ln in lines:
        r = diff(ln)
        if not r:
            continue
        found += 1
        tbl, changes = r
        print(f"{tbl} | " + ("; ".join(changes) if changes else "без изменений"))
    if not found:
        print("UPDATE-строк с old-key/new-tuple не найдено "
              "(проверьте, что REPLICA IDENTITY FULL)", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv)
