# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
from collections import OrderedDict
from urllib.parse import urlsplit, urlunsplit

def canonicalize(url: str) -> str:
    """
    Аккуратная нормализация:
    - трим пробелы
    - экранируем обратные слэши -> /
    - нижний регистр для scheme и host
    - убираем фрагмент (#...)
    - убираем дефолтные порты (:80, :443)
    (Путь/квери не трогаем, чтобы не слиплись разные ссылки)
    """
    u = url.strip().replace("\\", "/").strip('\'"<> ')
    if not u:
        return ""
    parts = urlsplit(u)
    scheme = (parts.scheme or "").lower()
    netloc = (parts.netloc or "").lower()

    # убрать дефолтные порты
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]

    # собрать без фрагмента
    return urlunsplit((scheme, netloc, parts.path, parts.query, ""))

def dedupe_lines(lines: list[str], loose: bool = False) -> list[str]:
    """
    Удаляет дубликаты, сохраняя порядок.
    loose=False: сравнение по строке после .strip()
    loose=True : сравнение по canonicalize()
    Возвращает список уникальных ЛИНИЙ для записи в файл.
    """
    seen = set()
    out = []
    for ln in lines:
        raw = ln.rstrip("\n")
        stripped = raw.strip()
        if not stripped:
            continue
        key = canonicalize(stripped) if loose else stripped
        if key in seen:
            continue
        seen.add(key)
        out.append(stripped)
    return out

def main():
    ap = argparse.ArgumentParser(description="Deduplicate sisters_discovered.txt")
    ap.add_argument("--file", default="sisters_discovered.txt",
                    help="Путь к входному файлу (по умолчанию sisters_discovered.txt)")
    ap.add_argument("--out", default=None,
                    help="Куда писать результат (по умолчанию перезаписываем --file)")
    ap.add_argument("--loose", action="store_true",
                    help="Включить мягкую нормализацию URL перед дедупликацией")
    args = ap.parse_args()

    with open(args.file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    unique = dedupe_lines(lines, loose=args.loose)

    out_path = args.out or args.file
    with open(out_path, "w", encoding="utf-8") as f:
        for u in unique:
            f.write(u + "\n")

    print(f"Готово. Было: {len(lines)} строк; стало уникальных: {len(unique)}. Записано в: {out_path}")

if __name__ == "__main__":
    main()
