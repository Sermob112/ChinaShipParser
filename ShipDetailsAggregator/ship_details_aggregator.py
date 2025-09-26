# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import json
import hashlib
import os

def norm_url(u: str) -> str:
    """Мягкая нормализация URL: трим, нижний регистр для схемы/хоста, без #fragment."""
    from urllib.parse import urlsplit, urlunsplit
    s = (u or "").strip().replace("\\", "/").strip('\'"<> ')
    if not s:
        return ""
    parts = urlsplit(s)
    scheme = (parts.scheme or "").lower()
    netloc = (parts.netloc or "").lower()
    # убрать дефолтный порт
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]
    return urlunsplit((scheme, netloc, parts.path, parts.query, ""))

@dataclass
class ShipDetailsAggregator:
    in_dir: Path
    out_json: Path
    out_ndjson: Optional[Path] = None
    # поведение на дубли:
    prefer_more_tables: bool = True
    prefer_newer_ts: bool = True

    def _iter_files(self) -> List[Path]:
        return sorted(self.in_dir.glob("ship_*.json"))

    def _load_node(self, p: Path) -> Optional[Dict[str, Any]]:
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if not isinstance(obj, dict):
                return None
            # базовая валидация
            if not obj.get("url"):
                return None
            # добавим служебное поле: размер файла — пригодится при тай-брейке
            obj["_filesize"] = p.stat().st_size
            obj["_filepath"] = str(p)
            return obj
        except Exception:
            return None

    def _score(self, node: Dict[str, Any]) -> Tuple[int, int, int]:
        # Чем больше — тем «лучше»
        tables = node.get("tables") or []
        tables_count = len(tables) if isinstance(tables, list) else 0
        ts = int(node.get("ts", 0)) if str(node.get("ts", "")).isdigit() else 0
        fsz = int(node.get("_filesize", 0))
        # базовый скор — в порядке важности
        return (tables_count if self.prefer_more_tables else 0,
                ts            if self.prefer_newer_ts else 0,
                fsz)

    def _choose_best(self, a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        # сравним по score
        sa = self._score(a)
        sb = self._score(b)
        if sb > sa:
            return b
        return a

    def aggregate(self) -> Dict[str, Dict[str, Any]]:
        best_by_url: Dict[str, Dict[str, Any]] = {}
        files = self._iter_files()
        total = 0
        for p in files:
            node = self._load_node(p)
            if not node:
                continue
            total += 1
            url_raw = node.get("url", "")
            url = norm_url(url_raw)
            if not url:
                continue
            if url in best_by_url:
                best_by_url[url] = self._choose_best(best_by_url[url], node)
            else:
                best_by_url[url] = node
        print(f"[AGG] Прочитано файлов: {total}; уникальных по URL: {len(best_by_url)}")
        return best_by_url

    def save_json(self, merged: Dict[str, Dict[str, Any]]) -> None:
        # уберём служебные поля
        arr = []
        for url, node in merged.items():
            obj = dict(node)
            obj.pop("_filesize", None)
            obj.pop("_filepath", None)
            arr.append(obj)
        with open(self.out_json, "w", encoding="utf-8") as f:
            json.dump(arr, f, ensure_ascii=False, indent=2)
        print(f"[AGG] JSON сохранён: {self.out_json}  (записей: {len(arr)})")

    def save_ndjson(self, merged: Dict[str, Dict[str, Any]]) -> None:
        if not self.out_ndjson:
            return
        with open(self.out_ndjson, "w", encoding="utf-8") as f:
            for url, node in merged.items():
                obj = dict(node)
                obj.pop("_filesize", None)
                obj.pop("_filepath", None)
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        print(f"[AGG] NDJSON сохранён: {self.out_ndjson}")

def main():
    base = Path(__file__).resolve().parent.parent  # корень проекта (на уровень выше папки ShipDetailsAggregator)
    in_dir = base / "10_ship_details"
    out_json = base / "ship_details_merged_10k.json"
    out_ndjson = base / "ship_details_merged_10k.ndjson"

    agg = ShipDetailsAggregator(
        in_dir=in_dir,
        out_json=out_json,
        out_ndjson=out_ndjson,
        prefer_more_tables=True,
        prefer_newer_ts=True,
    )
    merged = agg.aggregate()
    agg.save_json(merged)
    agg.save_ndjson(merged)

if __name__ == "__main__":
    main()
