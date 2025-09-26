# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Tuple
import json
import csv
import re

# ----------------- UTILS -----------------

def load_json_any(path: Path) -> List[Dict[str, Any]]:
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    try:
        obj = json.loads(raw)
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
        if isinstance(obj, dict):
            return [obj]
    except json.JSONDecodeError:
        out = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    out.append(row)
            except Exception:
                pass
        return out
    return []

def norm_spaces(s: str) -> str:
    s = (s or "").replace("\r", "\n").replace("\xa0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s)
    return s.strip()

def norm_header(s: str) -> str:
    s = (s or "").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > 240:
        s = s[:240]
    return s

# ----------------- TRANSLATION -----------------

RU_KEY = {
    # ship
    "Ship's Name / Hull No.": "Имя судна / № корпуса",
    "Ship's Name:":           "Имя судна",
    "Ship Type:":             "Тип судна",
    "IMO:":                   "IMO",
    "Owner (or Operator):":   "Владелец / Оператор",
    "Built Date:":            "Дата постройки",
    "Contract Date:":         "Дата контракта",

    # feature
    "Flag":                   "Флаг",
    "Call Sign":              "Позывной",
    "Length Overall":         "Длина наибольшая",
    "Length between Perp.":   "Длина между перпендикулярами",
    "Draught":                "Осадка",
    "Width":                  "Ширина",
    "Service Speed":          "Служебная скорость",
    "Gross Tonnage":          "Валовая вместимость",
    "Deadweight":             "Дедвейт",
    "Fuel":                   "Топливо",
    "Thruster":               "Подруливающее устройство",
    "Roro Equipment":         "Ro-Ro оборудование",

    # class
    "Class":                  "Класс",
    "Class Notation":         "Классификационные обозначения",
    "Class Inspection":       "Осмотры класса",

    # equip
    "Main Engine":            "Главный двигатель",
    "Auxi Engine":            "Вспомогательный двигатель",
    "Emergency Generator":    "Аварийный генератор",
    "Propulsion":             "Пропульсия",

    # builder
    "Builder:":               "Верфь (строитель)",
    "Hull No.":               "Номер корпуса",
}

def translate_key(key_en: str, table_id: str) -> str:
    k = key_en.strip()
    if not k:
        return ""
    if k == "Hull No." and table_id == "content_tb_builder":
        return "Номер корпуса (builder)"
    return RU_KEY.get(k, k)

# ----------------- COMPOSITE FIELD PARSERS -----------------

def parse_fuel(value_text: str) -> Dict[str, str]:
    text = norm_spaces(value_text)
    out: Dict[str, str] = {}
    m_desc = re.search(r"BunkersDescriptive\s*:\s*(.+)$", text, flags=re.I)
    if m_desc:
        out["Топливо / Описание"] = m_desc.group(1).strip()
        text = re.sub(r"BunkersDescriptive\s*:\s*.+$", "", text, flags=re.I).strip()

    parts = [p.strip(" ;") for p in text.split(";") if p.strip(" ;")]
    for part in parts:
        n = None
        m = re.search(r"FuelType\s*(\d+)\s*:\s*([^,;]+)", part, flags=re.I)
        if m:
            n = m.group(1)
            typ = m.group(2).strip()
            out[f"Топливо / Тип {n}"] = typ
        m2 = re.search(r"Capacity\s*:\s*([^;]+)$", part, flags=re.I)
        if m2:
            cap = m2.group(1).strip()
            if n:
                out[f"Топливо / Тип {n} — Вместимость"] = cap
            else:
                out["Топливо / Вместимость"] = cap
    return out

def _parse_engine_generic(value_text: str, prefix: str) -> Dict[str, str]:
    """
    Универсальный разбор для Main Engine / Auxi Engine.
    prefix = "ГД" (главный) или "ВД" (вспомогательный)
    """
    t = norm_spaces(value_text)
    out: Dict[str, str] = {}

    m = re.search(r"\bDesign\s*:\s*([^,;]+)", t, flags=re.I)
    if m:
        out[f"{prefix} / Конструкция (Design)"] = m.group(1).strip()

    m = re.search(r"\bEngine\s+Builder\s*:\s*([^,;]+(?:,[^,;]+)*)", t, flags=re.I)
    if m:
        out[f"{prefix} / Производитель (Engine Builder)"] = m.group(1).strip()

    m = re.search(r"\b(\d+)\s*x\s*([A-Za-z0-9\-]+[A-Za-z0-9\-\/]*)", t)
    if m:
        out[f"{prefix} / Кол-во × модель"] = f"{m.group(1)} x {m.group(2)}"

    m = re.search(r"(IN\-LINE|INLINE|V\-TYPE|VTYPE|H\-TYPE|HORIZONTAL|VERTICAL)[^,;]*?(?:,\s*VERTICAL|,\s*HORIZONTAL)?\s+(\d+)\s*Cy", t, flags=re.I)
    if m:
        out[f"{prefix} / Схема и цилиндры"] = norm_spaces(m.group(0))

    m = re.search(r"\b(\d{2,5})\s*x\s*(\d{2,5})\b", t)
    if m:
        out[f"{prefix} / Диаметр × ход"] = f"{m.group(1)} x {m.group(2)}"

    m = re.search(r"\bMcr\s*:\s*([\d,]+)(?:\(([\d,]+)\))?\s*at\s*(\d+)\s*rpm", t, flags=re.I)
    if m:
        out[f"{prefix} / Мощность MCR"] = m.group(1).replace(",", "").strip()
        if m.group(2):
            out[f"{prefix} / MCR (макс)"] = m.group(2).replace(",", "").strip()
        out[f"{prefix} / обороты (rpm)"] = m.group(3).strip()

    return out

def parse_main_engine(value_text: str) -> Dict[str, str]:
    return _parse_engine_generic(value_text, prefix="ГД")

def parse_aux_engine(value_text: str) -> Dict[str, str]:
    return _parse_engine_generic(value_text, prefix="ВД")

def parse_propulsion(value_text: str) -> Dict[str, str]:
    t = norm_spaces(value_text)
    out: Dict[str, str] = {}
    m = re.search(r"Propeller\s*:\s*([^;]+)$", t, flags=re.I)
    if m:
        out["Пропульсия / Тип винта"] = m.group(1).strip()
    t2 = re.sub(r"Propeller\s*:\s*[^;]+$", "", t, flags=re.I).strip()
    if t2 and (not out or t2.lower() not in ("centre or only", "center or only")):
        out["Пропульсия / Примечание"] = t2
    return out

# ----------------- EXTRACTION -----------------

def extract_kv_ru(table_id: str, key_en: str, value_text: str, include_links: bool, links: List[Dict[str, str]]) -> Dict[str, str]:
    key_ru = translate_key(key_en, table_id) or key_en.strip()
    val = norm_spaces(value_text)

    # составные поля
    if key_en.strip() == "Fuel":
        out = parse_fuel(val) or {"Топливо": val}
        return out
    if key_en.strip() == "Main Engine":
        out = parse_main_engine(val) or {"Главный двигатель": val}
        return out
    if key_en.strip() == "Auxi Engine":
        out = parse_aux_engine(val) or {"Вспомогательный двигатель": val}
        return out
    if key_en.strip() == "Propulsion":
        out = parse_propulsion(val) or {"Пропульсия": val}
        return out

    # обычные поля
    out = {key_ru: val}

    # (опционально) добавим колонку со ссылками
    if include_links and links:
        joined = "; ".join(
            f"{(l.get('text') or '').strip()}|{(l.get('href') or '').strip()}"
            for l in links if l.get("href")
        )
        if joined:
            out[f"{key_ru}__ссылки"] = joined

    return out

# ----------------- CONVERTER -----------------

@dataclass
class ShipDetailsToCSV10k:
    in_json: Path
    out_csv: Path
    include_links: bool = False
    excel_friendly: bool = True

    def _collect_header(self, records: List[Dict[str, Any]]) -> List[str]:
        cols = ["url", "ts", "origin_yard"]
        seen = set(cols)
        for rec in records:
            tables = rec.get("tables") or []
            for tb in tables:
                table_id = str(tb.get("table_id") or "").strip()
                rows = tb.get("rows") or []
                for row in rows:
                    key_en = str(row.get("key") or "").strip()
                    val = row.get("value_text") or ""
                    links = row.get("links") or []
                    pairs = extract_kv_ru(table_id, key_en, val, self.include_links, links)
                    for k in pairs.keys():
                        kh = norm_header(k)
                        if kh not in seen:
                            seen.add(kh)
                            cols.append(kh)
        return cols

    def write_csv(self) -> Tuple[int, int]:
        records = load_json_any(self.in_json)
        if not records:
            print(f"[CSV] Пустой вход: {self.in_json}")
            return 0, 0

        header = self._collect_header(records)
        newline = ""
        encoding = "utf-8-sig" if self.excel_friendly else "utf-8"

        written = 0
        with open(self.out_csv, "w", encoding=encoding, newline=newline) as f:
            w = csv.writer(f)
            w.writerow(header)
            for rec in records:
                row_map: Dict[str, str] = {}
                tables = rec.get("tables") or []
                for tb in tables:
                    table_id = str(tb.get("table_id") or "").strip()
                    rows = tb.get("rows") or []
                    for row in rows:
                        key_en = str(row.get("key") or "").strip()
                        val = row.get("value_text") or ""
                        links = row.get("links") or []
                        pairs = extract_kv_ru(table_id, key_en, val, self.include_links, links)
                        for k, v in pairs.items():
                            row_map[norm_header(k)] = v

                url    = rec.get("url", "")
                ts     = rec.get("ts", "")
                origin = rec.get("origin_yard", "")
                row = [url, ts, origin]

                for col in header[3:]:
                    row.append(row_map.get(col, ""))

                w.writerow(row)
                written += 1

        print(f"[CSV] Готово: {self.out_csv}  (строк: {written}, колонок: {len(header)})")
        return written, len(header)

# ----------------- CLI -----------------

def main():
    base = Path(__file__).resolve().parent
    in_json = base / "ship_details_merged_10k.json"
    out_csv = base / "ship_details_merged_10k_ru.csv"

    conv = ShipDetailsToCSV10k(
        in_json=in_json,
        out_csv=out_csv,
        include_links=False,   # True — если нужны доп. столбцы со ссылками
        excel_friendly=True,
    )
    conv.write_csv()

if __name__ == "__main__":
    main()
