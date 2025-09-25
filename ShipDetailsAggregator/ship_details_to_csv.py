# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
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

# Базовый перевод ключей на русский (без префиксов content_tb_*)
RU_KEY = {
    "Ship's Name / Hull No.": "Имя судна / № корпуса",
    "Ship Type:":             "Тип судна",
    "IMO:":                   "IMO",
    "Owner (or Operator):":   "Владелец / Оператор",
    "Built Date:":            "Дата постройки",
    "Contract Date:":         "Дата контракта",
    "Flag":                   "Флаг",
    "Length Overall":         "Длина наибольшая",
    "Length between Perp.":   "Длина между перпендикулярами",
    "Draught":                "Осадка",
    "Width":                  "Ширина",
    "Gross Tonnage":          "Валовая вместимость",
    "Deadweight":             "Дедвейт",
    "Fuel":                   "Топливо",
    "Main Engine":            "Главный двигатель",
    "Propulsion":             "Пропульсия",
    "Pump, Crane":            "Насосы / Краны",
    "Class":                  "Класс",
    "Builder:":               "Верфь (строитель)",
    "Hull No.":               "Номер корпуса",
    # fallback — оставим как есть, если чего-то не хватит
}

# Для кейсов, где одинаковый английский ключ встречается в разных блоках с разным смыслом,
# можно уточнить контекстом table_id
def translate_key(key_en: str, table_id: str) -> str:
    k = key_en.strip()
    if not k:
        return ""
    if k == "Hull No." and table_id == "content_tb_builder":
        return "Номер корпуса (builder)"
    return RU_KEY.get(k, k)

# ----------------- COMPOSITE FIELD PARSERS -----------------

def parse_fuel(value_text: str) -> Dict[str, str]:
    """
    Пример: "FuelType1: Yes, But Type Not Known , Capacity: UnKnown ; FuelType2: Not Applicable , Capacity: UnKnown"
    Достаём:
      - Топливо / Тип 1
      - Топливо / Тип 1 — Вместимость
      - Топливо / Тип 2
      - Топливо / Тип 2 — Вместимость
      - Топливо / Описание   (если встречается 'BunkersDescriptive: ...')
    """
    text = norm_spaces(value_text)
    out: Dict[str, str] = {}
    # разберём описатель, если он есть
    m_desc = re.search(r"BunkersDescriptive\s*:\s*(.+)$", text, flags=re.I)
    if m_desc:
        out["Топливо / Описание"] = m_desc.group(1).strip()
        text = re.sub(r"BunkersDescriptive\s*:\s*.+$", "", text, flags=re.I).strip()

    # Разбиваем по ';' на сегменты типов
    parts = [p.strip(" ;") for p in text.split(";") if p.strip(" ;")]
    for part in parts:
        # FuelTypeN: XXX , Capacity: YYY
        m = re.search(r"FuelType\s*(\d+)\s*:\s*([^,;]+)(?:,|\s|$)", part, flags=re.I)
        if m:
            n = m.group(1)
            typ = m.group(2).strip()
            out[f"Топливо / Тип {n}"] = typ
        m2 = re.search(r"Capacity\s*:\s*([^;]+)$", part, flags=re.I)
        if m2:
            cap = m2.group(1).strip()
            # Если был FuelTypeN поблизости — привяжем к тому же номеру, иначе просто «Вместимость»
            if 'n' in locals():
                out[f"Топливо / Тип {n} — Вместимость"] = cap
            else:
                out["Топливо / Вместимость"] = cap
    return out

def parse_main_engine(value_text: str) -> Dict[str, str]:
    """
    Пример:
      "Design: MAN-B&W , Engine Builder: HD Hyundai Heavy Industries Co Ltd - South Korea , 1 x 7S60ME-C10-GI , 2 , IN-LINE,VERTICAL 7 Cy , 600 x 2400 , Mcr: 12600(17131) at 93 rpm"
    Извлекаем:
      - ГД / Конструкция (Design)
      - ГД / Производитель (Engine Builder)
      - ГД / Кол-во × модель               (напр. '1 x 7S60ME-C10-GI')
      - ГД / Схема и цилиндры             (напр. 'IN-LINE,VERTICAL 7 Cy')
      - ГД / Диаметр × ход                (напр. '600 x 2400')
      - ГД / Мощность MCR                 (число)
      - ГД / MCR (макс)                   (в скобках)
      - ГД / обороты (rpm)                (число)
    """
    t = norm_spaces(value_text)
    out: Dict[str, str] = {}

    # Design
    m = re.search(r"\bDesign\s*:\s*([^,;]+)", t, flags=re.I)
    if m:
        out["ГД / Конструкция (Design)"] = m.group(1).strip()

    # Engine Builder
    m = re.search(r"\bEngine\s+Builder\s*:\s*([^,;]+(?:,[^,;]+)*)", t, flags=re.I)
    if m:
        out["ГД / Производитель (Engine Builder)"] = m.group(1).strip()

    # Кол-во × модель: "1 x 7S60ME-C10-GI"
    m = re.search(r"\b(\d+)\s*x\s*([A-Za-z0-9\-]+[A-Za-z0-9\-\/]*)", t)
    if m:
        out["ГД / Кол-во × модель"] = f"{m.group(1)} x {m.group(2)}"

    # Схема и цилиндры: "IN-LINE,VERTICAL 7 Cy" или "IN-LINE 6 Cy"
    m = re.search(r"(IN\-LINE|INLINE|V\-TYPE|VTYPE|H\-TYPE|HORIZONTAL|VERTICAL)[^,;]*?(?:,\s*VERTICAL|,\s*HORIZONTAL)?\s+(\d+)\s*Cy", t, flags=re.I)
    if m:
        scheme = m.group(0)
        out["ГД / Схема и цилиндры"] = norm_spaces(scheme)

    # Диаметр × ход: "600 x 2400"
    m = re.search(r"\b(\d{2,5})\s*x\s*(\d{2,5})\b", t)
    if m:
        out["ГД / Диаметр × ход"] = f"{m.group(1)} x {m.group(2)}"

    # Mcr: 12600(17131) at 93 rpm  (скобочная часть опциональна)
    m = re.search(r"\bMcr\s*:\s*([\d,]+)(?:\(([\d,]+)\))?\s*at\s*(\d+)\s*rpm", t, flags=re.I)
    if m:
        out["ГД / Мощность MCR"] = m.group(1).replace(",", "").strip()
        if m.group(2):
            out["ГД / MCR (макс)"] = m.group(2).replace(",", "").strip()
        out["ГД / обороты (rpm)"] = m.group(3).strip()

    return out

def parse_propulsion(value_text: str) -> Dict[str, str]:
    """
    Пример: "Centre Or Only  Propeller : Fixed Pitch(93 rpm)"
    Достаём 'Тип винта' и возможное примечание.
    """
    t = norm_spaces(value_text)
    out: Dict[str, str] = {}
    # Основной тип после 'Propeller :'
    m = re.search(r"Propeller\s*:\s*([^;]+)$", t, flags=re.I)
    if m:
        out["Пропульсия / Тип винта"] = m.group(1).strip()
    # Примечание — всё остальное без 'Propeller :'
    t2 = re.sub(r"Propeller\s*:\s*[^;]+$", "", t, flags=re.I).strip()
    if t2 and (not out or t2.lower() not in ("centre or only", "center or only")):
        out["Пропульсия / Примечание"] = t2
    return out

# ----------------- EXTRACTION -----------------

def extract_kv_ru(table_id: str, key_en: str, value_text: str, include_links: bool, links: List[Dict[str, str]]) -> Dict[str, str]:
    """
    Возвращает пары {Русский заголовок: значение}.
    Для составных полей добавляет несколько пар.
    Для обычных — одну пару: {переведённый ключ: value_text}.
    """
    key_ru = translate_key(key_en, table_id) or key_en.strip()
    val = norm_spaces(value_text)

    # Три кастомных парсера
    if key_en.strip() == "Fuel":
        out = parse_fuel(val)
        if not out:
            out = {"Топливо": val}
        return out

    if key_en.strip() == "Main Engine":
        out = parse_main_engine(val)
        if not out:
            out = {"Главный двигатель": val}
        return out

    if key_en.strip() == "Propulsion":
        out = parse_propulsion(val)
        if not out:
            out = {"Пропульсия": val}
        return out

    # Обычный кейс
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

@dataclass
class ShipDetailsToCSV:
    in_json: Path
    out_csv: Path
    include_links: bool = False         # доп. колонки со ссылками
    excel_friendly: bool = True         # BOM для Excel (utf-8-sig)

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

                # базовые поля
                url    = rec.get("url", "")
                ts     = rec.get("ts", "")
                origin = rec.get("origin_yard", "")
                row = [url, ts, origin]

                # динамика
                for col in header[3:]:
                    row.append(row_map.get(col, ""))

                w.writerow(row)
                written += 1

        print(f"[CSV] Готово: {self.out_csv}  (строк: {written}, колонок: {len(header)})")
        return written, len(header)

def main():
    base = Path(__file__).resolve().parent.parent   # корень проекта
    in_json = base / "ship_details_merged.json"
    out_csv = base / "ship_details_merged_ru.csv"

    conv = ShipDetailsToCSV(
        in_json=in_json,
        out_csv=out_csv,
        include_links=False,         # поставьте True, если нужны колонки с ссылками
        excel_friendly=True,
    )
    conv.write_csv()

if __name__ == "__main__":
    main()
