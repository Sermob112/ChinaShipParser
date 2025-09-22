# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional
from urllib.parse import urljoin

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


@dataclass
class YardsCollector:
    """
    Собирает список верфей (портов) со страницы shipyards.aspx из таблицы #content_tbYards.
    НЕ удаляет дубликаты по ссылке, чтобы сохранить все строки как на странице.
    Можно включить dedupe=True для удаления дублей по href.
    """
    driver: WebDriver
    base_url: str = "http://chinashipbuilding.cn/"
    wait_sec: int = 25

    def _open(self, url: str) -> None:
        self.driver.get(url)
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    def _wait_table(self) -> None:
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table#content_tbYards"))
        )

    @staticmethod
    def _norm(text: Optional[str]) -> str:
        return (text or "").replace("\xa0", " ").strip()

    def collect_yards(self, page_url: str, dedupe: bool = False) -> List[Dict[str, str]]:
        """
        Возвращает список словарей:
          {"no": 6, "name": "CSSC, Chengxi Shipyard (Yangzhou)", "link": "http://.../shipyard.aspx?..."}
        dedupe=False — сохраняем каждый ряд (даже если href повторяется).
        """
        self._open(page_url)
        self._wait_table()

        table = self.driver.find_element(By.CSS_SELECTOR, "table#content_tbYards")
        # иногда в tbody нет, поэтому берём просто tr из таблицы
        rows = table.find_elements(By.CSS_SELECTOR, "tr")

        out: List[Dict[str, str]] = []
        for tr in rows:
            # каждая строка держит <td> с "N. <a><b>NAME</b></a><br>"
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if not tds:
                continue
            td = tds[0]

            raw = self._norm(td.text)  # например: "6. CSSC, Chengxi Shipyard (Yangzhou)"
            # номер перед точкой
            no = None
            dot = raw.find(".")
            if dot > 0:
                maybe_no = raw[:dot].strip()
                if maybe_no.isdigit():
                    no = int(maybe_no)

            # ссылка и название
            try:
                link_el = td.find_element(By.CSS_SELECTOR, 'a[href*="shipyard.aspx"]')
            except Exception:
                continue

            # название чаще в <b>, но fallback — текст ссылки
            try:
                name_text = self._norm(link_el.find_element(By.CSS_SELECTOR, "b").text)
            except Exception:
                name_text = self._norm(link_el.text)

            href_abs = ""
            href_raw = link_el.get_attribute("href") or ""
            if href_raw:
                href_abs = urljoin(page_url, href_raw)

            out.append({
                "no": no if no is not None else "",
                "name": name_text,
                "link": href_abs,
            })

        if dedupe:
            # по желанию — убрать повторы по ссылке, сохранить первый вариант
            seen = set()
            unique = []
            for item in out:
                href = item.get("link", "")
                if href and href not in seen:
                    seen.add(href)
                    unique.append(item)
            out = unique

        # сортируем по номеру, если он есть (иначе — в хвост)
        out.sort(key=lambda x: (999999 if x["no"] in ("", None) else int(x["no"])))
        return out

    @staticmethod
    def save_json(items: List[Dict[str, str]], out_path: str) -> None:
        import json
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
