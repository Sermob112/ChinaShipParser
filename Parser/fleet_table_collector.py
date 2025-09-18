# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Callable
from urllib.parse import urljoin
import json
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


@dataclass
class FleetTableCollector:
    """
    Собирает строки из таблицы #content_tb_fleet и ссылки пагинации из #content_lnk_page.

    Методы:
      - collect_rows(page_url) -> List[dict]
      - collect_all_pagination_links(start_url) -> List[dict]   # обходит все блоки через >>
      - walk_pages_incremental(start_url, save_rows_cb, save_pager_cb)  # ИНКРЕМЕНТАЛЬНЫЙ ПРОХОД
    """
    driver: WebDriver
    base_url: str = "http://chinashipbuilding.cn/"
    wait_sec: int = 25

    # -------- базовые ожидания/нормализация --------
    def _open(self, url: str) -> None:
        self.driver.get(url)
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    def _wait_table(self) -> None:
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table#content_tb_fleet"))
        )

    def _wait_pager(self) -> None:
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#content_lnk_page"))
        )

    @staticmethod
    def _norm(text: Optional[str]) -> str:
        return (text or "").replace("\xa0", " ").strip()

    # -------- сбор таблицы --------
    def collect_rows(self, page_url: str) -> List[Dict[str, str]]:
        self._open(page_url)
        self._wait_table()

        table = self.driver.find_element(By.CSS_SELECTOR, "table#content_tb_fleet")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody > tr")
        out: List[Dict[str, str]] = []
        if not rows:
            return out

        data_rows = rows[1:] if len(rows) > 1 else []  # пропускаем шапку

        for tr in data_rows:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 6:
                continue

            no = self._norm(tds[0].text)
            name_td = tds[1]
            ship_type = self._norm(tds[2].text)
            owner_company = self._norm(tds[3].text)
            shipyard = self._norm(tds[4].text)
            date_built = self._norm(tds[5].text)

            # ссылка из колонки "Ship's Name"
            link_el = None
            try:
                link_el = name_td.find_element(By.CSS_SELECTOR, 'a[href*="ship.aspx"]')
            except Exception:
                pass

            href_abs = ""
            if link_el:
                href_raw = link_el.get_attribute("href") or ""
                if href_raw:
                    href_abs = urljoin(page_url, href_raw)

            name_text = self._norm(link_el.text if link_el else name_td.text)

            out.append({
                "no": no,
                "name": name_text,
                "ship_type": ship_type,
                "owner_company": owner_company,
                "shipyard": shipyard,
                "date_built": date_built,
                "link": href_abs,
            })

        return out

    # -------- пагинация --------
    def _extract_pager_links_on_current_page(self, current_url: str) -> Tuple[List[Dict[str, str]], Optional[str], Optional[str]]:
        """
        Возвращает:
          - список ссылок текущего блока пагинации: [{"text": "2", "href": "abs-url"}, ...]
          - href на '>>' (абсолютный) если он есть, иначе None
          - текущий номер страницы (берём жирный <b>...</b>)
        """
        self._wait_pager()
        pager = self.driver.find_element(By.CSS_SELECTOR, "#content_lnk_page")

        # текущая страница — в <b>1</b>
        current_page_no: Optional[str] = None
        try:
            b = pager.find_element(By.CSS_SELECTOR, "b")
            current_page_no = self._norm(b.text)
        except Exception:
            pass

        anchors = pager.find_elements(By.CSS_SELECTOR, 'a[href]')
        links: List[Dict[str, str]] = []
        next_block_abs: Optional[str] = None

        for a in anchors:
            text = self._norm(a.text)
            href_raw = a.get_attribute("href") or ""
            if not href_raw:
                continue
            href_abs = urljoin(current_url, href_raw)

            if text == ">>":
                next_block_abs = href_abs
            else:
                links.append({"text": text, "href": href_abs})

        return links, next_block_abs, current_page_no

    def collect_all_pagination_links(self, start_url: str) -> List[Dict[str, str]]:
        seen_hrefs = set()
        result: List[Dict[str, str]] = []

        current_url = start_url
        while True:
            self._open(current_url)
            block_links, next_block, _ = self._extract_pager_links_on_current_page(current_url)

            for item in block_links:
                if item["href"] not in seen_hrefs:
                    seen_hrefs.add(item["href"])
                    result.append(item)

            if next_block and next_block not in seen_hrefs:
                seen_hrefs.add(next_block)
                current_url = next_block
                continue
            break

        def key_fn(it: Dict[str, str]):
            t = it["text"]
            return int(t) if t.isdigit() else 10**9
        result.sort(key=key_fn)
        return result

    # -------- инкрементальный обход всего флота --------
    def walk_pages_incremental(
        self,
        start_url: str,
        save_rows_cb: Callable[[int, str, List[Dict[str, str]]], None],
        save_pager_cb: Callable[[int, str, List[Dict[str, str]]], None],
        max_pages: Optional[int] = None
    ) -> None:
        """
        Идём от start_url по страницам одну за другой.
        После КАЖДОЙ страницы сохраняем строки (save_rows_cb).
        Пагинацию сохраняем ТОЛЬКО на страницах 1, 11, 21, ... (save_pager_cb).

        Алгоритм:
          - открываем текущую страницу
          - читаем таблицу -> save_rows_cb(page_no, url, rows)
          - извлекаем пагинацию; если page_no % 10 == 1 -> save_pager_cb(...)
          - вычисляем next_url:
              * ищем в текущем pager ссылку с номером (page_no + 1)
              * если её нет, но есть '>>' -> переходим на '>>', затем там ищем (page_no + 1)
              * если ничего нет — выходим
        """
        current_url = start_url
        visited = set()

        while True:
            self._open(current_url)
            # Порой сайт «долгий» — чуть подождём до отрисовки
            time.sleep(0.2)

            # 1) таблица
            rows = self.collect_rows(current_url)  # уже откроет и дождётся table
            # после collect_rows страница уже открыта; получим pager на ТЕКУЩЕЙ
            _, _, page_no_str = self._extract_pager_links_on_current_page(current_url)
            try:
                page_no = int(page_no_str) if page_no_str and page_no_str.isdigit() else None
            except Exception:
                page_no = None

            save_rows_cb(page_no or -1, current_url, rows)

            # 2) пагинация (сохраняем только на 1, 11, 21, ...)
            block_links, next_block, _ = self._extract_pager_links_on_current_page(current_url)
            if page_no is None:
                page_no = 1  # на всякий случай
            if (page_no - 1) % 10 == 0:
                save_pager_cb(page_no, current_url, block_links)

            # 3) следующий URL
            next_page_no = (page_no + 1) if page_no is not None else None
            next_link = None
            if next_page_no is not None:
                # сперва ищем ссылку на следующую страницу в текущем блоке
                for it in block_links:
                    if it["text"].isdigit() and int(it["text"]) == next_page_no:
                        next_link = it["href"]
                        break

            # если не нашли — возможно, мы на 10-й/20-й/... странице; дергаем '>>'
            if not next_link and next_block:
                self._open(next_block)
                # взяли новый блок, но сохранять его в файл пагинации НЕ будем (условие экономии дублей)
                new_block_links, new_next_block, _ = self._extract_pager_links_on_current_page(next_block)
                # ищем всё тот же next_page_no в новом блоке
                if next_page_no is not None:
                    for it in new_block_links:
                        if it["text"].isdigit() and int(it["text"]) == next_page_no:
                            next_link = it["href"]
                            break
                # обновим ссылку на следующий блок для последующих шагов
                next_block = new_next_block

            if not next_link or next_link in visited:
                break

            visited.add(next_link)
            current_url = next_link

            if max_pages is not None and len(visited) >= max_pages:
                break

    # -------- утилиты сохранения --------
    @staticmethod
    def save_json(items: List[Dict[str, str]], out_path: str) -> None:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)

    @staticmethod
    def append_json(items: List[Dict[str, str]], out_path: str) -> None:
        """
        Безопасно дозаписывает список словарей в JSON-файл постранично.
        Если файла нет — создаём как JSON-массив. Если есть — читаем, добавляем, перезаписываем.
        (Для очень больших данных лучше писать per-page JSON, как в лаунчере.)
        """
        try:
            with open(out_path, "r", encoding="utf-8") as f:
                arr = json.load(f)
                if not isinstance(arr, list):
                    arr = []
        except Exception:
            arr = []
        arr.extend(items)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(arr, f, ensure_ascii=False, indent=2)
