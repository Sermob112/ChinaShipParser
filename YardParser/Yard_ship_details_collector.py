# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin
from pathlib import Path
from queue import Queue, Empty
import hashlib
import json
import threading
import time

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from chromedriver_factory import ChromeDriverFactory


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def norm_text(t: Optional[str]) -> str:
    if not t:
        return ""
    t = t.replace("\r", "\n").replace("\xa0", " ")
    # нормализуем переносы, сохраняя пустые строки как разделители блоков
    lines = [ln.strip() for ln in t.splitlines()]
    compact = "\n".join([ln for ln in lines if ln != ""])
    return compact.strip()


class ShipDetailsParser:
    """
    Универсальный парсер инфо-таблиц на странице ship.aspx?...:
    - Ищет все <table id="content_tb_*">;
    - С каждой таблицы берёт строки вида: <tr><td>ключ</td><td>значение</td></tr>
    - Сохраняет текст значения, raw HTML и ссылки.
    """
    def __init__(self, driver: WebDriver, wait_sec: int = 30):
        self.driver = driver
        self.wait_sec = wait_sec

    def _open(self, url: str) -> None:
        self.driver.get(url)
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    def _all_info_tables(self) -> List:
        # Берём все таблицы, id которых начинается на content_tb_
        return self.driver.find_elements(By.CSS_SELECTOR, 'table[id^="content_tb_"]')

    def _extract_links(self, element) -> List[Dict[str, str]]:
        links = []
        for a in element.find_elements(By.CSS_SELECTOR, "a[href]"):
            href = a.get_attribute("href") or ""
            text = norm_text(a.text)
            if href:
                links.append({"text": text, "href": href})
        return links

    def _parse_two_col_table(self, table_el, page_url: str) -> Dict:
        """
        Возвращает структуру по одной таблице:
        {
          "table_id": "...",
          "rows": [
            {"key": "...", "value_text": "...", "value_html": "...", "links": [...]},
            ...
          ]
        }
        """
        table_id = table_el.get_attribute("id") or ""
        tbody = table_el.find_element(By.TAG_NAME, "tbody")
        trs = tbody.find_elements(By.CSS_SELECTOR, "tr")

        rows_out = []
        # часто первая(ые) строки — заголовки/пустые ячейки; фильтруем по парам TD
        for tr in trs:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) != 2:
                continue

            key_cell, val_cell = tds[0], tds[1]
            key_text = norm_text(key_cell.text)
            if not key_text:
                # иногда ключ зашит в id ячейки
                key_text = key_cell.get_attribute("id") or ""

            # текст и HTML значения
            value_text = norm_text(val_cell.text)
            value_html = (val_cell.get_attribute("innerHTML") or "").strip()

            # абсолютные ссылки внутри значения
            # (превращать относительные в абсолютные не нужно — selenium даёт абсолютные, но на всякий случай)
            links = []
            for a in val_cell.find_elements(By.CSS_SELECTOR, "a[href]"):
                raw = a.get_attribute("href") or ""
                href = urljoin(page_url, raw) if raw else ""
                links.append({"text": norm_text(a.text), "href": href})

            rows_out.append({
                "key": key_text,
                "value_text": value_text,
                "value_html": value_html,
                "links": links
            })

        return {"table_id": table_id, "rows": rows_out}

    def parse_ship_details(self, page_url: str) -> Dict:
        """
        Возвращает объект:
        {
          "url": "...",
          "ts": 1727...,
          "tables": [
            {"table_id": "content_tb_ship", "rows": [...]},
            {"table_id": "content_tb_builder", "rows": [...]},
            ...
          ]
        }
        """
        self._open(page_url)
        time.sleep(0.2)  # сайт медленный

        tables = self._all_info_tables()
        out_tables = []
        for tb in tables:
            try:
                out_tables.append(self._parse_two_col_table(tb, page_url))
            except Exception:
                # даже если одна таблица не распарсилась — продолжаем
                continue

        return {
            "url": page_url,
            "ts": int(time.time()),
            "tables": out_tables
        }


@dataclass
class ShipDetailsCollectorManager:
    input_json: Path
    input_txt: Path
    out_dir: Path
    workers: int = 4
    wait_sec: int = 30
    use_profile_clone: bool = True
    # --- НОВОЕ: пауза для логина ---
    login_wait_sec: int = 0
    login_url_fallback: str = "http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN"

    def __post_init__(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._q: "Queue[str]" = Queue()
        self._print_lock = threading.Lock()

    # ... (все существующие методы без изменений)

    # --- НОВОЕ: последовательный проход в одном драйвере ---
    def _run_single_driver_sequential(self, urls: list[str]) -> tuple[int, int]:
        """Однопоточный режим: используем один драйвер, чтобы логин сохранился."""
        driver = None
        processed = 0
        try:
            driver = self._driver_factory()
            parser = ShipDetailsParser(driver=driver, wait_sec=self.wait_sec)

            # 1) Открываем страницу для логина
            first_url = urls[0] if urls else self.login_url_fallback
            try:
                print(f"[LOGIN] Открываю страницу для логина: {first_url}")
                driver.get(first_url)
            except Exception:
                # если первая ссылка битая — откроем fallback
                print(f"[LOGIN] Не удалось открыть {first_url}, открою fallback")
                driver.get(self.login_url_fallback)

            # 2) Отсчёт и ожидание
            wait = max(1, int(self.login_wait_sec))
            for sec in range(wait, 0, -1):
                print(f"[LOGIN] Время на логин: {sec} сек", end="\r")
                time.sleep(1)
            print("\n[LOGIN] Пауза закончилась, начинаю парсинг...")

            # 3) Проход по всем URL в этом же драйвере
            for url in urls:
                node_path = self._node_path(url)
                if node_path.exists():
                    continue
                try:
                    print(f"[SEQ] open: {url}")
                    data = parser.parse_ship_details(url)
                    with open(node_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print(f"[SEQ] saved -> {node_path.name} ({len(data.get('tables', []))} tables)")
                    processed += 1
                except Exception as e:
                    with open(self._error_path(url), "w", encoding="utf-8") as f:
                        json.dump({"url": url, "error": repr(e), "ts": int(time.time())},
                                  f, ensure_ascii=False, indent=2)
                    print(f"[SEQ] ERROR on {url}: {e}")
            print("[SEQ] Готово.")
            return len(urls), processed
        finally:
            if driver:
                try:
                    # driver.quit()
                    pass
                except Exception:
                    pass

    
    def __post_init__(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._q: "Queue[str]" = Queue()
        self._print_lock = threading.Lock()

    # -------- загрузка фронтира --------
    def _load_urls(self) -> List[str]:
        urls: List[str] = []

        def _add(u):
            if not u:
                return
            u = str(u).strip()
            if u:
                urls.append(u)

        # 1) Пытаемся прочитать JSON-файл (array / object / NDJSON)
        if self.input_json.exists():
            raw = self.input_json.read_text(encoding="utf-8").strip()
            if raw:
                try:
                    data = json.loads(raw)
                    # вариант: ["http://...", ...]
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, str):
                                _add(item)
                            elif isinstance(item, dict):
                                _add(item.get("url") or item.get("link"))
                    # вариант: {"url": "..."} — одиночный объект
                    elif isinstance(data, dict):
                        _add(data.get("url") or data.get("link"))
                except json.JSONDecodeError:
                    # NDJSON: по одному JSON-на-строке
                    for line in raw.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            if isinstance(obj, str):
                                _add(obj)
                            elif isinstance(obj, dict):
                                _add(obj.get("url") or obj.get("link"))
                        except Exception:
                            # возможно это просто сырой URL в строке
                            if line.startswith("http"):
                                _add(line)

        # 2) Если из JSON ничего не получилось — пробуем TXT
        if not urls and self.input_txt.exists():
            with open(self.input_txt, "r", encoding="utf-8") as f:
                for ln in f:
                    ln = ln.strip()
                    if ln:
                        _add(ln)

        # 3) Уникализируем с сохранением порядка
        seen = set()
        uniq = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                uniq.append(u)
        return uniq


    def _node_path(self, url: str) -> Path:
        return self.out_dir / f"ship_{md5_hex(url)}.json"

    def _error_path(self, url: str) -> Path:
        return self.out_dir / f"ship_{md5_hex(url)}.error.json"

    def _enqueue_pending(self, urls: List[str]) -> int:
        cnt = 0
        for u in urls:
            if self._node_path(u).exists():
                continue
            self._q.put(u)
            cnt += 1
        return cnt

    def _driver_factory(self) -> WebDriver:
        factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
        factory.use_profile_clone = self.use_profile_clone
        return factory.create()

    # -------- воркер --------
    def _worker(self, wid: int):
        driver = None
        try:
            driver = self._driver_factory()
            parser = ShipDetailsParser(driver=driver, wait_sec=self.wait_sec)

            while True:
                try:
                    url = self._q.get(timeout=1.5)
                except Empty:
                    break

                try:
                    with self._print_lock:
                        print(f"[W{wid}] open: {url}")

                    data = parser.parse_ship_details(url)

                    with open(self._node_path(url), "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)

                    with self._print_lock:
                        print(f"[W{wid}] saved -> {self._node_path(url).name} "
                              f"({len(data.get('tables', []))} tables)")
                except Exception as e:
                    with open(self._error_path(url), "w", encoding="utf-8") as f:
                        json.dump({"url": url, "error": repr(e), "ts": int(time.time())},
                                  f, ensure_ascii=False, indent=2)
                    with self._print_lock:
                        print(f"[W{wid}] ERROR on {url}: {e}")
                finally:
                    self._q.task_done()

        finally:
            if driver:
                try:
                    # driver.quit()
                    pass
                except Exception:
                    pass

    # -------- запуск --------
    # -------- запуск --------
    def run(self) -> Tuple[int, int]:
        urls = self._load_urls()
        # убираем уже обработанные
        urls_pending = [u for u in urls if not self._node_path(u).exists()]

        print(f"Всего ссылок в списке: {len(urls)}; к обработке: {len(urls_pending)}; потоков: {self.workers}")

        if not urls_pending:
            print("Нечего делать — все URL уже обработаны (или есть *.error.json для ретрая).")
            return len(urls), 0

        # если задан login_wait_sec — работаем в ОДНОМ драйвере с паузой на логин,
        # чтобы сохранить авторизацию в той же сессии
        if self.login_wait_sec and self.login_wait_sec > 0:
            print(f"[LOGIN] Включён режим ожидания логина ({self.login_wait_sec} сек). "
                  f"Будет использован ОДИН драйвер, независимо от --workers.")
            return self._run_single_driver_sequential(urls_pending)

        # обычный многопоточный режим
        todo = self._enqueue_pending(urls_pending)
        threads = []
        for i in range(self.workers):
            t = threading.Thread(target=self._worker, args=(i + 1,), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        print("Готово.")
        return len(urls), todo
