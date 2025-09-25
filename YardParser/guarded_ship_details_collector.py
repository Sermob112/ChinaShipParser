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
    import hashlib as _h
    return _h.md5(s.encode("utf-8")).hexdigest()


def norm_text(t: Optional[str]) -> str:
    if not t:
        return ""
    t = t.replace("\r", "\n").replace("\xa0", " ")
    lines = [ln.strip() for ln in t.splitlines()]
    compact = "\n".join([ln for ln in lines if ln != ""])
    return compact.strip()


class ShipDetailsParser:
    def __init__(self, driver: WebDriver, wait_sec: int = 30):
        self.driver = driver
        self.wait_sec = wait_sec

    def _open(self, url: str) -> None:
        self.driver.get(url)
        WebDriverWait(self.driver, self.wait_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    def _all_info_tables(self) -> List:
        return self.driver.find_elements(By.CSS_SELECTOR, 'table[id^="content_tb_"]')

    def _parse_two_col_table(self, table_el, page_url: str) -> Dict:
        table_id = table_el.get_attribute("id") or ""
        tbody = table_el.find_element(By.TAG_NAME, "tbody")
        trs = tbody.find_elements(By.CSS_SELECTOR, "tr")

        rows_out = []
        for tr in trs:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) != 2:
                continue
            key_cell, val_cell = tds[0], tds[1]
            key_text = norm_text(key_cell.text) or (key_cell.get_attribute("id") or "")
            value_text = norm_text(val_cell.text)
            value_html = (val_cell.get_attribute("innerHTML") or "").strip()

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
        self._open(page_url)
        time.sleep(0.2)  # сайт медленный

        tables = []
        for tb in self._all_info_tables():
            try:
                tables.append(self._parse_two_col_table(tb, page_url))
            except Exception:
                continue

        return {"url": page_url, "ts": int(time.time()), "tables": tables}


@dataclass
# --- в dataclass ShipDetailsCollectorManager добавь поля ---
@dataclass
class ShipDetailsCollectorManager:
    input_json: Path
    input_txt: Path
    out_dir: Path
    workers: int = 4
    wait_sec: int = 30
    use_profile_clone: bool = True
    login_wait_sec: int = 0
    login_url_fallback: str = "http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN"
    max_items_per_run: int | None = None
    min_tables_required: int = 7
    batch_logout_every: int | None = None     # <-- НОВОЕ: сколько страниц обрабатывать за одну сессию
    relogin_wait_sec: int = 30                # <-- НОВОЕ: пауза на ручной повторный вход


    def __post_init__(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._q: "Queue[str]" = Queue()
        self._print_lock = threading.Lock()
        self._stop_event = threading.Event()  # общий стоп-флаг

    # ---------- helpers ----------
    def _logout_safely(self, driver: WebDriver):
        """
        Кликает по кнопке Logout:
        <input id="content_hrd_header_btn_logon" src="images/logout.jpg" ...>
        """
        try:
            el = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable((By.ID, "content_hrd_header_btn_logon"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.2)
            el.click()
            with self._print_lock:
                print("[GUARD] Logout clicked.")
        except Exception as e:
            with self._print_lock:
                print(f"[GUARD] Logout failed: {e}")

    def _node_path(self, url: str) -> Path:
        return self.out_dir / f"ship_{md5_hex(url)}.json"

    def _error_path(self, url: str) -> Path:
        return self.out_dir / f"ship_{md5_hex(url)}.error.json"

    def _driver_factory(self) -> WebDriver:
        factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
        factory.use_profile_clone = self.use_profile_clone
        return factory.create()

    # ---------- загрузка ссылок ----------
    def _load_urls(self) -> List[str]:
        urls: List[str] = []

        def _add(u):
            if not u:
                return
            u = str(u).strip()
            if u:
                urls.append(u)

        # JSON / array / dict / NDJSON
        if self.input_json.exists():
            raw = self.input_json.read_text(encoding="utf-8").strip()
            if raw:
                try:
                    data = json.loads(raw)
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, str):
                                _add(item)
                            elif isinstance(item, dict):
                                _add(item.get("url") or item.get("link"))
                    elif isinstance(data, dict):
                        _add(data.get("url") or data.get("link"))
                except json.JSONDecodeError:
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
                            if line.startswith("http"):
                                _add(line)

        if not urls and self.input_txt.exists():
            for ln in self.input_txt.read_text(encoding="utf-8").splitlines():
                ln = ln.strip()
                if ln:
                    _add(ln)

        # unique keep order
        seen = set()
        uniq = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                uniq.append(u)
        return uniq

    # ---------- однопоточный режим (для login-wait) ----------
    def _run_single_driver_sequential(self, urls: list[str]) -> tuple[int, int]:
        """
        Однопоточный режим с опциональным:
        - login_wait_sec: первичное ожидание логина
        - batch_logout_every + relogin_wait_sec: циклический Logout/паузa/продолжение
        """
        if self.max_items_per_run is not None:
            urls = urls[: self.max_items_per_run]

        driver = None
        processed_total = 0
        processed_since_login = 0

        def wait_login(seconds: int):
            seconds = max(1, int(seconds))
            for sec in range(seconds, 0, -1):
                print(f"[LOGIN] Время на логин: {sec} сек", end="\r")
                time.sleep(1)
            print()

        try:
            driver = self._driver_factory()
            parser = ShipDetailsParser(driver=driver, wait_sec=self.wait_sec)

            # --- первичный вход ---
            first_url = urls[0] if urls else self.login_url_fallback
            try:
                print(f"[LOGIN] Открываю страницу для логина: {first_url}")
                driver.get(first_url)
            except Exception:
                print(f"[LOGIN] Не удалось открыть {first_url}, открою fallback")
                driver.get(self.login_url_fallback)

            if self.login_wait_sec and self.login_wait_sec > 0:
                wait_login(self.login_wait_sec)
            print("[LOGIN] Начинаю парсинг...")

            for url in urls:
                if self._stop_event.is_set():
                    break
                node_path = self._node_path(url)
                if node_path.exists():
                    continue

                try:
                    print(f"[SEQ] open: {url}")
                    data = parser.parse_ship_details(url)
                    with open(node_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)

                    tables_cnt = len(data.get("tables", []))
                    print(f"[SEQ] saved -> {node_path.name} ({tables_cnt} tables)")
                    processed_total += 1
                    processed_since_login += 1

                    # GUARD: если таблиц недостаточно — Logout и стоп
                    if tables_cnt < self.min_tables_required:
                        print(f"[GUARD] Tables {tables_cnt} < {self.min_tables_required}: выхожу из аккаунта и останавливаюсь.")
                        self._logout_safely(driver)
                        self._stop_event.set()
                        break

                    # --- БАТЧЕВЫЙ ЦИКЛ: достигли лимита за одну сессию? ---
                    if self.batch_logout_every and processed_since_login >= self.batch_logout_every:
                        print(f"[BATCH] Достигнут лимит {self.batch_logout_every} записей за сессию. Logout + ожидание повторного входа.")
                        self._logout_safely(driver)

                        # вернёмся на страницу логина/стартовую
                        try:
                            driver.get(self.login_url_fallback)
                        except Exception:
                            pass

                        # дать время на логин вручную
                        wait_login(self.relogin_wait_sec)

                        # сброс счётчика сессии и продолжаем
                        processed_since_login = 0
                        print("[BATCH] Продолжаю парсинг после повторного входа.")

                except Exception as e:
                    with open(self._error_path(url), "w", encoding="utf-8") as f:
                        json.dump({"url": url, "error": repr(e), "ts": int(time.time())},
                                f, ensure_ascii=False, indent=2)
                    print(f"[SEQ] ERROR on {url}: {e}")

            print("[SEQ] Готово.")
            return len(urls), processed_total

        finally:
            if driver:
                try:
                    # driver.quit()
                    pass
                except Exception:
                    pass


    # ---------- многопоточный режим ----------
    def _enqueue_pending(self, urls: List[str]) -> int:
        cnt = 0
        for u in urls:
            if self._node_path(u).exists():
                continue
            self._q.put(u)
            cnt += 1
        return cnt

    def _worker(self, wid: int):
        driver = None
        try:
            driver = self._driver_factory()
            parser = ShipDetailsParser(driver=driver, wait_sec=self.wait_sec)

            while not self._stop_event.is_set():
                try:
                    url = self._q.get(timeout=1.5)
                except Empty:
                    break
                if self._stop_event.is_set():
                    self._q.task_done()
                    break

                try:
                    with self._print_lock:
                        print(f"[W{wid}] open: {url}")

                    data = parser.parse_ship_details(url)
                    node_path = self._node_path(url)
                    with open(node_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)

                    tables_cnt = len(data.get("tables", []))
                    with self._print_lock:
                        print(f"[W{wid}] saved -> {node_path.name} ({tables_cnt} tables)")

                    if tables_cnt < self.min_tables_required:
                        with self._print_lock:
                            print(f"[GUARD][W{wid}] Tables {tables_cnt} < {self.min_tables_required}: выхожу из аккаунта и останавливаю все потоки.")
                        self._logout_safely(driver)
                        self._stop_event.set()
                        # очищаем очередь, чтобы другие потоки не брали задания
                        try:
                            while True:
                                self._q.get_nowait()
                                self._q.task_done()
                        except Empty:
                            pass
                        break

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

    # ---------- запуск ----------
    def run(self) -> Tuple[int, int]:
        urls = self._load_urls()
        urls_pending = [u for u in urls if not self._node_path(u).exists()]

        if self.max_items_per_run is not None:
            urls_pending = urls_pending[: self.max_items_per_run]

        print(f"Всего ссылок в списке: {len(urls)}; к обработке: {len(urls_pending)}; потоков: {self.workers}")

        if not urls_pending:
            print("Нечего делать — все URL уже обработаны (или есть *.error.json для ретрая).")
            return len(urls), 0

        if self.login_wait_sec and self.login_wait_sec > 0:
            print(f"[LOGIN] Включён режим ожидания логина ({self.login_wait_sec} сек). Один драйвер.")
            return self._run_single_driver_sequential(urls_pending)

        # многопоточно
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
