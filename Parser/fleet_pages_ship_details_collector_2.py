# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import json
import time
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# используем те же кирпичики, что и у вас
from YardParser.rotating_guarded_ship_details_collector_6 import (  # noqa
    ChromeDriverFactory,
    ShipDetailsParser,
    AccountsPool,
    md5_hex,
)

# ---------- утилиты чтения fleet_page_*.json ----------

def _read_json(p: Path) -> Dict:
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}

def _iter_fleet_rows_from_dir(pages_dir: Path) -> List[Dict]:
    rows: List[Dict] = []
    for pf in sorted(pages_dir.glob("fleet_page_*.json")):
        obj = _read_json(pf)
        for r in (obj.get("rows") or []):
            if isinstance(r, dict) and r.get("link"):
                r["_source_page_file"] = str(pf)
                rows.append(r)
    return rows
def _load_accounts_any(accounts_file: Path) -> List[Dict]:
    """
    Загружает аккаунты из папки/файла:
      - папка Registrator/: ищем shipbuilding_accounts.json/jsonl/ndjson/txt
      - файл: json/jsonl/ndjson/txt
    Возвращает список dict с полями email/password (+прочие, если есть).
    """
    def parse_any(path: Path) -> list[dict]:
        out: list[dict] = []
        if not path.exists():
            return out
        suf = path.suffix.lower()

        def _append_obj(o: dict):
            if isinstance(o, dict):
                out.append(o)

        if suf == ".json":
            raw = path.read_text(encoding="utf-8").strip()
            if raw:
                try:
                    data = json.loads(raw)
                    if isinstance(data, list):
                        for o in data:
                            _append_obj(o)
                    elif isinstance(data, dict):
                        _append_obj(data)
                except Exception:
                    pass
            return out

        if suf in {".jsonl", ".ndjson"}:
            for line in path.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s:
                    continue
                try:
                    o = json.loads(s)
                    _append_obj(o)
                except Exception:
                    continue
            return out

        if suf == ".txt":
            for line in path.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                if s.startswith("{") and s.endswith("}"):
                    try:
                        o = json.loads(s); _append_obj(o); continue
                    except Exception:
                        pass
                parts = re.split(r"[,\|\:;\t ]+", s)
                if len(parts) >= 2 and "@" in parts[0]:
                    rec = {"email": parts[0].strip(), "password": parts[1].strip()}
                    if len(parts) >= 3 and parts[2]: rec["full_name"] = parts[2].strip()
                    if len(parts) >= 4 and parts[3]: rec["company"]   = parts[3].strip()
                    if len(parts) >= 5 and parts[4]: rec["role"]      = parts[4].strip()
                    out.append(rec)
            return out

        # fallback: попробуем как json или ndjson
        try:
            raw = path.read_text(encoding="utf-8").strip()
            if raw:
                try:
                    data = json.loads(raw)
                    if isinstance(data, list):
                        out.extend([o for o in data if isinstance(o, dict)])
                    elif isinstance(data, dict):
                        out.append(data)
                except json.JSONDecodeError:
                    for line in raw.splitlines():
                        s = line.strip()
                        if not s:
                            continue
                        try:
                            o = json.loads(s)
                            if isinstance(o, dict):
                                out.append(o)
                        except Exception:
                            continue
        except Exception:
            pass
        return out

    accs: list[dict] = []
    src = accounts_file
    if src.is_dir():
        for p in [
            src / "shipbuilding_accounts.json",
            src / "shipbuilding_accounts.jsonl",
            src / "shipbuilding_accounts.ndjson",
            src / "shipbuilding_accounts.txt",
        ]:
            accs.extend(parse_any(p))
    else:
        accs.extend(parse_any(src))

    # фильтрация и дедуп по email (с max timestamp если есть)
    accs = [a for a in accs if a.get("email") and a.get("password")]
    by_email: dict[str, dict] = {}
    for a in accs:
        email = str(a.get("email", "")).strip().lower()
        if not email:
            continue
        old = by_email.get(email)
        if old is None or (a.get("timestamp", 0) >= old.get("timestamp", 0)):
            by_email[email] = a
    return list(by_email.values())
def _seed_worker_cursors(accounts_file: Path, cursor_base: Path, workers: int) -> int:
    accounts = _load_accounts_any(accounts_file)
    n = len(accounts)
    if n == 0:
        raise RuntimeError(f"[ACCOUNTS] Не найдено аккаунтов в {accounts_file}")
    if workers > n:
        print(f"[ACCOUNTS][WARN] потоков {workers} больше, чем аккаунтов {n} — некоторые потоки будут делить учётки.")

    # читаем базовый индекс из cursor_base (например, {"index": 68})
    base_index = 0
    try:
        if cursor_base.exists():
            raw = cursor_base.read_text(encoding="utf-8").strip()
            if raw:
                obj = json.loads(raw)
                if isinstance(obj, dict) and isinstance(obj.get("index"), int):
                    base_index = int(obj["index"]) % n
    except Exception:
        pass

    cursor_base.parent.mkdir(parents=True, exist_ok=True)
    for wid in range(max(1, workers)):
        idx = (base_index + wid) % n
        cursor_path = cursor_base.with_name(cursor_base.stem + f".w{wid}").with_suffix(".json")

        if cursor_path.exists():
            # НЕ перезаписываем — у воркера уже есть прогресс
            print(f"[ACCOUNTS] keep existing cursor for W{wid}: {cursor_path}")
            continue

        try:
            cursor_path.write_text(
                json.dumps({"index": idx}, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            print(f"[ACCOUNTS] seed cursor for W{wid}: index={idx} → {cursor_path}")
        except Exception as e:
            print(f"[ACCOUNTS][WARN] не смог записать курсор {cursor_path}: {e}")

    print(f"[ACCOUNTS] подготовлено курсоров: {workers}, аккаунтов: {n}, base_index={base_index}")
    return n

# ---------- класс-раннер ----------

@dataclass
class FleetPagesShipDetailsParallelRunner:
    pages_dir: Path
    out_dir: Path
    wait_sec: int = 30
    workers: int = 4
    use_profile_clone: bool = True

    # логин/лимиты
    login_url_fallback: str = "http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN"
    login_page_url: str = "http://chinashipbuilding.cn/en/signin.aspx"
    min_tables_required: int = 7
    batch_logout_every: int = 50
    rotate_on_low_tables: bool = False  # НЕ ротировать на недоборе таблиц (<7)

    # аккаунты
    accounts_file: Path = Path("Registrator")  # можно файл или папку
    account_cursor_base: Path = Path("Registrator/account_cursor.json")
    first_login_wait_sec: int = 60
    relogin_wait_sec: int = 30
    relogin_manual: bool = False

    # ограничение набора (для тестов)
    max_items_per_run: Optional[int] = None

    def __post_init__(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._print_lock = threading.Lock()

    # ---- пути для нод ----
    def _node_path(self, url: str) -> Path:
        return self.out_dir / f"ship_{md5_hex(url)}.json"

    def _error_path(self, url: str) -> Path:
        return self.out_dir / f"ship_{md5_hex(url)}.error.json"

    # ---- сбор задач ----
    def _load_tasks(self) -> List[Dict]:
        rows = _iter_fleet_rows_from_dir(self.pages_dir)
        tasks: List[Dict] = []
        for r in rows:
            url = str(r.get("link") or "").strip()
            if not url:
                continue
            meta = {
                "no": r.get("no"),
                "name": r.get("name"),
                "ship_type": r.get("ship_type"),
                "owner_company": r.get("owner_company"),
                "shipyard": r.get("shipyard"),
                "date_built": r.get("date_built"),
                "_source_page_file": r.get("_source_page_file"),
            }
            tasks.append({"url": url, "meta": meta})
        # уникализируем по url (сохраняем первый meta)
        seen = set()
        uniq: List[Dict] = []
        for t in tasks:
            u = t["url"]
            if u not in seen:
                seen.add(u)
                uniq.append(t)
        return uniq

    # ---- логин-виджет ----
    def _get_login_widget_info(self, driver: WebDriver):
        try:
            el = driver.find_element(By.ID, "content_hrd_web_btn_logon")
            src = (el.get_attribute("src") or "").lower()
            if "images/logout.jpg" in src:
                return el, "logout"
            if "images/logon.jpg" in src:
                return el, "login"
            return el, "unknown"
        except Exception:
            pass
        try:
            el = driver.find_element(By.ID, "content_hrd_header_btn_logon")
            src = (el.get_attribute("src") or "").lower()
            if "images/logout.jpg" in src:
                return el, "logout"
            if "images/logon.jpg" in src:
                return el, "login"
            return el, "unknown"
        except Exception:
            pass
        return None, "unknown"

    def _is_logged_in(self, driver: WebDriver) -> bool:
        el, state = self._get_login_widget_info(driver)
        return state == "logout"

    def _go_to_login_via_header(self, driver: WebDriver, open_start_url: bool = True, wait_form_timeout: int = 12) -> bool:
        try:
            if open_start_url:
                driver.get(self.login_url_fallback)
                WebDriverWait(driver, self.wait_sec).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            if self._is_logged_in(driver):
                return True
            btn, state = self._get_login_widget_info(driver)
            if btn and state == "login":
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                time.sleep(0.15)
                try:
                    btn.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", btn)
            WebDriverWait(driver, wait_form_timeout).until(
                EC.presence_of_element_located((By.ID, "content_ctl_signin_txt_userid"))
            )
            return True
        except Exception:
            return False

    def _logout_safely(self, driver: WebDriver):
        try:
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception:
            pass
        try:
            el, state = self._get_login_widget_info(driver)
            if el and state == "logout":
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.1)
                try:
                    el.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", el)
                print("[AUTH] Logout clicked.")
            else:
                print(f"[AUTH] Logout skipped (state={state}).")
        except Exception as e:
            print(f"[AUTH] Logout failed: {e}")

    def _login_automatically(self, driver: WebDriver, email: str, password: str) -> bool:
        from selenium.common.exceptions import TimeoutException
        target_suffix = "shipbuilds.aspx?nmkhTk8Pl4EN".lower()
        try:
            driver.get(self.login_url_fallback)
            WebDriverWait(driver, self.wait_sec).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            if self._is_logged_in(driver):
                print(f"[AUTH] Already logged in → {driver.current_url}")
                return True
            ok = self._go_to_login_via_header(driver, open_start_url=False)
            if not ok:
                driver.get(self.login_page_url)
                WebDriverWait(driver, self.wait_sec).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            user_el = WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((By.ID, "content_ctl_signin_txt_userid"))
            )
            pass_el = driver.find_element(By.ID, "content_ctl_signin_txt_password")
            login_btn = driver.find_element(By.ID, "content_ctl_signin_btn_logon")
            user_el.clear(); user_el.send_keys(email)
            pass_el.clear(); pass_el.send_keys(password)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", login_btn)
            time.sleep(0.1)
            try:
                login_btn.click()
            except Exception:
                driver.execute_script("arguments[0].click();", login_btn)

            ok = False
            try:
                WebDriverWait(driver, 12).until(lambda d: self._is_logged_in(d))
                ok = True
            except TimeoutException:
                try:
                    WebDriverWait(driver, 8).until(EC.url_contains(target_suffix))
                    ok = self._is_logged_in(driver) or True
                except TimeoutException:
                    ok = self._is_logged_in(driver)

            if ok and target_suffix not in (driver.current_url or "").lower():
                driver.get(self.login_url_fallback)
                ok = self._is_logged_in(driver)
            if ok:
                print(f"[AUTH] Logged in as: {email} → {driver.current_url}")
                return True
            print(f"[AUTH] Login seems failed (no logout-image), URL: {driver.current_url}")
            return False
        except Exception as e:
            print(f"[AUTH] Auto-login failed for {email}: {e}")
            return False

    # ---- парс одной ссылки с ретраями ----
    def _parse_with_retry(self, parser: ShipDetailsParser, url: str, min_tables: int, retries: int = 2, delay: float = 1.5) -> Dict:
        data = parser.parse_ship_details(url)
        cnt = len(data.get("tables", []))
        att = 0
        while cnt < min_tables and att < retries:
            att += 1
            print(f"[RETRY] tables={cnt} < {min_tables}. Попытка {att}/{retries}...")
            try:
                time.sleep(delay)
                parser.open(url)
                data = parser.parse_ship_details(url)
                cnt = len(data.get("tables", []))
            except Exception:
                break
        return data

    # ---- worker ----
    def _worker(self, wid: int, jobs: List[Dict]) -> Tuple[int, int]:
        """Обрабатывает пачку jobs в одном потоке."""
        saved = 0
        since_login = 0
        driver = None

        # У каждой нити — свой AccountsPool с «своим» курсором, чтобы не бодаться за файл.
        cursor_file = self.account_cursor_base.with_name(self.account_cursor_base.stem + f".w{wid}.json").with_suffix(".json")
        acc_pool = AccountsPool(self.accounts_file, cursor_file)
        try:
            # драйвер
            factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
            factory.use_profile_clone = self.use_profile_clone
            driver = factory.create()

            parser = ShipDetailsParser(driver=driver, wait_sec=self.wait_sec)

            # логин
            cur = acc_pool.current()
            print(acc_pool.debug_state())
            if self.relogin_manual:
                # ручной логин — дадим время
                driver.get(self.login_url_fallback)
                for sec in range(self.first_login_wait_sec, 0, -1):
                    print(f"[AUTH][W{wid}] Время на ручной логин: {sec} сек", end="\r")
                    time.sleep(1)
                print()
            else:
                ok = self._login_automatically(driver, cur["email"], cur["password"])
                if not ok:
                    cur = acc_pool.next()
                    print(acc_pool.debug_state())
                    self._login_automatically(driver, cur["email"], cur["password"])

            # цикл задач
            for t in jobs:
                url = t["url"]
                meta = t["meta"]
                node_path = self._node_path(url)
                if node_path.exists():
                    continue

                try:
                    data = self._parse_with_retry(parser, url, min_tables=self.min_tables_required, retries=2, delay=1.5)
                    cnt = len(data.get("tables", []))
                   
                    # лимит 50/день?
                    if parser.has_daily_limit_banner():
                        print(f"[W{wid}][GUARD] Лимит 50/день — смена учётки.")
                        self._logout_safely(driver)
                        if self.relogin_manual:
                            driver.get(self.login_url_fallback)
                            for sec in range(self.relogin_wait_sec, 0, -1):
                                print(f"[AUTH][W{wid}] Ручной логин (после лимита): {sec} сек", end="\r")
                                time.sleep(1)
                            print()
                        else:
                            cur = acc_pool.next()
                            print(acc_pool.debug_state())
                            self._login_automatically(driver, cur["email"], cur["password"])
                        since_login = 0
                        # ещё раз попробуем текущую ссылку
                        data = self._parse_with_retry(parser, url, min_tables=self.min_tables_required, retries=2, delay=1.5)

             

                    cnt = len(data.get("tables", []))
                    print(f"[W{wid}][FLEET] saved -> {node_path.name} ({cnt} tables)")
                    saved += 1
                    since_login += 1

                    # при <7 таблиц — НЕ ротируем (если не включено явно)
                    # if cnt < self.min_tables_required and self.rotate_on_low_tables:
                    #     print(f"[W{wid}][GUARD] Tables {cnt} < {self.min_tables_required}: ротация включена, переключаем учётку.")
                    #     self._logout_safely(driver)
                    #     if self.relogin_manual:
                    #         driver.get(self.login_url_fallback)
                    #         for sec in range(self.relogin_wait_sec, 0, -1):
                    #             print(f"[AUTH][W{wid}] Ручной логин (после low-tables): {sec} сек", end="\r")
                    #             time.sleep(1)
                    #         print()
                    #     else:
                    #         cur = acc_pool.next()
                    #         print(acc_pool.debug_state())
                    #         self._login_automatically(driver, cur["email"], cur["password"])
                    #     since_login = 0
                    if cnt < 6:
                        print(f"[W{wid}][SKIP] {url} → только {cnt} таблиц (< 6) — пропуск без сохранения.")
                        # по желанию — лог в отдельный файл:
                        with open(self._error_path(url).with_suffix(".skipped.json"), "w", encoding="utf-8") as f:
                            json.dump({"url": url, "reason": f"tables={cnt} < 6", "ts": int(time.time()), "from_fleet": meta}, f, ensure_ascii=False, indent=2)
                        continue
                    # батч-логаут
                    if self.batch_logout_every and since_login >= self.batch_logout_every:
                        print(f"[W{wid}][BATCH] достигнут батч {self.batch_logout_every} — ротация.")
                        self._logout_safely(driver)
                        if self.relogin_manual:
                            driver.get(self.login_url_fallback)
                            for sec in range(self.relogin_wait_sec, 0, -1):
                                print(f"[AUTH][W{wid}] Ручной логин (батч): {sec} сек", end="\r")
                                time.sleep(1)
                            print()
                        else:
                            cur = acc_pool.next()
                            print(acc_pool.debug_state())
                            self._login_automatically(driver, cur["email"], cur["password"])
                        since_login = 0
                       # приклеить мета и сохранить
                    data["from_fleet"] = meta
                    with open(node_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print(f"[W{wid}][FLEET] saved -> {node_path.name} ({cnt} tables)")
                    saved += 1
                    since_login += 1
                except Exception as e:
                    with open(self._error_path(url), "w", encoding="utf-8") as f:
                        json.dump({"url": url, "error": repr(e), "ts": int(time.time()), "from_fleet": meta},
                                  f, ensure_ascii=False, indent=2)
                    print(f"[W{wid}][FLEET] ERROR on {url}: {e}")

        finally:
            try:
                if driver:
                    # driver.quit()  # по желанию
                    pass
            except Exception:
                pass

        return (len(jobs), saved)

    # ---- run ----
    def run(self) -> Tuple[int, int]:
        tasks = self._load_tasks()
        
        # ограничим набор
        if self.max_items_per_run and self.max_items_per_run > 0:
            tasks = tasks[: self.max_items_per_run]

        # уже сохранённые — выкидываем
        pending = [t for t in tasks if not self._node_path(t["url"]).exists()]

        print(f"[PAR] pages_dir = {self.pages_dir.resolve()}")
        print(f"[PAR] Всего по файлам флота: {len(tasks)}; к обработке: {len(pending)}; потоков: {self.workers}")
        if not pending:
            print("[PAR] Нечего делать — всё уже сохранено.")
            return (len(tasks), 0)
        _seed_worker_cursors(self.accounts_file, self.account_cursor_base, self.workers)
        # разбивка на чанки
        W = max(1, int(self.workers))
        chunks: List[List[Dict]] = [[] for _ in range(W)]
        for i, t in enumerate(pending):
            chunks[i % W].append(t)

        totals = 0
        saved = 0
        with ThreadPoolExecutor(max_workers=W) as ex:
            futs = {ex.submit(self._worker, wid, chunk): wid for wid, chunk in enumerate(chunks)}
            for fut in as_completed(futs):
                wid = futs[fut]
                try:
                    total_w, saved_w = fut.result()
                    totals += total_w
                    saved += saved_w
                    with self._print_lock:
                        print(f"[PAR][W{wid}] done: total={total_w}, saved={saved_w}")
                except Exception as e:
                    with self._print_lock:
                        print(f"[PAR][W{wid}] raised: {e}")

        print(f"[PAR] Готово. Итого: assigned={totals}, saved={saved}")
        return (totals, saved)
