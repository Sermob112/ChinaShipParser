# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin
from pathlib import Path
from queue import Queue
import hashlib
import json
import threading
import time
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlsplit, urlunsplit
from chromedriver_factory import ChromeDriverFactory



def md5_hex(s: str) -> str:
    import hashlib as _h
    return _h.md5(s.encode("utf-8")).hexdigest()

def canonical_url(u: str) -> str:
    s = (u or "").strip().replace("\\", "/").strip('\'"<> ')
    if not s:
        return ""
    parts = urlsplit(s)
    scheme = (parts.scheme or "").lower()
    netloc = (parts.netloc or "").lower()
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]
    return urlunsplit((scheme, netloc, parts.path, parts.query, ""))

def norm_text(t: Optional[str]) -> str:
    if not t:
        return ""
    t = t.replace("\r", "\n").replace("\xa0", " ")
    lines = [ln.strip() for ln in t.splitlines()]
    return "\n".join([ln for ln in lines if ln != ""]).strip()


# -------------------- Accounts Pool --------------------

class AccountsPool:
    """
    Загружает аккаунты из shipbuilding_accounts.json (поддерживает:
      - JSON-массив объектов [{email,password,...}, ...]
      - NDJSON: по объекту в строке
    Хранит курсор в account_cursor.json, чтобы между перезапусками продолжать со следующего.
    """
    def __init__(self, accounts_file: Path, cursor_file: Path):
        self.accounts_file = accounts_file
        self.cursor_file = cursor_file
        self.accounts: List[Dict] = self._load_accounts()
        self._idx = self._load_cursor()
    def size(self) -> int:
        return len(self.accounts)
    def force_set_index(self, idx: int) -> None:
        """Принудительно установить стартовый индекс и сразу сохранить курсор."""
        if not self.accounts:
            raise ValueError("Нет аккаунтов")
        self._idx = int(idx) % len(self.accounts)   # <-- было self.index
        try:
            self._save_cursor()                     # запишем {"index": <...>}
        except Exception:
            pass
    def _load_accounts(self) -> List[Dict]:
        """
        Загружает аккаунты из:
        - self.accounts_file, если это файл (json/jsonl/ndjson/txt),
        - или, если это ПАПКА, ищет внутри:
            shipbuilding_accounts.json
            shipbuilding_accounts.jsonl / .ndjson
            shipbuilding_accounts.txt
        Поддерживает NDJSON, обычный JSON-массив/объект, и txt-строки "email,password" / "email|password" / "email:password".
        Дедуп по email (берём запись с большим timestamp, если есть).
        """
        import json, re
        from pathlib import Path

        def parse_any(path: Path) -> list[dict]:
            out: list[dict] = []
            if not path.exists():
                return out
            suf = path.suffix.lower()

            def _append_obj(o: dict):
                if isinstance(o, dict):
                    out.append(o)

            if suf in {".json"}:
                try:
                    raw = path.read_text(encoding="utf-8").strip()
                    if not raw:
                        return out
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
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        o = json.loads(line)
                        _append_obj(o)
                    except Exception:
                        # допустим в txt попался, проигнорим
                        continue
                return out

            if suf in {".txt"}:
                # форматы строк: "email,password" | "email|password" | "email:password"
                for line in path.read_text(encoding="utf-8").splitlines():
                    s = line.strip()
                    if not s or s.startswith("#"):
                        continue
                    # если это случайно json-строка — попробуем как jsonl
                    if s.startswith("{") and s.endswith("}"):
                        try:
                            o = json.loads(s)
                            _append_obj(o)
                            continue
                        except Exception:
                            pass
                    # иначе разделители
                    parts = re.split(r"[,\|\:;\t ]+", s)
                    if len(parts) >= 2 and "@" in parts[0]:
                        email = parts[0].strip()
                        password = parts[1].strip()
                        rec = {"email": email, "password": password}
                        # необязательные поля, если дописаны: full_name, company, role
                        if len(parts) >= 3 and parts[2]:
                            rec["full_name"] = parts[2].strip()
                        if len(parts) >= 4 and parts[3]:
                            rec["company"] = parts[3].strip()
                        if len(parts) >= 5 and parts[4]:
                            rec["role"] = parts[4].strip()
                        out.append(rec)
                return out

            # неизвестное расширение — попробуем как json/ndjson
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
                        # NDJSON fallback
                        for line in raw.splitlines():
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                o = json.loads(line)
                                if isinstance(o, dict):
                                    out.append(o)
                            except Exception:
                                continue
            except Exception:
                pass
            return out

        accs: list[dict] = []

        src = Path(self.accounts_file)  # у вас может быть файл или папка
        if src.is_dir():
            candidates = [
                src / "shipbuilding_accounts.json",
                src / "shipbuilding_accounts.jsonl",
                src / "shipbuilding_accounts.ndjson",
                src / "shipbuilding_accounts.txt",
            ]
            for p in candidates:
                accs.extend(parse_any(p))
        else:
            accs.extend(parse_any(src))

        # фильтруем валидные
        accs = [a for a in accs if (a.get("email") and a.get("password"))]

        if not accs:
            raise ValueError(f"Не найдено ни одного аккаунта (email/password). Проверялось: {src}")

        # дедуп по email, оставляя запись с бОльшим timestamp (если есть)
        by_email: dict[str, dict] = {}
        for a in accs:
            email = str(a.get("email")).strip().lower()
            if not email:
                continue
            if email not in by_email:
                by_email[email] = a
            else:
                old = by_email[email]
                t_new = a.get("timestamp") or 0
                t_old = old.get("timestamp") or 0
                if t_new >= t_old:
                    by_email[email] = a

        arr = list(by_email.values())
        if not arr:
            raise ValueError("После дедупликации не осталось валидных аккаунтов")
        return arr

    def _load_cursor(self) -> int:
        try:
            obj = json.loads(self.cursor_file.read_text(encoding="utf-8"))
            i = int(obj.get("index", 0))
            if 0 <= i < len(self.accounts):
                return i
        except Exception:
            pass
        return 0

    def _save_cursor(self) -> None:
        try:
            self.cursor_file.write_text(json.dumps({"index": self._idx}, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def current(self) -> Dict:
        return self.accounts[self._idx]

    def next(self) -> Dict:
        self._idx = (self._idx + 1) % len(self.accounts)
        self._save_cursor()
        return self.current()

    def debug_state(self) -> str:
        a = self.current()
        return f"[ACCOUNTS] {self._idx+1}/{len(self.accounts)}  {a.get('email')}"


# -------------------- Ship Details Parser --------------------

class ShipDetailsParser:
    """
    Универсальный парсер инфо-таблиц на странице ship.aspx?...:
    - Ищет все <table id="content_tb_*">;
    - Для каждой таблицы берёт пары <td>ключ</td><td>значение</td>
    - Сохраняет value_text, value_html и ссылки.
    """
    def __init__(self, driver: WebDriver, wait_sec: int = 30):
        self.driver = driver
        self.wait_sec = wait_sec

    def open(self, url: str) -> None:
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
        self.open(page_url)
        time.sleep(0.2)  # сайт медленный
        tables = []
        for tb in self._all_info_tables():
            try:
                tables.append(self._parse_two_col_table(tb, page_url))
            except Exception:
                continue
        return {"url": page_url, "ts": int(time.time()), "tables": tables}

    def has_daily_limit_banner(self) -> bool:
        """
        Определяем баннер про лимит 50/день:
        <img id="content_img_vip" ...> + текст с 'limited to 50 records'
        """
        try:
            # любая из проверок достаточно
            if self.driver.find_elements(By.ID, "content_img_vip"):
                return True
            # запасной вариант по тексту
            if self.driver.find_elements(By.XPATH, "//td[contains(., 'limited to 50 records')]"):
                return True
        except Exception:
            pass
        return False


# -------------------- Rotating Guarded Manager --------------------

@dataclass
class ShipDetailsCollectorManager:
    # входные данные
    input_json: Path
    input_txt: Path
    # выход
    out_dir: Path
    # сессия/тайминги
    wait_sec: int = 30
    use_profile_clone: bool = True
    login_url_fallback: str = "http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN"
    # границы/правила
    min_tables_required: int = 7
    batch_logout_every: int = 50         # после скольких сохранённых страниц делать relogin
    # логин/аккаунты
    accounts_file: Path = Path("shipbuilding_accounts.json")
    account_cursor_file: Path = Path("account_cursor.json")
    first_login_wait_sec: int = 60       # первичная пауза на ручной логин (если не хотим авто-логин первой учёткой)
    relogin_wait_sec: int = 30           # пауза на relogin, если выбран ручной режим relogin_manual=True
    relogin_manual: bool = False         # False=логин автоматический по accounts.json; True=даём время на ручной вход
    # предел за один запуск (опционально)
    max_items_per_run: Optional[int] = None

    def __post_init__(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._print_lock = threading.Lock()
        self._origin_by_url: Dict[str, str] = {}
        self._stop_event = threading.Event()
        self._accounts = AccountsPool(self.accounts_file, self.account_cursor_file)

    # ---------- низкоуровневые действия ----------
    def _driver_factory(self) -> WebDriver:
        factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
        factory.use_profile_clone = self.use_profile_clone
        return factory.create()

    def _node_path(self, url: str) -> Path:
        return self.out_dir / f"ship_{md5_hex(url)}.json"

    def _error_path(self, url: str) -> Path:
        return self.out_dir / f"ship_{md5_hex(url)}.error.json"

    def _logout_safely(self, driver):
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


    from selenium.common.exceptions import TimeoutException

    def _get_login_widget_info(self, driver):
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
    def _go_to_login_via_header(self, driver, open_start_url: bool = True, wait_form_timeout: int = 12) -> bool:
        """
        Открываем стартовую страницу (при необходимости), кликаем по
        #content_hrd_web_btn_logon, если она в состоянии 'login' (Images/logon.jpg),
        и ждём появления полей логина.
        Возвращает True если форма видна.
        """
        try:
            if open_start_url:
                driver.get(self.login_url_fallback)  # http://chinashipbuilding.cn/shipbuilds.aspx?nmkhTk8Pl4EN
                WebDriverWait(driver, self.wait_sec).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

            # если уже залогинены — форма не нужна
            if self._is_logged_in(driver):
                return True

            btn, state = self._get_login_widget_info(driver)
            if btn and state == "login":
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                time.sleep(0.1)
                try:
                    btn.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", btn)

            # ждём поля формы логина
            try:
                WebDriverWait(driver, wait_form_timeout).until(
                    EC.presence_of_element_located((By.ID, "content_ctl_signin_txt_userid"))
                )
                return True
            except TimeoutException:
                return False
        except Exception:
            return False

    def _is_logged_in(self, driver) -> bool:
        el, state = self._get_login_widget_info(driver)
        return state == "logout"
    def _logout_safely(self, driver: WebDriver):
        """
        Кликаем logout только если видим кнопку со state='logout'.
        """
        try:
            # подождём чуть, вдруг страница ещё дорендеривается
            WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception:
            pass

        try:
            el, state = self._get_login_widget_info(driver)
            if el and state == "logout":
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.2)
                try:
                    el.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", el)
                with self._print_lock:
                    print("[AUTH] Logout clicked.")
            else:
                with self._print_lock:
                    print(f"[AUTH] Logout skipped (state={state}).")
        except Exception as e:
            with self._print_lock:
                print(f"[AUTH] Logout failed: {e}")

    def _login_automatically(self, driver, email: str, password: str) -> bool:
        """
        Логика:
        1) Открыть стартовую (shipbuilds.aspx).
        2) Если уже залогинены (logout.jpg) — готово.
        3) Иначе кликнуть #content_hrd_web_btn_logon (Images/logon.jpg) => перейти на форму.
        4) Ввести email/password, нажать content_ctl_signin_btn_logon.
        5) Ждать появления logout.jpg, затем убедиться, что на shipbuilds.aspx.
        """
        target_suffix = "shipbuilds.aspx?nmkhTk8Pl4EN".lower()

        try:
            # 1) стартовая
            driver.get(self.login_url_fallback)
            WebDriverWait(driver, self.wait_sec).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # 2) уже залогинен?
            if self._is_logged_in(driver):
                print(f"[AUTH] Already logged in → {driver.current_url}")
                return True

            # 3) перейти на форму через клик по шапке
            ok = self._go_to_login_via_header(driver, open_start_url=False)
            if not ok:
                # fallback: напрямую на signin.aspx
                driver.get(self.login_page_url)
                WebDriverWait(driver, self.wait_sec).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # 4) ввести и кликнуть логин
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

            # 5) ждать logout.jpg или редирект + проверка state
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
                print(f"[AUTH] Logged in as: {email} (logout-image present) → {driver.current_url}")
                return True

            print(f"[AUTH] Login seems failed (no logout-image), URL: {driver.current_url}")
            return False

        except Exception as e:
            print(f"[AUTH] Auto-login failed for {email}: {e}")
            return False

    def _login_manually(self, driver, wait_secs: int):
        """
        Даём время на ручной вход. Перед этим открываем стартовую и кликаем по логин-кнопке,
        чтобы форма точно появилась. После паузы проверяем наличие logout.jpg.
        """
        try:
            driver.get(self.login_url_fallback)
            WebDriverWait(driver, self.wait_sec).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception:
            pass

        # если уже залогинены — ничего не делаем
        if not self._is_logged_in(driver):
            self._go_to_login_via_header(driver, open_start_url=False)

        wait_secs = max(1, int(wait_secs))
        for sec in range(wait_secs, 0, -1):
            print(f"[AUTH] Время на ручной логин: {sec} сек", end="\r")
            time.sleep(1)
        print()

        ok = self._is_logged_in(driver)
        if not ok:
            # небольшой поллинг
            for _ in range(10):
                time.sleep(1)
                if self._is_logged_in(driver):
                    ok = True
                    break

        if ok:
            if "shipbuilds.aspx?nmkhTk8Pl4en" not in (driver.current_url or "").lower():
                try:
                    driver.get(self.login_url_fallback)
                except Exception:
                    pass
            print(f"[AUTH] Login OK (logout-image present) → {driver.current_url}")
        else:
            print(f"[AUTH] Не вижу logout-image. Вероятно, не авторизованы. URL: {driver.current_url}")


    # ---------- источники ссылок ----------
    def _load_urls(self) -> List[str]:
        urls: List[str] = []

        def add_url(u: str, origin: Optional[str] = None):
            if not u:
                return
            u = str(u).strip()
            if not u:
                return
            urls.append(u)
            if origin:
                self._origin_by_url[canonical_url(u)] = str(origin).strip()

        # JSON/ARRAY/DICT/NDJSON
        if self.input_json.exists():
            raw = self.input_json.read_text(encoding="utf-8").strip()
            if raw:
                try:
                    data = json.loads(raw)
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, str):
                                add_url(item, None)
                            elif isinstance(item, dict):
                                add_url(item.get("url") or item.get("link"),
                                        item.get("origin_yard") or item.get("origin") or item.get("yard"))
                    elif isinstance(data, dict):
                        add_url(data.get("url") or data.get("link"),
                                data.get("origin_yard") or data.get("origin") or data.get("yard"))
                except json.JSONDecodeError:
                    for line in raw.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            if isinstance(obj, str):
                                add_url(obj, None)
                            elif isinstance(obj, dict):
                                add_url(obj.get("url") or obj.get("link"),
                                        obj.get("origin_yard") or obj.get("origin") or obj.get("yard"))
                        except Exception:
                            if line.startswith("http"):
                                add_url(line, None)

        if not urls and self.input_txt.exists():
            for ln in self.input_txt.read_text(encoding="utf-8").splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                # txt может быть просто url или url|origin_yard
                if "|" in ln:
                    u, oy = ln.split("|", 1)
                    add_url(u.strip(), oy.strip())
                else:
                    add_url(ln, None)

        # уникальные в исходном порядке
        seen = set()
        uniq = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                uniq.append(u)
        return uniq

    # ---------- основной цикл (один драйвер, ротация аккаунтов) ----------
    def run(self) -> Tuple[int, int]:
        urls = self._load_urls()
        urls_pending = [u for u in urls if not self._node_path(u).exists()]
        if self.max_items_per_run is not None and self.max_items_per_run > 0:
            urls_pending = urls_pending[: self.max_items_per_run]

        print(f"Всего ссылок в списке: {len(urls)}; к обработке: {len(urls_pending)}")
        if not urls_pending:
            print("Нечего делать — все URL уже обработаны (или есть *.error.json для ретрая).")
            return len(urls), 0

        driver = None
        processed_total = 0
        processed_since_login = 0

        try:
            driver = self._driver_factory()
            parser = ShipDetailsParser(driver=driver, wait_sec=self.wait_sec)

            # --- первичный вход: либо ручной, либо автологин текущим аккаунтом ---
            cur_acc = self._accounts.current()
            print(self._accounts.debug_state())
            if self.relogin_manual:
                # ручной вход (даём время)
                self._login_manually(driver, self.first_login_wait_sec)
            else:
                # авто-вход текущей учёткой
                ok = self._login_automatically(driver, cur_acc["email"], cur_acc["password"])
                if not ok:
                    # пробуем сразу следующую
                    cur_acc = self._accounts.next()
                    print(self._accounts.debug_state())
                    self._login_automatically(driver, cur_acc["email"], cur_acc["password"])

            # --- обходим ссылки ---
            for url in urls_pending:
                if self._stop_event.is_set():
                    break
                node_path = self._node_path(url)
                if node_path.exists():
                    continue

                try:
                    data = self._parse_with_retry(parser, url, min_tables=self.min_tables_required, retries=2, delay=1.5)

                    # Баннер «limited to 50 records»?
                    if parser.has_daily_limit_banner():
                        print("[GUARD] Обнаружен баннер лимита 50/день — выполняю ротацию аккаунта.")
                        # Logout и relogin
                        self._logout_safely(driver)
                        if self.relogin_manual:
                            self._login_manually(driver, self.relogin_wait_sec)
                        else:
                            cur_acc = self._accounts.next()
                            print(self._accounts.debug_state())
                            self._login_automatically(driver, cur_acc["email"], cur_acc["password"])
                        processed_since_login = 0
                        # повторно парсим после relogin (с ретраями)
                        data = self._parse_with_retry(parser, url, min_tables=self.min_tables_required, retries=2, delay=1.5)

                    # ---- добавляем origin_yard, если есть ----
                    origin = self._origin_by_url.get(canonical_url(url))
                    if origin:
                        data["origin_yard"] = origin

                    # сохраняем результат
                    with open(node_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)

                    tables_cnt = len(data.get("tables", []))
                    print(f"[SEQ] saved -> {node_path.name} ({tables_cnt} tables)")
                    processed_total += 1
                    processed_since_login += 1

                    # если после ретраев всё ещё мало таблиц — считаем это ограничением → ротация
                    if tables_cnt < self.min_tables_required:
                        print(f"[GUARD] Tables {tables_cnt} < {self.min_tables_required}: ротация аккаунта.")
                        self._logout_safely(driver)
                        if self.relogin_manual:
                            self._login_manually(driver, self.relogin_wait_sec)
                        else:
                            cur_acc = self._accounts.next()
                            print(self._accounts.debug_state())
                            self._login_automatically(driver, cur_acc["email"], cur_acc["password"])
                        processed_since_login = 0
                        continue

                    # каждые batch_logout_every — Logout и relogin
                    if self.batch_logout_every and processed_since_login >= self.batch_logout_every:
                        print(f"[BATCH] Достигнут лимит {self.batch_logout_every} записей: ротация аккаунта.")
                        self._logout_safely(driver)
                        if self.relogin_manual:
                            self._login_manually(driver, self.relogin_wait_sec)
                        else:
                            cur_acc = self._accounts.next()
                            print(self._accounts.debug_state())
                            self._login_automatically(driver, cur_acc["email"], cur_acc["password"])
                        processed_since_login = 0

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




    def _is_logged_in(self, driver: WebDriver) -> bool:
        """
        Логин успешен, если:
        - видна кнопка Logout (#content_hrd_header_btn_logon), ИЛИ
        - URL содержит shipbuilds.aspx?nmkhTk8Pl4EN
        """
        try:
            if driver.find_elements(By.ID, "content_hrd_header_btn_logon"):
                return True
            cur = (driver.current_url or "").lower()
            if "shipbuilds.aspx?nmkhTk8Pl4en" in cur:  # <--- правильная строка
                return True
        except Exception:
            pass
        return False

    def _parse_with_retry(self, parser, url: str, min_tables: int, retries: int = 2, delay: float = 1.5) -> Dict:
        """
        Открывает и парсит страницу. Если таблиц меньше min_tables — делает ещё retries попыток
        с небольшим ожиданием/refresh. Возвращает последний результат.
        """
        data = parser.parse_ship_details(url)
        tables_cnt = len(data.get("tables", []))
        attempt = 0
        while tables_cnt < min_tables and attempt < retries:
            attempt += 1
            print(f"[RETRY] tables={tables_cnt} < {min_tables}. Попытка {attempt}/{retries}...")
            try:
                time.sleep(delay)
                parser.open(url)  # переоткрыть
                data = parser.parse_ship_details(url)
                tables_cnt = len(data.get("tables", []))
            except Exception:
                break
        return data
