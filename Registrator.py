import os
import time
import random
import string
import json
import threading
from dataclasses import dataclass
from typing import Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from chromedriver_factory import ChromeDriverFactory


# ========= Утилиты =========

def _rand_sleep(a: float = 0.8, b: float = 2.2) -> None:
    time.sleep(random.uniform(a, b))


def _now_ts() -> int:
    return int(time.time())


def _safe_lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()


# ========= Конфиг =========

@dataclass
class OutputConfig:
    jsonl_path: str = "shipbuilding_accounts.jsonl"   # безопасная дозапись (по строке JSON на аккаунт)
    snapshot_json_path: str = "shipbuilding_accounts.json"  # слепок всех аккаунтов (перезаписывается вручную при вызове)
    txt_path: str = "shipbuilding_accounts.txt"      # для быстрого просмотра email:pass
    truncate_on_start: bool = False                   # НЕ стирать файлы по умолчанию


# ========= Основной класс =========

class ChinaShipbuildingRegistrator:
    def __init__(
        self,
        use_existing_profile: bool = False,
        profile_name: str = "Default",
        headless: bool = False,
        output: OutputConfig = OutputConfig(),
        max_retries: int = 3,
    ) -> None:
        self.accounts: list[Dict[str, Any]] = []
        self.lock = threading.Lock()
        self.success_count = 0
        self.total_count = 0
        self.output = output
        self.max_retries = max_retries

        # --- Фабрика драйверов ---
       
        self.factory = ChromeDriverFactory.with_default_windows_profile(profile_name="Default")
     
        self.factory.headless = headless
        # Клонирование профиля включайте только если оно реально поддерживается вашей фабрикой
        if hasattr(self.factory, "use_profile_clone"):
            self.factory.use_profile_clone = not use_existing_profile

        # --- Подготовка выходных файлов ---
        self._prepare_output_files()

    # ---------- Файловая подсистема ----------
    def _prepare_output_files(self) -> None:
        # JSONL — безопасный для многопоточной дозаписи формат (по строке на запись)
        if self.output.truncate_on_start:
            # Полная очистка
            for p in [self.output.jsonl_path, self.output.snapshot_json_path, self.output.txt_path]:
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass

        # Создаем пустые файлы, если их нет
        for p in [self.output.jsonl_path, self.output.txt_path]:
            if not os.path.exists(p):
                with open(p, "w", encoding="utf-8") as f:
                    if p.endswith(".jsonl"):
                        # ничего, оставим пустым — одна запись = одна строка JSON
                        pass
                    else:
                        f.write("")

    def flush_json_snapshot(self) -> None:
        """Собрать shipbuilding_accounts.json из JSONL (удобно по окончании работы)."""
        try:
            items = []
            if os.path.exists(self.output.jsonl_path):
                with open(self.output.jsonl_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            items.append(json.loads(line))
                        except Exception:
                            # пропускаем битые строки
                            pass
            with open(self.output.snapshot_json_path, "w", encoding="utf-8") as jf:
                json.dump(items, jf, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠ Не удалось обновить слепок JSON: {e}")

    def _append_jsonl_threadsafe(self, obj: Dict[str, Any]) -> None:
        line = json.dumps(obj, ensure_ascii=False)
        with self.lock:
            with open(self.output.jsonl_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            with open(self.output.txt_path, "a", encoding="utf-8") as tf:
                tf.write(f"{obj['email']}:{obj['password']}\n")

    # ---------- Генерация данных ----------
    def generate_random_data(self, index: int) -> Dict[str, Any]:
        domains = ["mail.com", "email.com", "inbox.com", "post.com", "mail.net"]

        # Гарантированная уникальность email по времени + индекс
        email_local = (
            ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
            + f"{_now_ts()}{index}"
        )
        email = f"{email_local}@{random.choice(domains)}"

        # Надежный пароль (минимум 8 символов)
        pw_chars = string.ascii_letters + string.digits
        password = ''.join(random.choices(pw_chars, k=10))

        name_chars = string.ascii_letters
        full_name = (
            ''.join(random.choices(name_chars, k=random.randint(6, 10)))
            + " "
            + ''.join(random.choices(name_chars, k=random.randint(6, 10)))
        )

        company_words = [
            "Company", "Corp", "Inc", "Ltd", "Group", "Solutions", "Tech"
        ]
        company = ''.join(random.choices(string.ascii_uppercase, k=3)) + " " + random.choice(company_words)

        roles = [
            (40, "Ship Owner"),
            (30, "Ship Yard"),
            (20, "Broker"),
            (25, "Equipment Supplier"),
        ]
        role_value, role_name = random.choice(roles)

        return {
            "index": index,
            "full_name": full_name,
            "email": email,
            "password": password,
            "company": company,
            "role_value": role_value,
            "role_name": role_name,
            "timestamp": _now_ts(),
        }

    # ---------- Selenium шаги ----------
    def init_driver(self):
        return self.factory.create()

    def fill_optional_fields(self, driver) -> None:
        try:
            mobile_number = str(random.randint(1_000_000, 9_999_999))
            tel_input = driver.find_element(By.ID, "content_ctl_register_txt_tel")
            tel_input.clear()
            tel_input.send_keys(mobile_number)
        except Exception:
            pass

    def _check_success_registration(self, driver) -> bool:
        """Проверяем несколько вариантов сообщения об успехе."""
        try:
            el = WebDriverWait(driver, 12).until(
                EC.presence_of_element_located((By.ID, "content_lbAct"))
            )
            t = _safe_lower(el.text)
            # Страница иногда пишет с опечаткой — учитываем оба варианта
            return (
                ("thanks for registration" in t) or
                ("thanks for registeration" in t)  # опечатка сайта (если есть)
            ) and ("logon" in t or "login" in t or "log on" in t)
        except Exception:
            return False

    def _submit_form(self, driver, data: Dict[str, Any]) -> None:
        # Выбор роли
        role_select = Select(driver.find_element(By.ID, "content_ctl_register_lst_role"))
        role_select.select_by_value(str(data["role_value"]))

        # Поля
        driver.find_element(By.ID, "content_ctl_register_txt_name").send_keys(data["full_name"])  # noqa: E501
        driver.find_element(By.ID, "content_ctl_register_txt_email").send_keys(data["email"])  # noqa: E501
        driver.find_element(By.ID, "content_ctl_register_txt_password").send_keys(data["password"])  # noqa: E501
        driver.find_element(By.ID, "content_ctl_register_txt_repassword").send_keys(data["password"])  # noqa: E501
        driver.find_element(By.ID, "content_ctl_register_txt_company").send_keys(data["company"])  # noqa: E501

        self.fill_optional_fields(driver)

        # Отправка формы
        btn = driver.find_element(By.ID, "content_ctl_register_btn_submite")
        driver.execute_script("arguments[0].click();", btn)

    # ---------- Внешние методы ----------
    def register_single_account(self, data: Dict[str, Any]) -> bool:
        url = "http://chinashipbuilding.cn/en/register.aspx"
        driver = None
        try:
            driver = self.init_driver()
            driver.set_page_load_timeout(60)
            driver.get(url)

            # Ожидаем форму
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "content_ctl_register_lst_role"))
            )

            # Несколько попыток с экспон. бэкофом
            for attempt in range(1, self.max_retries + 1):
                try:
                    _rand_sleep(0.4, 1.2)
                    self._submit_form(driver, data)
                    _rand_sleep(1.5, 3.0)
                    if self._check_success_registration(driver):
                        with self.lock:
                            self.accounts.append(data)
                            self.success_count += 1
                        # Мгновенно и безопасно дозаписываем
                        self._append_jsonl_threadsafe({
                            "index": data["index"],
                            "full_name": data["full_name"],
                            "email": data["email"],
                            "password": data["password"],
                            "company": data["company"],
                            "role": data["role_name"],
                            "timestamp": data["timestamp"],
                        })
                        print(f"✓ [{threading.current_thread().name}] Зарегистрирован {data['email']}")
                        return True
                    else:
                        raise RuntimeError("Сайт не подтвердил успешную регистрацию")
                except Exception as e:
                    if attempt < self.max_retries:
                        delay = 1.5 * attempt + random.random()
                        print(f"⚠ Попытка {attempt}/{self.max_retries} не удалась ({e}). Повтор через {delay:.1f}s…")
                        time.sleep(delay)
                    else:
                        print(f"✗ [{threading.current_thread().name}] Ошибка регистрации {data['email']}: {e}")
                        return False
        except Exception as e:
            print(f"✗ Критическая ошибка при открытии/ожидании страницы: {e}")
            return False
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    def register_accounts_multithreaded(self, count: int = 20, max_workers: int = 3) -> None:
        self.total_count = count
        self.success_count = 0
        self.accounts.clear()

        print(f"🚀 Старт регистрации {count} аккаунтов в {max_workers} поток(а/ов)")
        print(f"📁 JSONL: {self.output.jsonl_path}\n📁 TXT:   {self.output.txt_path}")

        start = time.time()
        accounts_data = [self.generate_random_data(i + 1) for i in range(count)]

        # Важно: не задирайте сильно кол-во потоков, сайт может резать/капчить
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(self.register_single_account, d): d for d in accounts_data}
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    d = futures[fut]
                    print(f"⚠ Необработанная ошибка для {d['email']}: {e}")

        dur = time.time() - start
        print("\n🎉 Готово!")
        print(f"⏱ Время: {dur:.1f}s | ✅ Успешно: {self.success_count}/{self.total_count}")
        if dur > 0:
            print(f"📊 Скорость: {self.success_count/dur:.2f} акк/сек")

        # По запросу — собираем слепок JSON (можно закомментировать, если не нужен каждый раз)
        self.flush_json_snapshot()

    def get_accounts_summary(self) -> str:
        if not self.accounts:
            return "Аккаунты не зарегистрированы"
        roles = {}
        for a in self.accounts:
            roles[a["role_name"]] = roles.get(a["role_name"], 0) + 1
        parts = [f"Всего аккаунтов: {len(self.accounts)}", "Распределение по ролям:"]
        for r, c in roles.items():
            parts.append(f"  {r}: {c}")
        return "\n".join(parts)


# ===== Пример =====
if __name__ == "__main__":
    registrator = ChinaShipbuildingRegistrator(
        use_existing_profile=False,
        headless=False,
        output=OutputConfig(
            jsonl_path="shipbuilding_accounts.jsonl",
            snapshot_json_path="shipbuilding_accounts.json",
            txt_path="shipbuilding_accounts.txt",
            truncate_on_start=False,  # ← Больше НЕ стираем файлы при старте
        ),
        max_retries=3,
    )

    try:
        registrator.register_accounts_multithreaded(count=20, max_workers=3)
        print("\n" + "=" * 50)
        print(registrator.get_accounts_summary())
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
