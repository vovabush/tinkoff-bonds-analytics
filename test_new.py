import os
import time
import json
import math
import argparse
import logging
import requests
import xlsxwriter
import pandas as pd
from tqdm import tqdm
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from subprocess import Popen
import winreg

from tinkoff.invest import Client
from tinkoff.invest.utils import quotation_to_decimal
from bs4 import BeautifulSoup
from functools import lru_cache
import random
from grpc import RpcError, StatusCode


def get_coupons_with_smart_retry(client, figi, date_from, date_to, max_retries=5):
    delay = 0.5
    last_err = None
    for i in range(max_retries):
        try:
            return client.instruments.get_bond_coupons(figi=figi, from_=date_from, to=date_to).events
        except RpcError as e:
            last_err = e
            code = getattr(e, "code", lambda: None)()
            # на INTERNAL увеличиваем бэкофф заметнее
            if code == StatusCode.INTERNAL:
                jitter = random.uniform(0, 0.25)
                time.sleep(delay + jitter)
                delay *= 2  # экспоненциально
                continue
            # для других ошибок — используй обычный бэкофф
            time.sleep(0.5 * (2 ** i))
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** i))
    raise last_err

# ---- Глобальные настройки / совместимость со старым кодом ----
requests.packages.urllib3.disable_warnings()  # (№7 не внедряем таймауты/verify=True)

DIV = 1_000_000_000
UTCNOW = datetime.now(timezone.utc)

TOKEN = ""
API_DELAY = 0.5
EXCEL_TABLE_NAME = "bonds.xlsx"
FOR_QUAL_INVESTOR = None
AMORTIZATION = False
FLOATING_COUPON = False
NOT_WRITE_WITHOUT_RATING = False

FILENAME_FOR_NRA_OUTPUT = "NRA_ratings.xlsx"
FILENAME_FOR_NKR_OUTPUT = "NKR_ratings.xlsx"

# ---- Клиент Tinkoff: один на всё исполнение ----
_CLIENT_CTX = None  # сам контекст-менеджер (на нём вызываем __exit__)
_CLIENT = None      # объект, возвращённый __enter__ (на нём есть .instruments, .market_data и т.п.)

def get_client():
    global _CLIENT_CTX, _CLIENT
    if _CLIENT is None:
        _CLIENT_CTX = Client(TOKEN)      # создаём контекст-менеджер
        _CLIENT = _CLIENT_CTX.__enter__()  # входим и сохраняем возвращённый объект
    return _CLIENT

def close_client():
    global _CLIENT_CTX, _CLIENT
    if _CLIENT_CTX is not None:
        try:
            _CLIENT_CTX.__exit__(None, None, None)
        finally:
            _CLIENT_CTX = None
            _CLIENT = None

# ---- Ретраи с экспоненциальным backoff (№2) ----
def call_with_retry(fn, *args, retries=3, delay=0.5, **kwargs):
    last_err = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            logging.warning("Retry %d/%d for %s: %s", i + 1, retries, getattr(fn, "__name__", "call"), e)
            time.sleep(delay * (2 ** i))
    raise last_err

# ---- Перевод сектора (как было) ----
def translate_sector(sector_en):
    mapping = {
        "financial": "Финансы",
        "consumer": "Потребительский",
        "real_estate": "Недвижимость",
        "materials": "Ресурсы",
        "utilities": "Коммунальный",
        "telecom": "Телекоммуникации",
        "industrials": "Промышленность",
        "other": "Другое",
        "health_care": "Здравоохранение",
        "it": "ИТ",
        "energy": "Энергетика",
        "municipal": "Муниципальный",
        "government": "Государственный",
    }
    return mapping.get(sector_en, sector_en)

# ---- Кэш рейтингов и ИНН (№8) ----
@lru_cache(maxsize=5000)
def get_company_itn(isin: str) -> str:
    data_for_get_itn = {
        "from_code": "isin",
        "input_from_isin": isin,
        "isin_code_state": "Y",
        "cfi_code_state": "Y",
        "search": 1,
    }
    try:
        r = requests.post("https://www.isin.ru/ru/ru_isin/db/", data=data_for_get_itn, verify=False)
    except Exception as e:
        raise ValueError("get_company_itn::" + str(e))

    if r.text.find("index.php?type=issue_id") == -1:
        return ""
    company_url = "https://www.isin.ru/ru/ru_isin/db/" + r.text[r.text.find("index.php?type=issue_id"):].split("\"")[0]

    try:
        r = requests.get(company_url, verify=False)
    except Exception as e:
        raise ValueError("get_company_itn::" + str(e))

    if r.text.find("ИНН") == -1:
        return ""
    try:
        return str(int(r.text[r.text.find("ИНН") + 16:].split("<")[0]))
    except Exception:
        return ""

@lru_cache(maxsize=5000)
def get_acra_rating_by_isin(isin: str) -> str:
    try:
        r = requests.get("https://www.acra-ratings.ru/search/?q=" + isin, verify=False,
                         headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    except Exception as e:
        raise ValueError("get_acra_rating_by_isin::" + str(e))
    if r.status_code != 200:
        raise ValueError("get_acra_rating_by_isin::Ошибка запроса: " + str(r.status_code))

    try:
        parsed_html = BeautifulSoup(r.text, "lxml")
        # на странице выдачи ищем карточки
        search_items = parsed_html.body.find_all('div', attrs={'class': "search-result__item"})
        for item in search_items:
            try:
                tag_text = item.find('div', attrs={'class': "tag"}).text.replace(" ", "")
                if "Выпуск" in tag_text:
                    href = item.find('a', attrs={'class': "search-result__item-text"})['href']
                    return acra_get_rating_by_url(href)
            except Exception:
                continue
    except Exception as e:
        raise ValueError("get_acra_rating_by_isin::" + str(e))
    return "Не оценен"

def acra_get_rating_by_url(url: str) -> str:
    request_url = "https://www.acra-ratings.ru" + url
    r = requests.get(request_url, verify=False,
                     headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    if r.status_code != 200:
        raise ValueError("acra_get_rating_by_url::Ошибка: " + str(r.status_code))
    parsed_html = BeautifulSoup(r.text, "lxml")
    widget = parsed_html.body.find('div', attrs={'class': "rating-widget"})
    return widget.text.replace(" ", "").replace("\n", "") if widget else "Не оценен"

def ensure_daily_file(path: str, url: str, headers=None):
    """Грубо: если файла нет или он создан/изменён не сегодня — скачать заново."""
    if not os.path.exists(path) or datetime.fromtimestamp(os.path.getmtime(path)).date() != datetime.now().date():
        r = requests.get(url, headers=headers or {})
        open(path, 'wb').write(r.content)

@lru_cache(maxsize=5000)
def get_NRA_rating_by_itn(itn: str) -> str:
    ensure_daily_file(
        FILENAME_FOR_NRA_OUTPUT,
        "https://www.ra-national.ru/wp-load.php?security_key=100c906f36a0b90e&export_id=20&action=get_data",
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    )
    df = pd.read_excel(FILENAME_FOR_NRA_OUTPUT)
    if "ИНН" not in df.columns or "Рейтинг" not in df.columns:
        return "Не оценен"
    rows = df[df["ИНН"].astype(str) == str(itn)]
    if rows.empty:
        return "Не оценен"
    try:
        return rows.iloc[0]["Рейтинг"]
    except Exception:
        return "Не оценен"

@lru_cache(maxsize=5000)
def get_NKR_rating_by_itn(itn: str) -> str:
    ensure_daily_file(
        FILENAME_FOR_NKR_OUTPUT,
        "https://ratings.ru/issuers.php"
    )
    df = pd.read_excel(FILENAME_FOR_NKR_OUTPUT)
    # ожидаемые колонки "TIN" / "Rating"
    if "TIN" not in df.columns or "Rating" not in df.columns:
        return "Не оценен"
    rows = df[df["TIN"].astype(str) == str(itn)]
    if rows.empty:
        return "Не оценен"
    try:
        return rows.iloc[0]["Rating"]
    except Exception:
        return "Не оценен"

# ---- Фильтрация облигаций (№12 уточнённая логика слегка) ----
def is_available_bond(bond) -> bool:
    return (
        bond.buy_available_flag
        and bond.floating_coupon_flag == FLOATING_COUPON
        and bond.amortization_flag == AMORTIZATION
        and bond.for_qual_investor_flag == FOR_QUAL_INVESTOR
        and bond.currency == "rub"
        and bond.class_code != 'PSAU'
        and bond.coupon_quantity_per_year > 0
    )

# ---- Модель строки для Excel (№10) ----
@dataclass
class BondRow:
    Имя: str
    Тикер: str
    Цена_плюс_НКД: float
    Купон: float
    Годовая_доходность: float  # как было (с учётом 0.87)
    Доходность_к_погашению: object  # может быть "Н/д"
    Купонов_в_год: int
    Лет_до_погашения: float
    Дюрация: float
    Рейтинг_АКРА: str | None = None
    Рейтинг_НРА: str | None = None
    Рейтинг_НКР: str | None = None
    Риск_Тинькофф: str | None = None
    Сектор: str | None = None

# ---- Сбор данных и расчёты (№1,3,9,11 частично) ----
def collect_bonds():
    client = get_client()
    # все инструменты
    instruments = call_with_retry(lambda: client.instruments.bonds()).instruments

    # применим фильтр
    bonds = [b for b in instruments if is_available_bond(b)]
    if not bonds:
        return []

    # батч котировок (№9)
    figis = [b.figi for b in bonds]
    last_prices_resp = call_with_retry(lambda: client.market_data.get_last_prices(figi=figis))
    last_prices_map = {lp.figi: lp.price for lp in last_prices_resp.last_prices}

    rows: list[BondRow] = []
    pbar = tqdm(total=len(bonds), desc="Прогресс", unit="облигация")

    for b in bonds:
        pbar.update(1)
        try:
            nominal = b.nominal.units + (b.nominal.nano / DIV)

            # Купоны на «долгий» горизонт (как было — до 2*3650 дней это избыточно; ставим 30 лет)
            '''
            coupons = call_with_retry(
                lambda: client.instruments.get_bond_coupons(
                    figi=b.figi, from_=UTCNOW, to=UTCNOW + timedelta(days=365 * 30)
                )
            ).events
            '''
            end_to = b.maturity_date

            # если по какой-то причине дата погашения не задана/в прошлом — всё равно ограничим вменяемо
            if not end_to or end_to <= UTCNOW:
                end_to = UTCNOW + timedelta(days=365 * 3)
            else:
                end_to = max(end_to + timedelta(days=7), UTCNOW + timedelta(days=365 * 3))

            coupons = get_coupons_with_smart_retry(client, b.figi, UTCNOW, end_to)

            # купон (первый будущий)
            try:
                coupon_value = coupons[0].pay_one_bond.units + (coupons[0].pay_one_bond.nano / DIV)
            except Exception:
                coupon_value = 0.0

            # цена
            quotation = last_prices_map.get(b.figi)
            if quotation is not None:
                q = quotation
                # как было: денежная цена = (units + nano/DIV)/100 * nominal
                price_clean = (q.units + (q.nano / DIV)) / 100.0 * nominal
            else:
                price_clean = 0.0

            aci = b.aci_value.units + (b.aci_value.nano / DIV)
            price_dirty = price_clean + aci

            # годовая доходность (как было, с «0.87»)
            try:
                yeild = round((0.87 * (coupon_value * b.coupon_quantity_per_year)) /
                              (price_clean + aci), 3) * 100
            except Exception:
                yeild = 0.0

            years_before_maturity = round((b.maturity_date - UTCNOW).days / 365.25, 1)

            # duration «как было» — без дисконтирования (№4 исключён)
            duration_acc = 0.0
            common_income = 0.0
            for c in coupons:
                coupon_cost = c.pay_one_bond.units + (c.pay_one_bond.nano / DIV)
                years_before_payment = round((c.coupon_date - UTCNOW).days / 365.25, 2)
                duration_acc += coupon_cost * years_before_payment
                common_income += coupon_cost
            common_income += nominal
            try:
                duration = round((duration_acc + nominal * years_before_maturity) / common_income, 2)
            except Exception:
                duration = 0.0

            # простая «доходность к погашению» из исходника (№4 исключён)
            ytm = 0
            coupon_income_sum = 0
            ytm_na = False
            for c in coupons:
                if c.pay_one_bond.units + (c.pay_one_bond.nano / DIV) == 0:
                    ytm_na = True
                coupon_income_sum += c.pay_one_bond.units + (c.pay_one_bond.nano / DIV)

            if ytm_na:
                ytm_value = "Н/д"
            else:
                try:
                    days_to_maturity = (b.maturity_date - UTCNOW).days
                    ytm_val = (0.87 * 365 * (coupon_income_sum + nominal - price_clean - aci) /
                               (days_to_maturity * (price_clean + aci)))
                    ytm_value = round(ytm_val, 3) * 100
                except Exception:
                    ytm_value = 0.0

            # Риски/секторы/рейтинги (№8 кэш, без запросов для госов)
            sector_ru = translate_sector(b.sector)
            try:
                risk_tinkoff = b.risk_level.value
            except Exception:
                risk_tinkoff = "Не оценен"

            acra = "Отсутствует"
            nra = "Отсутствует"
            nkr = "Отсутствует"

            if b.sector != "government":
                try:
                    acra = get_acra_rating_by_isin(b.isin)
                except Exception as e:
                    logging.warning("ACRA error for %s: %s", b.isin, e)
                    acra = "Ошибка запроса"

                try:
                    itn = get_company_itn(b.isin)
                    if itn:
                        try:
                            nra = get_NRA_rating_by_itn(itn)
                        except Exception as e:
                            logging.warning("NRA error for %s: %s", itn, e)
                            nra = "Ошибка запроса"
                        try:
                            nkr = get_NKR_rating_by_itn(itn)
                        except Exception as e:
                            logging.warning("NKR error for %s: %s", itn, e)
                            nkr = "Ошибка запроса"
                    else:
                        nra = "Отсутствует"
                        nkr = "Отсутствует"
                except Exception as e:
                    logging.warning("ITN error for %s: %s", b.isin, e)
                    nra = "Ошибка запроса ИНН"
                    nkr = "Ошибка запроса ИНН"

            # формирование строки
            row = BondRow(
                Имя=b.name,
                Тикер=b.ticker,
                Цена_плюс_НКД=round(price_dirty, 2),
                Купон=round(coupon_value, 2),
                Годовая_доходность=yeild,
                Доходность_к_погашению=ytm_value,
                Купонов_в_год=b.coupon_quantity_per_year,
                Лет_до_погашения=years_before_maturity,
                Дюрация=duration,
                Рейтинг_АКРА=acra if b.sector != "government" else "Отсутствует",
                Рейтинг_НРА=nra if b.sector != "government" else "Отсутствует",
                Рейтинг_НКР=nkr if b.sector != "government" else "Отсутствует",
                Риск_Тинькофф=risk_tinkoff,
                Сектор=sector_ru,
            )

            rows.append(row)
        except Exception as e:
            logging.exception("Error on bond %s: %s", getattr(b, "ticker", "?"), e)

        # уважим лимиты API между итерациями (как было)
        time.sleep(API_DELAY)

    pbar.close()
    return rows

# ---- Excel вывод через pandas + xlsxwriter (№14). Оставляем autofit() (№5 исключён) ----
def write_excel(government_rows: list[BondRow], corporate_rows: list[BondRow], filename: str):
    # сортировка по «Годовая_доходность» по убыванию (как было по yeild)
    df_gov = pd.DataFrame([asdict(r) for r in government_rows]).sort_values("Годовая_доходность", ascending=False)
    df_corp = pd.DataFrame([asdict(r) for r in corporate_rows]).sort_values("Годовая_доходность", ascending=False)

    # фильтр «не писать без рейтингов» только для корп. листа (и с исправленной проверкой №6)
    global NOT_WRITE_WITHOUT_RATING
    if NOT_WRITE_WITHOUT_RATING and not df_corp.empty:
        mask_has_any = ~(
            (df_corp["Рейтинг_АКРА"] == "Не оценен") &
            (df_corp["Рейтинг_НРА"] == "Не оценен") &
            (df_corp["Рейтинг_НКР"] == "Не оценен")
        )
        df_corp = df_corp[mask_has_any]

    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        df_gov.to_excel(writer, sheet_name="Государственные", index=False)
        df_corp.to_excel(writer, sheet_name="Корпоративные", index=False)

        # оставляем попытку autofit(), как в исходнике (№5 не исправляем)
        try:
            writer.sheets["Государственные"].autofit()
            writer.sheets["Корпоративные"].autofit()
        except Exception:
            # xlsxwriter обычно не имеет autofit(); оставим молча — согласно исключению №5
            pass

def open_excel(filename: str):
    # старый поиск пути к Excel через реестр, как было
    try:
        handle = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                                r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\excel.exe")
        num_values = winreg.QueryInfoKey(handle)[1]
        excel_path = None
        for i in range(num_values):
            if winreg.EnumValue(handle, i)[0] == 'Path':
                excel_path = winreg.EnumValue(handle, i)[1] + "EXCEL.EXE"
                break
        if not excel_path:
            raise FileNotFoundError
    except Exception:
        raise ValueError("get_excel_path::Путь к Excel.exe не найден. Откройте файл самостоятельно.")

    try:
        Popen([excel_path, filename])
    except Exception as ex:
        raise ValueError(f"open_output_table::Файл {filename} готов, но excel.exe не запускается: {ex}")

# ---- Конфиг и CLI (№13) ----
def parse_config():
    global TOKEN, API_DELAY, EXCEL_TABLE_NAME, FOR_QUAL_INVESTOR, AMORTIZATION, FLOATING_COUPON
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        TOKEN = cfg["TOKEN"]
        if not TOKEN:
            raise ValueError("В файле config.json нет токена!")
        API_DELAY = cfg.get("API_DELAY", 0.5)
        EXCEL_TABLE_NAME = cfg.get("EXCEL_TABLE_NAME", "bonds.xlsx")
        FOR_QUAL_INVESTOR = cfg.get("FOR_QUAL_INVESTOR", None)
        AMORTIZATION = cfg.get("AMORTIZATION", False)
        FLOATING_COUPON = cfg.get("FLOATING_COUPON", False)
    except Exception as e:
        raise ValueError("parse_parameters_from_config::" + str(e))

def main():
    parser = argparse.ArgumentParser(description="Загрузка облигаций через Tinkoff Invest API и экспорт в Excel.")
    parser.add_argument(
        "-c", "--clear", action="store_true", default=False,
        help="Не выводить в листе 'Корпоративные' компании без всех трёх рейтингов (АКРА, НРА, НКР)"
    )
    parser.add_argument("--out", default="bonds.xlsx", help="Имя выходного Excel файла (переопределяет config.json)")
    parser.add_argument("--log", default="WARNING", help="Уровень логирования: DEBUG|INFO|WARNING|ERROR")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s: %(message)s")

    global NOT_WRITE_WITHOUT_RATING, EXCEL_TABLE_NAME
    NOT_WRITE_WITHOUT_RATING = args.clear

    try:
        parse_config()
        if args.out:
            EXCEL_TABLE_NAME = args.out

        client = get_client()  # открыть соединение один раз (№1)

        # загрузка и разделение на гос/корп
        all_rows = collect_bonds()
        gov_rows = []
        corp_rows = []
        for r in all_rows:
            if r.Сектор == "Государственный" or r.Сектор == "Муниципальный":
                # оставим логику «government» как отдельный лист; муниципальные можно считать к государственным
                gov_rows.append(r)
            else:
                corp_rows.append(r)

        write_excel(gov_rows, corp_rows, EXCEL_TABLE_NAME)
        open_excel(EXCEL_TABLE_NAME)

    except Exception as ex:
        print(ex)
    finally:
        close_client()

if __name__ == "__main__":
    main()
