import time
import winreg
import json
import xlsxwriter
import requests
import urllib.parse
import os
import pandas

from datetime import datetime, timezone, timedelta
from subprocess import Popen
from tinkoff.invest import Client
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
from optparse import OptionParser
from tqdm import tqdm

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

TOKEN = ""
API_DELAY = 0.5
EXCEL_TABLE_NAME = None
FOR_QUAL_INVESTOR = None
AMORTIZATION = False
FLOATING_COUPON = False
DIV = 1000000000
REQUEST_TO_AKRA = {"text": "",
                   "sectors": [],
                   "activities": [],
                   "countries": [],
                   "forecasts": [],
                   "on_revision": 0,
                   "rating_scale": 0,
                   "rate_from": 0,
                   "rate_to": 0,
                   "page": 1,
                   "sort": "",
                   "count": "1500"}
START_TIME = None
FILENAME_FOR_NRA_OUTPUT = "NRA_ratings.xlsx"
FILENAME_FOR_NKR_OUTPUT = "NKR_ratings.xlsx"
NOT_WRITE_WITHOUT_RATING = False


class Bond:
    def __init__(self, bond_desc):
        nominal = bond_desc.nominal.units + (bond_desc.nominal.nano / DIV)
        with Client(TOKEN) as client:
            try:
                coupons = client.instruments.get_bond_coupons(figi=bond_desc.figi,
                                                              from_=datetime.today(),
                                                              to=datetime.today() + timedelta(days=3650*2)).events
            except:
                time.sleep(API_DELAY)
                coupons = client.instruments.get_bond_coupons(figi=bond_desc.figi,
                                                              from_=datetime.today(),
                                                              to=datetime.today() + timedelta(days=3650*2)).events

            try:
                self.coupon = coupons[0].pay_one_bond.units + (coupons[0].pay_one_bond.nano / DIV)
            except:
                self.coupon = 0

        self.ticker = bond_desc.ticker
        self.name = bond_desc.name
        self.years_before_maturity = round((bond_desc.maturity_date - datetime.now(timezone.utc)).days / 365.25, 1)
        self.accumulated_coupon_income = bond_desc.aci_value.units + (bond_desc.aci_value.nano / DIV)
        self.coupon_per_year = bond_desc.coupon_quantity_per_year

        with Client(TOKEN) as client:
            try:
                quotation = client.market_data.get_last_prices(figi=[bond_desc.figi]).last_prices[0].price
            except:
                time.sleep(API_DELAY)
                quotation = client.market_data.get_last_prices(figi=[bond_desc.figi]).last_prices[0].price

            try:
                self.price = (quotation.units + (quotation.nano / DIV)) / 100 * nominal
            except:
                self.price = 0
        
        try:
            self.yeild = round((0.87 * (self.get_coupon() * self.get_coupon_per_year())) /
                               (self.get_price() + self.get_accumulated_coupon_income()), 3) * 100
        except:
            self.yeild = 0

        self.isin = bond_desc.isin
        self.duration = 0
        common_income = 0
        for coupon in coupons:
            coupon_cost = (coupon.pay_one_bond.units + (coupon.pay_one_bond.nano / DIV))
            years_before_payment = round((coupon.coupon_date - datetime.now(timezone.utc)).days / 365.25, 2)
            self.duration += coupon_cost * years_before_payment
            common_income += coupon_cost
        common_income += nominal
        try:
            self.duration = round((self.duration + nominal * self.years_before_maturity) / common_income, 2)
        except:
            self.duration = 0

        self.yield_to_maturity = 0
        coupon_income = 0
        for coupon in coupons:
            if coupon.pay_one_bond.units + (coupon.pay_one_bond.nano / DIV) == 0:
               self.yield_to_maturity = "Н/д" 
            coupon_income += coupon.pay_one_bond.units + (coupon.pay_one_bond.nano / DIV)

        if self.yield_to_maturity != "Н/д":
            try:
                self.yield_to_maturity = round((0.87 * 365 * (coupon_income + nominal - self.get_price() - self.get_accumulated_coupon_income()) /
                                   (((bond_desc.maturity_date - datetime.now(timezone.utc)).days) * (self.get_price() + self.get_accumulated_coupon_income()))), 3) * 100
            except:
                self.yield_to_maturity = 0

        self.sector = translate_sector(bond_desc.sector)

        if bond_desc.sector == "government":
            self.rating_acra = "Отсутствует"
            self.itn = "Отсутствует"
            self.rating_nra = "Отсутствует"
            self.rating_nkr = "Отсутствует"
        else:
            try:
                self.rating_acra = get_acra_rating_by_isin(self.isin)
            except Exception as e:
                print(str(e))
                self.rating_acra = "Ошибка запроса"

            try:
                self.itn = self.get_company_itn()
            except Exception as e:
                print(str(e))
                self.rating_nra = "Ошибка запроса ИНН"
                self.rating_nkr = "Ошибка запроса ИНН"

            try:
                self.rating_nra = get_NRA_rating_by_isin(self.itn)
            except Exception as e:
                print(str(e))
                self.rating_nra = "Ошибка запроса"

            try:
                self.rating_nkr = get_NKR_rating_by_isin(self.itn)
            except Exception as e:
                print(str(e))
                self.rating_nkr = "Ошибка запроса"

        try:
            self.risk_tinkoff = bond_desc.risk_level.value
        except Exception as e:
            print(str(e))
            self.risk_tinkoff = "Не оценен"

    def get_price(self):
        return self.price

    def get_name(self):
        return self.name

    def get_ticker(self):
        return self.ticker

    def get_years_before_maturity(self):
        return self.years_before_maturity

    def get_coupon(self):
        return self.coupon

    def get_accumulated_coupon_income(self):
        return self.accumulated_coupon_income

    def get_coupon_per_year(self):
        return self.coupon_per_year

    def get_yeild(self):
        return self.yeild

    def get_yield_to_maturity(self):
        return self.yield_to_maturity

    def get_duration(self):
        return self.duration

    def get_acra_rating(self):
        return self.rating_acra

    def get_nra_rating(self):
        return self.rating_nra

    def get_nkr_rating(self):
        return self.rating_nkr

    def get_company_itn(self):
        data_for_get_itn = {"from_code": "isin",
                            "input_from_isin": self.isin,
                            "isin_code_state": "Y",
                            "cfi_code_state": "Y",
                            "search": 1}

        try:
            r = requests.post("https://www.isin.ru/ru/ru_isin/db/", data=data_for_get_itn, verify=False)
        except Exception as e:
            raise ValueError("get_company_itn::" + str(e))

        if r.text.find("index.php?type=issue_id") == -1:
            return ""
        else:
            company_url = "https://www.isin.ru/ru/ru_isin/db/" + r.text[r.text.find("index.php?type=issue_id"):].split("\"")[0]

        try:
            r = requests.get(company_url, verify=False)
        except Exception as e:
            raise ValueError("get_company_itn::" + str(e))

        if r.text.find("ИНН") == -1:
            return ""
        else:
            try:
                return int(r.text[r.text.find("ИНН")+16:].split("<")[0])
            except:
                return ""

    def get_tinkoff_risk(self):
        return self.risk_tinkoff

    def get_sector(self):
        return self.sector


def translate_sector(sector_en):
    if sector_en == "financial":
        return "Финансы"
    elif sector_en == "consumer":
        return "Потребительский"
    elif sector_en == "real_estate":
        return "Недвижимость"
    elif sector_en == "materials":
        return "Ресурсы"
    elif sector_en == "utilities":
        return "Коммунальный"
    elif sector_en == "telecom":
        return "Телекоммуникации"
    elif sector_en == "industrials":
        return "Промышленность"
    elif sector_en == "other":
        return "Другое"
    elif sector_en == "health_care":
        return "Здравоохранение"
    elif sector_en == "it":
        return "ИТ"
    elif sector_en == "energy":
        return "Энергетика"
    elif sector_en == "municipal":
        return "Муниципальный"
    else:
        return sector_en


def get_NKR_rating_by_isin(itn):

    if not os.path.exists(FILENAME_FOR_NKR_OUTPUT) or \
            datetime.fromtimestamp(os.path.getctime(FILENAME_FOR_NKR_OUTPUT)).day != datetime.now().day:
        r = requests.get("https://ratings.ru/issuers.php")
        open(FILENAME_FOR_NKR_OUTPUT, 'wb').write(r.content)

    current_nkr_rating_data_frame = pandas.read_excel(FILENAME_FOR_NKR_OUTPUT)
    rows_by_itn = current_nkr_rating_data_frame[current_nkr_rating_data_frame["TIN"] == itn]
    if rows_by_itn.empty:
        return "Не оценен"
    try:
        return rows_by_itn.at[rows_by_itn.index[0], 'Rating']
    except:
        return "Не оценен"


def get_NRA_rating_by_isin(itn):

    if not os.path.exists(FILENAME_FOR_NRA_OUTPUT) or \
            datetime.fromtimestamp(os.path.getctime(FILENAME_FOR_NRA_OUTPUT)).day != datetime.now().day:
        r = requests.get("https://www.ra-national.ru/wp-load.php?security_key=100c906f36a0b90e&export_id=20&action"
                         "=get_data")
        open(FILENAME_FOR_NRA_OUTPUT, 'wb').write(r.content)

    current_nra_rating_data_frame = pandas.read_excel(FILENAME_FOR_NRA_OUTPUT)
    rows_by_itn = current_nra_rating_data_frame[current_nra_rating_data_frame["ИНН"] == itn]
    if rows_by_itn.empty:
        return "Не оценен"
    try:
        return rows_by_itn.at[rows_by_itn.index[0], 'Рейтинг']
    except:
        return "Не оценен"


def acra_get_rating_by_url(url):
    request_url = "https://www.acra-ratings.ru" + url
    r = requests.get(request_url, verify=False, headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    if r.status_code != 200:
        raise ValueError("acra_get_rating_by_url::" + "Запрос к " + url + " произошёл с ошибкой: " + str(r.status_code))

    parsed_html = BeautifulSoup(r.text, "lxml")
    return parsed_html.body.find('div', attrs={'class': "rating-widget"}).text.replace(" ", "").replace("\n", "")


def get_acra_rating_by_isin(isin):
    try:
        r = requests.get("https://www.acra-ratings.ru/search/?q=" + isin, verify=False, headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    except Exception as e:
        raise ValueError("get_acra_rating_by_isin::" + str(e))
    if r.status_code != 200:
        raise ValueError("get_acra_rating_by_isin::" + "Попытка 1 запросить рейтинги эмитента" + isin + "у АКРА "
                                                                                                        "произошла с "
                                                                                                        "ошибкой: " +
                         str(r.status_code))

    try:
        parsed_html = BeautifulSoup(r.text, "lxml")
        search_tag = parsed_html.body.find('div', attrs={'class': "search-page__all-result search-tag"}).text
        search_results = (int(search_tag[search_tag.find("Найдено") + len("Найдено"):]))
    except Exception as e:
        raise ValueError("get_acra_rating_by_isin::" + str(e))

    for _ in range(search_results):
        search_items = parsed_html.body.find_all('div', attrs={'class': "search-result__item"})
        for item in search_items:
            try:
                searched_name = item.find('div', attrs={'class': "tag"}).text.replace(" ", "")
            except Exception as e:
                raise ValueError("get_acra_rating_by_isin::" + str(e))

            if "Выпуск" in searched_name:
                time.sleep(0.1)
                try:
                    return acra_get_rating_by_url(item.find('a', attrs={'class': "search-result__item-text"})['href'])
                except Exception as e:
                    raise ValueError("get_acra_rating_by_isin::" + str(e))
    return "Не оценен"


def is_available_bond(bond):
    if bond.buy_available_flag \
            and bond.floating_coupon_flag == FLOATING_COUPON \
            and bond.amortization_flag == AMORTIZATION \
            and bond.for_qual_investor_flag == FOR_QUAL_INVESTOR \
            and bond.currency == "rub" \
            and bond.class_code != 'PSAU' \
            and bond.coupon_quantity_per_year > 0:
        return True
    else:
        return False


def download_bonds_info(governmentBondObjects, corporateBondsObjects):
    with Client(TOKEN) as client:
        try:
            bonds = client.instruments.bonds()
        except Exception as ex:
            raise ValueError("download_bonds_info::" + "Нет связи с сервером по причине: " + str(ex))
    progress_bar = tqdm(total=len(bonds.instruments), desc="Прогресс", unit="облигация")
    for bond in bonds.instruments:
        progress_bar.update(1)
        if is_available_bond(bond):
            if bond.sector == "government":
                governmentBondObjects.append(Bond(bond))
            else:
                corporateBondsObjects.append(Bond(bond))
    progress_bar.close()


def write_list_in_excel_file(workbook, sheet, list):
    cell_format = workbook.add_format({'align': 'center'})
    sheet.write('A1', "Имя", cell_format)
    sheet.write('B1', "Тикер", cell_format)
    sheet.write('C1', "Цена + НКД", cell_format)
    sheet.write('D1', "Купон", cell_format)
    sheet.write('E1', "Годовая доходность", cell_format)
    sheet.write('F1', "Доходность к погашению", cell_format)
    sheet.write('G1', "Купонов в год", cell_format)
    sheet.write('H1', "Лет до погашения", cell_format)
    sheet.write('I1', "Дюрация", cell_format)
    if sheet.get_name() == "Корпоративные":
        sheet.write('J1', "Рейтинг (АКРА)", cell_format)
        sheet.write('K1', "Рейтинг (НРА)", cell_format)
        sheet.write('L1', "Рейтинг (НКР)", cell_format)
        sheet.write('M1', "Риск (Тинькофф)", cell_format)
        sheet.write('N1', "Сектор", cell_format)
    count = 2
    for bond in list:
        try:
            if bond.get_coupon() and bond.get_price():
                if NOT_WRITE_WITHOUT_RATING and \
                        sheet.get_name() == "Корпоративные" and \
                        bond.get_acra_rating() == "Не оценен" and \
                        bond.get_nkr_rating() == "Не оценен" and \
                        bond.get_nkr_rating() == "Не оценен":
                    continue
                sheet.write('A' + str(count), bond.get_name(), cell_format)
                sheet.write('B' + str(count), bond.get_ticker(), cell_format)
                sheet.write('C' + str(count), bond.get_price() + bond.get_accumulated_coupon_income(), cell_format)
                sheet.write('D' + str(count), bond.get_coupon(), cell_format)
                sheet.write('E' + str(count), bond.get_yeild(), cell_format)
                sheet.write('F' + str(count), bond.get_yield_to_maturity(), cell_format)
                sheet.write('G' + str(count), bond.get_coupon_per_year(), cell_format)
                sheet.write('H' + str(count), bond.get_years_before_maturity(), cell_format)
                sheet.write('I' + str(count), bond.get_duration(), cell_format)
                if sheet.get_name() == "Корпоративные":
                    sheet.write('J' + str(count), bond.get_acra_rating(), cell_format)
                    sheet.write('K' + str(count), bond.get_nra_rating(), cell_format)
                    sheet.write('L' + str(count), bond.get_nkr_rating(), cell_format)
                    sheet.write('M' + str(count), bond.get_tinkoff_risk(), cell_format)
                    sheet.write('N' + str(count), bond.get_sector(), cell_format)
                count += 1
        except:
            pass


def write_government_bonds_in_excel_file(workbook, governmentBondOdjects):
    governmentSheet = workbook.add_worksheet("Государственные")
    write_list_in_excel_file(workbook, governmentSheet, governmentBondOdjects)
    governmentSheet.autofit()


def write_corporate_bonds_in_excel_file(workbook, corporateBondsObjects):
    corporateSheet = workbook.add_worksheet("Корпоративные")
    write_list_in_excel_file(workbook, corporateSheet, corporateBondsObjects)
    corporateSheet.autofit()


def sort_bonds_list_by_yeild(bonds_list):
    bonds_list.sort(key=lambda x: x.yeild, reverse=True)


def create_output_table(governmentBondObjects, corporateBondsObjects):
    workbook = xlsxwriter.Workbook(EXCEL_TABLE_NAME)
    sort_bonds_list_by_yeild(governmentBondObjects)
    sort_bonds_list_by_yeild(corporateBondsObjects)
    try:
        write_government_bonds_in_excel_file(workbook, governmentBondObjects)
        write_corporate_bonds_in_excel_file(workbook, corporateBondsObjects)
    except Exception as ex:
        raise ValueError("create_output_table::" + "Что-то пошло не так во время составления таблиц: " + str(ex))
    workbook.close()


def get_excel_path():
    try:
        handle = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                            r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\excel.exe")

        num_values = winreg.QueryInfoKey(handle)[1]
        for i in range(num_values):
            if winreg.EnumValue(handle, i)[0] == 'Path':
                return winreg.EnumValue(handle, i)[1] + "EXCEL.EXE"
    except:
        raise ValueError("get_excel_path::" + "Путь к Excel.exe не найден. Откройте bonds.xlsx самостоятельно.")


def open_output_table():
    try:
        runnuingString = get_excel_path()
    except Exception as ex:
        raise ValueError(str(ex))
    args = [runnuingString, EXCEL_TABLE_NAME]
    try:
        Popen(args)
    except Exception as ex:
        raise ValueError("open_output_table" + "Файл "
                         + EXCEL_TABLE_NAME + " готов, но excel.exe не запускается: " + str(ex))


def debugAPI(ticker):
    with Client(TOKEN) as client:
        bonds = client.instruments.bond_by(id_type=2, class_code='TQCB', id=ticker)
        print(bonds)
        print(client.market_data.get_last_prices(figi=["TCS00A105QL6"]).last_prices)
        # print(str(client.instruments.get_accrued_interests(figi="BBG00QXGFHS6", from_=datetime.today() , 
        # to=datetime.today() + timedelta(days=365)).accrued_interests)) 
        exit(0)


def parse_parameters_from_config():
    global TOKEN
    global API_DELAY
    global EXCEL_TABLE_NAME
    global FOR_QUAL_INVESTOR
    global AMORTIZATION
    global FLOATING_COUPON
    try:
        with open('config.json') as json_file:
            config = json.load(json_file)
            TOKEN = config["TOKEN"]
            if TOKEN == "":
                raise ValueError("parse_parameters_from_config::" + "В файле config.json нет токена!")
            API_DELAY = config["API_DELAY"]
            EXCEL_TABLE_NAME = config["EXCEL_TABLE_NAME"]
            FOR_QUAL_INVESTOR = config["FOR_QUAL_INVESTOR"]
            AMORTIZATION = config["AMORTIZATION"]
            FLOATING_COUPON = config["FLOATING_COUPON"]
    except Exception as e:
        raise ValueError("parse_parameters_from_config::" + "Проблема чтения конфигурации: " + str(e))


parser = OptionParser()
parser.add_option("-c", "--clear", action="store_true", default=False, help="Не выводить в итоговой таблице"
                                                                            " компании без рейтингов")
(options, args) = parser.parse_args()
NOT_WRITE_WITHOUT_RATING = options.clear

try:
    parse_parameters_from_config()
    governmentBondObjects = []
    corporateBondsObjects = []
    download_bonds_info(governmentBondObjects, corporateBondsObjects)
    create_output_table(governmentBondObjects, corporateBondsObjects)
    open_output_table()
except Exception as ex:
    print(ex)
