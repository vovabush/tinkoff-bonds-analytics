# Что это
Репозиторий содержит скрипт для определения текущих показателей облигаций, доступных конкретному клиенту Тинькофф. Скрипт агрегирует данные из самого Тинькоффа, а также берет рейтинги из АКРА, НКР и НРА.

# Как этим пользоваться
1. Выпустить собственный токен в Тинькофф.Инвестиции: https://tinkoff.github.io/investAPI/token/ ;
2. Скачать репозиторий;
3. Добавить в config.json выпущенный токен и подкорректировать остальные параметры;
4. Установить необходимые библиотеки из requirements.txt: `python -m pip install -r requirements.txt` (возможно, понадобится https://visualstudio.microsoft.com/visual-cpp-build-tools/ );
5. Проверить наличие Excel;
6. Запустить при помощи: `python bondsList.py`. Можно использовать ключ "-c", тогда в итоговой таблице не будут выведены облигации эмитентов, не имеющих рейтинговых оценок ни в одном из рейтинговых агентств: АРКА, НРА, НКР.

По окончанию работы откроется Excel файл, в котором два листа: с государственными и корпоративными облигациями. Данные на листах сортируются по доходности. 

На каждом листе располагаются следующие столбцы:
- **Имя**: название инструмента;
- **Тикер**: краткое название в биржевой информации инструмента;
- **Цена + НКД**: текущая рыночная стоимость инструмента, учитывающая и рыночную цену, и накопленный купонный доход;
- **Годовая доходность**: выражена в годовых процентах, вычисляемая как отношение суммы выплачиваемых за год купонов за вычетом налога к цене + НКД (не учитывает комиссию и разницу между стоимостью покупки и погашения);
- **Купонов в год**: количество выплачиваемых купонов в год по инструменту;
- **Лет до погашения**: количество лет до погашения инструмента эмитентом;
- **Дюрация**: параметр, исходя из ожиданий по инфляции, подробнее см. тут: https://smart-lab.ru/blog/703874.php ;
- **Рейтинг (АКРА)**: текущий рейтинг инструмента по данным АКРА.
- **Рейтинг (НРА)**: текущий рейтинг инструмента по данным НРА.
- **Рейтинг (НКР)**: текущий рейтинг инструмента по данным НКР.

# Что понадобиться
- Excel;
- Библиотеки python из requirements.txt;
- Личный кабинет в Тинькофф.Инвестиции;
- Интернет.

# Описание файлов в репозитории
- bondsList.py - главный скрипт;
- config.json - файл конфигурации, который содержит следующие поля:
	- TOKEN - персонализированный токен, выпустить и почитать о котором можно здесь: https://tinkoff.github.io/investAPI/token/ ;
	- API_DELAY - задержка в секунда между запросами. Тинькофф ограничивает в 100-300 запросов в минуту, соответственно этот параметр в пределах 0.2 - 0.5;
	- EXCEL_TABLE_NAME - имя выходного Excel файла;
	- FOR_QUAL_INVESTOR - флаг, включать ли облигации для квалифицированных инвесторов:
		- True - включать,
		- False - не включать;
	- AMORTIZATION - флаг, включать ли облигации с амортизацией:
		- True - включать,
		- False - не включать;
	- FLOATING_COUPON - флаг, включать ли облигации с плавающим купоном (1):
		- True - включать,
		- False - не включать;
- requirements.txt - набор дополнительных библиотек для установки перед запуском;
- Readme.md - это я;
- bonds.xlsx - пример выходного файла.

# Нюансы
1. У Тинькоффа плохо работает backend на тему плавающих купонов, описание и запрос на исправление здесь: https://github.com/Tinkoff/invest-python/issues/190
2. С радостью бы добавил рейтинг и из Тинькоффа, но у них и это не работает, описание и запрос на исправление здесь: https://github.com/Tinkoff/invest-python/issues/189
3. Иногда АКРА начинает возмущаться количеству запросов к ней. В этом случае рекомендуется увеличить параметр API_DELAY в config.json.
4. Флаги AMORTIZATION и FLOATING_COUPON добавлены на будущее и пока что не работают.
5. В базе данных НРА почему-то бывают неправильные ИНН. Это приводит к тому, что некоторым компаниям могут быть приписаны не их рейтинги. Они об этом проинформированы, ожидаю ответа. В большинстве рейтинги соответствуют компаниям, но необходимость ручной проверки в случае сомнений не отменяется.
