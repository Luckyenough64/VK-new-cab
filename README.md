# VK-new-cab
Получаем данные по кампаниям из нового кабинета VK ADS и радуемся жизни

## Шаг 0.5

Устанавливаем библиотеки и зависимости к ним, если необходимо

~~~
import pandas as pd
from pandas import json_normalize
import requests
import re
~~~

## Шаг 0.75

Получаем от поддержки txt'шник с данными - для простоты я нашёл способ просто подгрузив его выцепить всё необходимое - это будет работать в том случае, если вы оставили файл "as is" и ничего в нём не меняли.

~~~
credentials = pd.read_table("C:\\путь к txt файлу, который вы получили от поддержки VK.txt", header=None, names=['Message'])

client_id = credentials.loc[credentials['Message'].str.contains('Client ID'), 'Message'].values[0].split(': ')[1]
client_secret = credentials.loc[credentials['Message'].str.contains('Client secret'), 'Message'].values[0].split(': ')[1]
~~~

## Шаг 0.95

Получаем токен доступа (ALЯRM! Данный скрипт делает вечный токен, а согласно документации ВК вы можете завести только 5 (вроде) токенов на аккаунт - просто предупреждение, что не надо выполнять этот скрипт несколько раз, иначе будете (как я) делать запросы на очистку токенов)

~~~
# Создание параметров для запроса на получение токена
token_params = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'permanent': 'true'  # Добавляем параметр для получения вечного токена
}

# Отправка запроса на получение токена
response = requests.post('https://ads.vk.com/api/v2/oauth2/token.json', data=token_params)

if response.status_code == 200:
    token_data = response.json()
    access_token = token_data['access_token']
    print('Access Token:', access_token)
else:
    print('Ошибка при получении токена доступа:', response.text)
~~~

## Шаг 1

Получим статистику по нашему кабинету ПО ДНЯМ в разрезе групп

~~~
date_from = '2023-09-01'
date_to = '2023-09-11'

url = f'https://ads.vk.com/api/v2/statistics/ad_groups/day.json?date_from={date_from}&date_to={date_to}&metrics=all'

headers = {
    'Authorization': f'Bearer {access_token}'
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    items = data['items']

    # Преобразование данных в DataFrame
    df = pd.json_normalize(items)

    # Развертывание JSON-данных в столбцы
    json_columns = ['rows']
    for column in json_columns:
        json_data = json_normalize(df[column].explode()).add_prefix(f'{column}.')
        df = df.join(json_data)
        df = df.drop(columns=[column])
else:
    print('Ошибка при выполнении запроса:', response.status_code)

df = df.sort_values(by='total.base.spent', ascending=False)
~~~

## Шаг 2

Получим параметры, нужные нам - мне были нужны UTM-ки, объявления и их названия, и я их получил

~~~
fields = 'id,ad_plan_id,name,utm,banners,delivery'

offset = 0
limit = 100
df_ad_group_stats = []

base_url = f'https://ads.vk.com/api/v2/ad_groups.json?fields={fields}&offset={offset}&limit={limit}'

while True:
    # Отправьте GET-запрос для получения данных о рекламных группах
    response = requests.get(base_url, headers=headers)

    # Проверьте статус-код ответа
    if response.status_code == 200:
        data = response.json()
        items = data['items']

        # Добавьте полученные данные в список
        df_ad_group_stats.extend(items)

        # Проверьте, есть ли еще данные для получения
        if len(items) < limit:
            break
        else:
            # Увеличьте смещение для следующей страницы
            offset += limit
            base_url = f'https://ads.vk.com/api/v2/ad_groups.json?fields={fields}&offset={offset}&limit={limit}'
    else:
        print('Ошибка во время запроса:', response.status_code)
        break

df_ad_group_stats = pd.DataFrame(df_ad_group_stats)
~~~

## Шаг 3

Пишем и применяем функцию для получения UTM-параметров и разбиваем их. Данный шаг, в принципе, опционален, но кому-то он может пригодиться.

~~~
def extract_utm_params(utm):
    utm_params = {}
    if utm and isinstance(utm, str):
        # Используем регулярное выражение для поиска UTM-параметров
        utm_matches = re.findall(r'utm_([^=&]+)=([^&]+)', utm)
        for match in utm_matches:
            param_name, param_value = match
            utm_params[param_name] = param_value
    return utm_params


# Применяем функцию к столбцу 'utm'
df_ad_group_stats['utm_params'] = df_ad_group_stats['utm'].apply(extract_utm_params)

df_ad_group_stats['utm_source'] = df_ad_group_stats['utm_params'].apply(lambda x: x.get('source', None))
df_ad_group_stats['utm_medium'] = df_ad_group_stats['utm_params'].apply(lambda x: x.get('medium', None))
df_ad_group_stats['utm_campaign'] = df_ad_group_stats['utm_params'].apply(lambda x: x.get('campaign', None))
df_ad_group_stats['utm_id'] = df_ad_group_stats['utm_params'].apply(lambda x: x.get('id', None))
df_ad_group_stats['utm_content'] = df_ad_group_stats['utm_params'].apply(lambda x: x.get('content', None))

df_ad_group_stats.drop('utm_params', axis=1, inplace=True)
~~~

## Шаг 3.5

Джойним датафреймы
~~~
df = df.merge(df_ad_group_stats, how='left', left_on='id', right_on='id')
~~~

## Шаг 4  

Получаем названия рекламных кампаний и их айдишники (опять же, нужно было лично мне)
~~~
base_plans_url = 'https://ads.vk.com/api/v2/ad_plans.json'

all_ad_plans_data = []

while True:
    # Формирование URL с учетом параметров пагинации
    url = f'{base_plans_url}?offset={offset}&limit={limit}'

    # Отправка GET-запроса и получение ответа
    response = requests.get(url, headers=headers)

    # Проверка успешности запроса
    if response.status_code == 200:
        data = response.json()
        items = data['items']

        # Создание списка словарей с данными
        ad_plans_data = []

        for item in items:
            ad_plan = {
                'Ad Plan ID': item['id'],
                'Name': item['name'],
            }
            ad_plans_data.append(ad_plan)

        all_ad_plans_data.extend(ad_plans_data)

        # Увеличение смещения для следующей порции данных
        offset += limit

        # Если больше нет данных, завершаем цикл
        if not items:
            break
    else:
        print('Ошибка при выполнении запроса:', response.status_code)
        break

df_plans = pd.DataFrame(all_ad_plans_data)

~~~

## Шаг 4.5

Джойним датафреймы ещё раз
~~~
df = df.merge(df_plans, how='left', left_on='ad_plan_id', right_on='Ad Plan ID')
~~~

## Шаг 5

Наводим красоту либо так (если вам нужны все 200+ колонок)
~~~
new_column_names = {
    'ad_plan_id': 'ID кампании',
    'Name': 'Название кампании',
    'name': 'Название группы',
    'utm_source': 'utm_source',
    'utm_medium': 'utm_medium',
    'utm_campaign': 'utm_campaign',
    'utm_id': 'utm_id',
    'utm_content': 'utm_content',
    'id': 'Номер группы',
    'rows.date': 'Дата',
    'total.base.shows': 'Показы',
    'total.base.clicks': 'Клики',
    'total.base.goals': 'Достижение целей',
    'total.base.spent': 'Потрачено',
    'total.base.cpm': 'CPM',
    'total.base.cpc': 'CPC',
    'total.base.cpa': 'CPA',
    'total.base.ctr': 'CTR',
    'total.uniques.total': 'Показов уникальным пользователям',
    'total.uniques.frequency': 'Суточная частота показа',
    'total.uniques.increment': 'Прирост количества уникальных пользователей',
    'total.romi.romi': 'ROMI'
}

df = df.rename(columns=new_column_names)

print(df)
~~~

ИЛИ ТАК, если хотите отфильтровать колонки:
~~~
df = df[['ad_plan_id',
        'Name',
        'name', 
        'utm_source',
        'utm_medium',
        'utm_campaign',
        'utm_id',
        'utm_content',
        'id', 
        'rows.date', 
        'total.base.shows', 
        'total.base.clicks', 
        'total.base.goals', 
        'total.base.spent', 
        'total.base.cpm', 
        'total.base.cpc', 
        'total.base.cpa', 
        'total.base.ctr', 
        'total.uniques.total', 
        'total.uniques.frequency', 
        'total.uniques.increment', 
        'total.romi.romi']]
~~~

## Результат

Вы обмазались данными из кабинета VK и не обляпались вкуснотищей в виде документации к новому кабинету. Ваши спасибо и пожалуйста можете сказать [в телеге](https://t.me/luckyenough/ "телега") или по [электронной почте](mailto:yegor2010yy@yahoo.com "e-мыло"), но нет ничего слаще, чем лишняя соточка [на Tinkoff](https://www.tinkoff.ru/rm/lukyanov.egor5/82all28131/ "Тинёк").

### Нюансы

Скрипт можно (и нужно доработать), и я  даже очень знаю как:

- Группировка и оптимизация URL.

Соединить их воедино где-то таким образом:

~~~
base_ads_url = 'https://ads.vk.com/api/v2/statistics/ad_groups/day.json'
base_groups_url = 'https://ads.vk.com/api/v2/ad_groups.json'
base_plans_url = 'https://ads.vk.com/api/v2/ad_plans.json'
~~~
- Сделать функции для бытовых нужд
~~~
# Функция для отправки запроса и обработки ошибок
def send_request(url):
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print('Ошибка при выполнении запроса:', response.status_code)
        return None
    return response.json()
~~~
- Упрощение мерзких циклов

...и так далее и тому подобное
