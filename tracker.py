from json import loads
from requests import get
from sqlalchemy import create_engine
from sqlalchemy.types import Float, DateTime
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px


from prefect import task, Flow

from prefect.schedules import IntervalSchedule
from prefect.executors import DaskExecutor


# Extract
@task(max_retries=3, retry_delay=timedelta(seconds=5))
def extract_data():
    url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.json'
    print('Downloading from {}'.format(url))
    r = get(url)
    raw_data = loads(r.text)
    return raw_data


# Transform
@task
def get_global_vaccinated_percentage(ref_data):
    countries = dict()
    for country in ref_data:
        vaccine_data = extract_data_by_country(country, ref_data, args=[
            'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated'])
        country_percentages = get_vaccinated_percentage_by_country(
            vaccine_data[country])
        countries[country] = country_percentages
    df = pd.DataFrame.from_dict(countries).transpose()
    return df


# Load
@task
def store_percentages(percentages: pd.DataFrame):
    df = percentages.copy(deep=True)
    timestamp = datetime.utcnow() + timedelta(seconds=1)
    timestamps = []
    for i in range(len(df)):
        timestamps.append(timestamp)
    df.insert(0, 'date', timestamps)
    engine = create_engine('sqlite:///percentages.db', echo=False)
    with engine.begin() as connection:
        df.to_sql('percentages', con=connection,
                  if_exists='append', index=True, index_label='iso_code',
                  dtype={'date': DateTime(),
                         'vaccinated': Float(),
                         'fully_vaccinated': Float()})


@task
def create_chart(percentages: pd.DataFrame):
    fig = px.bar(percentages, labels={
        'index': 'Country',
        'value': 'Vaccines (millions)',
    }, title='Vaccination Rate Per Country')
    fig.show()


# Helpers
def extract_data_by_country(iso_code, ref_data, **item):
    if not item:
        return ref_data[iso_code]
    raw_region_data = dict()
    raw_region_data[iso_code] = dict()
    for arg in item['args']:
        raw_region_data[iso_code][arg] = ref_data.get(iso_code).get(arg)
    return raw_region_data


def get_vaccinated_percentage_by_country(raw_region_data):
    try:
        total_vaccinations = raw_region_data['total_vaccinations']
        people_vaccinated = raw_region_data['people_vaccinated']
        people_fully_vaccinated = raw_region_data['people_fully_vaccinated']
        percentage = {
            'vaccinated': round(people_vaccinated*100/total_vaccinations, 2),
            'fully_vaccinated': round(people_fully_vaccinated*100/total_vaccinations, 2)}
    except (KeyError, TypeError, ZeroDivisionError) as e:
        percentage = {'vaccinated': 0, 'fully_vaccinated': 0}
    return percentage


def main():
    extract_data_schedule = IntervalSchedule(
        start_date=datetime.utcnow() + timedelta(seconds=1),
        interval=timedelta(hours=12)
    )

    with Flow('Vacc Flow', schedule=extract_data_schedule) as etl_flow:
        raw_data = extract_data()
        vacc_percentages = get_global_vaccinated_percentage(raw_data)
        store_percentages(vacc_percentages)
        create_chart(vacc_percentages)

    etl_flow.run(executor=DaskExecutor())


if __name__ == '__main__':
    main()
