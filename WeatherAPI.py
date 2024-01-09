from requests import Session
from WeatherAPI_config import api_config
import json
import pandas as pd
from datetime import datetime, timedelta

class weatherAPI:
    def __init__(self, session, api_key):
        self.session = session
        self.api_key = api_key
        self.base_url = 'http://api.weatherapi.com/v1'
        self.session.headers.update({'key': self.api_key})

    def ensure_session(self, session):
        if session is None:
            return self.session
        else:
            return session

    def get(self, session, endpoint, params = None):
        url = self.base_url + endpoint
        session = self.ensure_session(session)
        response = session.get(url = url, params = params)
        return response

class Weather:
    def __init__(self, session = None):
        if session is None:
            self.session = Session()
        else:
            self.session = session
        self.api_handler = weatherAPI(self.session, api_config.api_key)
    
    def get_current_weather(self, session = None, q = 'NE6'):
        response = self.api_handler.get(session = session, endpoint = '/current.json', params = {'q': q})
        data = json.loads(response.text)
        return pd.json_normalize(data)
    
    def get_historic_weather(self, session = None, q = 'NE6', dt = datetime.today().date() - timedelta(days=14)):
        params = {'q': q,
                  'dt': dt}
        response = self.api_handler.get(session = session, endpoint = '/history.json', params = params)
        data = json.loads(response.text)
        return pd.json_normalize(data)
    
    def get_weather_forecast(self, q, days = 7, session = None):
        params = {'q': q,
                  'days': days}
        response = self.api_handler.get(session = session, endpoint = '/forecast.json', params = params)
        data = json.loads(response.text)
        forecast_day = pd.json_normalize(data['forecast']['forecastday'], max_level=2)
        forecast_hour = pd.DataFrame()
        for day_hours in forecast_day['hour']:
            forecast_hour = pd.concat([forecast_hour, pd.json_normalize(day_hours)])
        #forecast_day.set_index('date', inplace = True)
        #forecast_hour.set_index('time', inplace = True)
        location = pd.json_normalize(data['location'])
        return {'location': location,
                'forecast_day': forecast_day,
                'forecast_hour': forecast_hour}

    def get_hourly_forecast(self, q, days, session = None):
        data = self.get_weather_forecast(q, days, session)
        data['forecast_hour'][data['location'].columns] = data['location'].iloc[0]
        return data['forecast_hour']

    def multi_location_weather(self, getter, qs, session = None, **kwargs):
        weather = pd.DataFrame()
        for q in qs:
            response = getter(session = session, q = q, **kwargs)
            weather = pd.concat([weather,response])
        return weather

    def export_weather_as_csv(self, data, file_path = None, filename = None):
        if filename is None:
            filename = 'weatherAPI_' + datetime.today().strftime('%Y_%m_%d %H-%M-%S') + '.csv'
        data.to_csv(filename,)