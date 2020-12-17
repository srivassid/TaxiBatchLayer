from elasticsearch import Elasticsearch, helpers
import pandas as pd

class InsertDataToES():

    def __init__(self):
        pass

    def push_data(self):
        self.es = Elasticsearch()
        self.df = pd.read_csv('results/month_time.csv/part-00000-88189e97-f7c4-4e04-adbd-d39d31c0f308-c000.csv')
        self.df['sum_trip_time'] = self.df['sum_trip_time'].apply(lambda x:format(x,'f'))
        print(self.df)

        self.data = []
        for i, row in self.df.iterrows():
            # print(row)
            self.data.append({
                "_index": 'month_time',
                "type": 'taxidata',
                "_source": {
                    'month': row['month'],
                    'time': row['sum_trip_time']
                }
            })
        print(self.data)
        helpers.bulk(self.es, self.data)
        print("data inserted")

if __name__ == '__main__':
    es = InsertDataToES()
    es.push_data()