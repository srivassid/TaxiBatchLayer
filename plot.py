import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
pd.set_option('display.float_format', lambda x: '%.3f' % x)

pd.options.display.width = 0

class PlotData():

    def __init__(self):
        pass

    def plot_data(self):
        self.df = pd.read_csv('results/month_trips.csv/part-00000-90edf094-3c28-4b46-b190-656df34c0114-c000.csv')
        self.df['count_trip_time'] = self.df['count_trip_time'].astype(float)
        self.df['count_trip_time'] = self.df['count_trip_time'].apply(lambda x: format(x, 'f'))
        self.df['count_trip_time'] = self.df['count_trip_time'].astype(float)
        months = ['Oct', 'Jan', 'Nov', 'Sep', 'Feb', 'Dec', 'Aug']
        values = self.df['count_trip_time'].tolist()
        xlocs = [i + 1 for i in range(0, 10)]
        for i, v in enumerate(values):
            plt.text(xlocs[i] - 1.25, v + 1, str(v))

        plt.bar(months, values,color='lightgreen')
        plt.title('Total number of trips each month')
        plt.ylabel('No of trips')
        plt.xlabel('Months')
        plt.show()

    def heat_map(self):
        self.df = pd.read_csv('results/heat/part-00000-f5f28e3a-3d4d-49c8-a50a-8063fa2e50e4-c000.csv')
        print(self.df.head())
        self.df_dist = self.df.pivot_table(index=['month'],columns=['day'],values=['sum_trip_distance'])
        print(self.df_dist.head())
        sns.heatmap(self.df_dist)
        plt.title('HeatMap of distance travelled each day')
        plt.show()

        self.df_time = self.df.pivot_table(index=['month'], columns=['day'], values=['sum_trip_time'])
        plt.title('HeatMap of time spent travelling each day')
        sns.heatmap(self.df_time)
        plt.show()

if __name__ == '__main__':
    pld = PlotData()
    pld.plot_data()
    pld.heat_map()