import json
import pandas as pd


if __name__ == '__main__':
    data = pd.read_json('test.json')
    data.to_csv('test.csv')
