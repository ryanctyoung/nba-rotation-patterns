import json
import pandas as pd


if __name__ == '__main__':
    data = pd.read_csv('test.csv')
    data.to_json('test.json')
