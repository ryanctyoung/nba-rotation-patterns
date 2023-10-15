import pandas as pd
import numpy as np

seconds_per_game = 60 * 12 * 4


def string_to_int_list(string):
    if string == '[]':
        return []
    subs_strings = string.strip('][').split(', ')
    return list(map(lambda a: int(a), subs_strings))

if __name__ == '__main__':
    data = pd.read_csv('../../data/test.csv');
    subs = data.loc[:, 'SUBS'].map(string_to_int_list)
    # time_axis = np.zeros(seconds_per_game)

    i = 0
    active_time = []
    while i < len(subs) - 1:
        for j in range(subs[i], subs[i + 1]):
            active_time.append(j)
        i += 2

    data = {
            'SUBS': [active_time]
            }
    display_df = pd.DataFrame(data)