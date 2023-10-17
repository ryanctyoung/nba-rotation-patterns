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

    def iter_through_performances(performance):
        i = 0
        active_time = []
        while i < len(performance) - 1:
            for j in range(performance[i], performance[i + 1]):
                active_time.append(j)
            i += 2
        return active_time

    subs = subs.map(iter_through_performances)
    data['SUBS'] = subs
