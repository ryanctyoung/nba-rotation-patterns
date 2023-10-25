environments = ['dev', 'prod']

current_env = environments[0]
def print_to_log(s):
    if current_env == environments[1]:
        print(s, file=open('../../log/log.txt', 'a'))
    else:
        print(s)
