from math import isnan

def dd(value):
    print(value)
    print(type(value))
    input()
    
def round_float(value:float):
    try:
        if isnan(value):
            return 0
        if -1 < value < 1:
            return value
        return round(value, 2)
    except:
        return value
