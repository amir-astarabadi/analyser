from math import isnan

def dd(value):
    print(value)
    print(type(value))
    input()
    
def round_float(value):
    try:
        
        if isnan(value):
            return 0
        
        if value == int(value):
            return int(value)
        
        if value < -1 or value > 1:
            value = round(value, 2)
        
        return value.__float__()
    except:
        return value
