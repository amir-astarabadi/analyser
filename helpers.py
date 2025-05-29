from math import isnan
from scipy.stats import gaussian_kde
from numpy import array as np_array
from numpy import linspace as np_linspace

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

def density_curve(data):
    density_curve_data = np_array(data)
    density_curve_len = len(density_curve_data)
    if density_curve_len < 2:
        return []
    density_curve_len = density_curve_len if density_curve_len < 100 else 100
    kde = gaussian_kde(density_curve_data)
    density_curve_x = np_linspace(min(density_curve_data), max(density_curve_data), density_curve_len)
    density_curve_y = kde(density_curve_x)
    return [[x.__float__(),y.__float__()] for x,y  in zip(density_curve_x, density_curve_y)]
