import time
import random

def handler(params,context):
    iterations = int(params["n"])
    inside_circle = 0
    
    for _ in range(iterations):
        x = random.random()
        y = random.random()
        if x**2 + y**2 <= 1.0:
            inside_circle += 1
            
    #formula di nonte carlo per pi greco
    pi_estimate = (inside_circle / iterations) * 4
    
    
    return pi_estimate