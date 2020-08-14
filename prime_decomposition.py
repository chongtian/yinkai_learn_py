# Prime Decomposition
import math
from random import *
x = randint(10000000, 1000000000)
x= 23232
print(x)
count = 0
i = 2
primes = []
while(i <= math.sqrt(x)):
    if(x % i == 0):
        primes.append(i)
        x /= i
    else:
        i += 1
    count += 1
primes.append(int(x))
print(primes)
print(count)
