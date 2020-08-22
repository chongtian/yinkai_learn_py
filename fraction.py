import math

def add(n1, d1, n2, d2):
    d = d1 * d2 // math.gcd(d1,d2)
    n = n1 * d // d1 + n2 * d // d2
    s = math.gcd (d,n)
    if s != 1:
        d = d//s
        n = n//s
    return n,d

# test
print(add(2,4,1,6))
