import time
from functools import wraps


def timethis(func):
    '''
    Decorator that reports the execution time.
    '''

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(func.__name__, end - start)
        return result

    return wrapper


@timethis
def countdown(n):
    while n > 0:
        n = n - 1


countdown(10000)

@timethis
def count(n):
    sum=0
    for i in range(1,n+1):
        sum=sum+i
    return sum
print(count(100))

count.__name__
count.__doc__
print(count.__wrapped__(100000))