# -*- coding: utf-8 -*-
from mrjob.job import MRJob

class symbol_count(MRJob):
    def mapper(self, _, line):
        data = line.strip().split(' ')
        line_id = int(data[0].strip())
        for i in range(1, len(data)):
            yield(data[i], line_id)
    
    def reducer(self, symbol, lines):
        # use the characteristics of set to solve the situation where a symbol occurs in
        # the same line multiple times
        yield(symbol, sorted(list(set(lines))))
        
if __name__ == '__main__':
    symbol_count.run()

