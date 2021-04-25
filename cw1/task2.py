# -*- coding: utf-8 -*-
from mrjob.job import MRJob, MRStep

class age_count(MRJob):
    def mapper(self, _, line):
        data = line.split(", ")
        age = data[0].strip()
        yield(age, 1)
        
    # compute age counts
    def reducer1(self, age, counts):
        yield(None, (sum(counts), age))
    
    # top-10 values
    def reducer2(self, _, list_of_values):
        list_of_values = sorted(list(list_of_values), reverse=True)
        return list_of_values[:10]
        
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]
    
if __name__ == "__main__":
    age_count.run()
