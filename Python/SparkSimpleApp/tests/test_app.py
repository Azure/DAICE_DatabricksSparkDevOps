from sparksimpleapp import rdd_csv_col_count


class mockRDD(object):
    def __init__(self, values):
        self.values = values
    
    def map(self, func):
        return map(func, self.values) 

def test_rdd_csv_col_count():
    rdd = mockRDD(['a,b', 'c', 'd,e,f,'])
    expected = [2,1,4]
    results = rdd_csv_col_count(rdd)
    assert all([x == y[0] for x,y in zip(expected, results)])