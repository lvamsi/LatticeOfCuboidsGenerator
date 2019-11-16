"""
pk
dimension_cols
aggregated_fact_cols
"""
from com.samsung.locgenerator.dao.Table import Table


class LatticePoint(Table):
    def __init__(self):
        pass

    def set_create_query(self, create_query: str):
        self.create_query = create_query

    def set_insert_query(self, insert_query: str):
        self.insert_query = insert_query

    def set_dimension_cols(self, dimension_cols: list):
        self.dimension_cols = dimension_cols

    def set_aggregated_fact_cols(self, aggregated_fact_cols: list):
        self.aggregated_fact_cols = aggregated_fact_cols
