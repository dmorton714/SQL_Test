import pandas as pd
import numpy
import sqlite3
from db_builder import SchoolDataGenerator

# Instantiate the class
school_data = SchoolDataGenerator()

# Generate data & create the database
school_data.generate_data()
school_data.populate_db()

# Global variable 
sql = school_data.sql
conn = school_data.conn

class QueryTest:
    def __init__(self):
        # Initialize the SchoolDataGenerator and populate the database
        self.school_data = SchoolDataGenerator()
        self.school_data.generate_data()
        self.school_data.populate_db()

        # Store the connection and SQL method globally
        self.conn = self.school_data.conn
        self.sql = self.school_data.sql

    def select_test(self, query):
        select = self.sql(query)
        expected_cols = ['student_id', 'first_name', 'last_name', 'grade_level', 'email']

        if select is not None:
            if select.shape == (30, 5) and list(select.columns) == expected_cols:
                print("Select query was successful with the correct shape and columns")
            elif select.shape == (30, 5):
                print("Select query returned the correct shape but incorrect columns")
            else:
                print("Select query returned unexpected shape or columns")
        else:
            print("Select query was not successful")

    def where_test(self, query):
        where_result = self.sql(query)
        if where_result is not None:
            if all(where_result['grade_level'] == '12th'):
                print("WHERE query was successful: All rows have grade_level '12th'")
            else:
                incorrect_grades = where_result['grade_level'].unique()
                print(f"WHERE query returned rows with unexpected grade levels: {incorrect_grades}")
        else:
            print("WHERE query was not successful")

    def orderby_test(self, query, order='ASC'):
        orderby_result = self.sql(query).head()
        if orderby_result is not None:
            if order == 'ASC':
                expected_order = orderby_result['last_name'].sort_values(ascending=True)
            elif order == 'DESC':
                expected_order = orderby_result['last_name'].sort_values(ascending=False)
            else:
                print("Invalid order specified. Use 'ASC' or 'DESC'.")
                return
            if (orderby_result['last_name'].values == expected_order.values).all():
                print(f"ORDER BY query was successful: First few rows are sorted by last_name in {order} order")
            else:
                incorrect_order = orderby_result['last_name'].tolist()
                print(f"ORDER BY query did not sort correctly. Incorrect order detected: {incorrect_order}")
        else:
            print("ORDER BY query was not successful")

    def multi_query_test(self, query):
        multi_result = self.sql(query).head()

        if multi_result is not None:
            if not all(multi_result['grade_level'] == '12th'):
                incorrect_grades = multi_result['grade_level'].unique().tolist()
                print(f"WHERE clause failed: Found grade levels other than '12th': {incorrect_grades}")

            expected_order = multi_result['last_name'].sort_values(ascending=True)
            if (multi_result['last_name'].values == expected_order.values).all():
                print("Multi-query was successful: All rows have grade_level '12th' and are sorted by last_name")
            else:
                incorrect_order = multi_result['last_name'].tolist()
                print(f"ORDER BY clause failed: Incorrect order detected: {incorrect_order}")
        else:
            print("Multi-query was not successful")

    def left_join_test(self, query):
        left_join_result = self.sql(query)

        if left_join_result is not None:
            expected_columns = ['student_id', 'first_name', 'last_name', 'assignment_id', 'grade']

            if left_join_result.shape == (300, 5):
                print("LEFT JOIN query shape is correct: (300, 5)")
            else:
                print(f"LEFT JOIN query failed: The result has an unexpected shape of {left_join_result.shape}")
                return

            actual_columns = list(left_join_result.columns)
            missing_columns = [col for col in expected_columns if col not in actual_columns]

            if not missing_columns:
                print("LEFT JOIN query contains all expected columns.")
            else:
                print(f"LEFT JOIN query is missing columns: {missing_columns}")
        else:
            print("LEFT JOIN query was not successful")

    def inner_join_test(self, query):
        inner_join_result = self.sql(query)

        if inner_join_result is not None:
            expected_columns = ['first_name', 'last_name', 'grade']

            if inner_join_result.shape == (300, 3):
                print("INNER JOIN query shape is correct: (300, 3)")
            else:
                print(f"INNER JOIN query failed: The result has an unexpected shape of {inner_join_result.shape}")
                return

            actual_columns = list(inner_join_result.columns)
            missing_columns = [col for col in expected_columns if col not in actual_columns]

            if not missing_columns:
                print("INNER JOIN query contains all expected columns.")
            else:
                print(f"INNER JOIN query is missing columns: {missing_columns}")
        else:
            print("INNER JOIN query was not successful")
    
    def insert_test(self, query):
        student_31_check = sql("SELECT * FROM students WHERE student_id = 31")

        if student_31_check is not None and not student_31_check.empty:
            student_row = student_31_check.iloc[0]

            if (student_row['student_id'] == 31 and
                student_row['first_name'].strip() == 'John' and
                student_row['last_name'].strip() == 'Doe' and
                int(student_row['grade_level']) == 12 and
                student_row['email'].strip() == 'student31@school.com'):
                print("INSERT query was successful")
            else:
                print("INSERT query returned unexpected data")
                print("Actual data:", student_row)
        else:
            print("INSERT query was not successful")

    def delete_test(self, query):
        student_31_check = sql("SELECT * FROM students WHERE student_id = 31")

        if student_31_check is None or student_31_check.empty:
            print("DELETE query was successful")
        else:
            print("DELETE query failed")
            print("Student still exists:", student_31_check)

    def update_query_test(self, query):
        student_31_check = sql("SELECT * FROM students WHERE student_id = 31")
        if student_31_check is not None and not student_31_check.empty:
            student_row = student_31_check.iloc[0]

            if (student_row['student_id'] == 31 and
                student_row['first_name'].strip() == 'Jane' and
                student_row['last_name'].strip() == 'Doe' and
                int(student_row['grade_level']) == 12 and
                student_row['email'].strip() == 'student31@school.com'):
                print("UPDATE query was successful")
                return
            else:
                print("UPDATE query returned unexpected data")
                print("Actual data:", student_row)
        else:
            print("UPDATE query was not successful")

    def delete_query(self):
        delete_query = """
      DELETE FROM students WHERE student_id = 31
      """

        conn.execute(delete_query)
        conn.commit()
        return print("Student 31 has been removed")
    
    def logical_operators_test(self, query):
        result = sql(query)
        expected_cols = ['student_id', 'first_name', 'last_name', 'grade_level', 'grade']

        if result is not None:
            # Check if the columns are correct
            if list(result.columns) == expected_cols:
                # Check if all grade_level values are '12th'
                if (result['grade_level'] == '12th').all():
                    # Check if all grade values are 'A' or 'B'
                    if result['grade'].isin(['A', 'B']).all():
                        print("Logical_operators query was successful with correct columns, grade_level, and grade")
                    else:
                        print("Logical_operators query returned incorrect grade values")
                else:
                    print("Logical_operators query returned incorrect grade_level values")
            else:
                print("Logical_operators query returned incorrect columns")
        else:
            print("Logical_operators query was not successful")

    def filter_query_test(self, query):
        result = sql(query)
        expected_cols = ['student_id', 'first_name', 'last_name', 'grade_level', 'grade']

        if result is not None:
            # Check if the columns are correct
            if list(result.columns) == expected_cols:
                # Check if student_id values are between 1 and 7
                if result['student_id'].between(1, 7).all():
                    # Check if first_name starts with 'A' or 'J'
                    if result['first_name'].str.startswith(('A', 'J')).all():
                        # Check if grade is in ('A', 'B', 'C')
                        if result['grade'].isin(['A', 'B', 'C']).all():
                            print("filter_query was successful with correct columns, student_id, first_name, and grade")
                        else:
                            print("filter_query returned incorrect grade values")
                    else:
                        print("filter_query returned incorrect first_name values")
                else:
                    print("filter_query returned incorrect student_id values")
            else:
                print("filter_query returned incorrect columns")
        else:
            print("filter_query was not successful")

    def groupby_query_test(self, query):
        internal_query = """
        SELECT
            students.grade_level,
            AVG(grades.numeric_score) AS average_grade
        FROM
            students
        INNER JOIN grades ON students.student_id = grades.student_id
        GROUP BY
            students.grade_level;
        """
        
        provided_result = sql(query)
        expected_result = sql(internal_query)

        assert provided_result is not None and not provided_result.empty, "Provided query returned no results"
        assert expected_result is not None and not expected_result.empty, "Internal query returned no results"

        expected_columns = ['grade_level', 'average_grade']
        assert list(provided_result.columns) == expected_columns, "Column names of the provided query do not match"

        pd.testing.assert_frame_equal(provided_result.sort_values('grade_level').reset_index(drop=True), 
                                      expected_result.sort_values('grade_level').reset_index(drop=True), 
                                      check_exact=True)

        print("Test passed successfully: Provided query matches the expected results")




# Example usage:
if __name__ == "__main__":
    query_tester = QueryTest()
