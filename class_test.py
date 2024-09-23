import pandas as pd
import numpy # noqa
import sqlite3 # noqa
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
        self.school_data = SchoolDataGenerator()
        self.school_data.generate_data()
        self.school_data.populate_db()

        self.conn = self.school_data.conn
        self.sql = self.school_data.sql

    def select_test(self, query):
        select = self.sql(query)
        expected_cols = ['student_id', 'first_name', 'last_name', 'grade_level', 'email'] # noqa

        if select is not None:
            if select.shape == (30, 5) and list(select.columns) == expected_cols: # noqa
                print("Select query was successful with the correct shape and columns") # noqa
            elif select.shape == (30, 5):
                print("Select query returned the correct shape but incorrect columns") # noqa
            else:
                print("Select query returned unexpected shape or columns")
        else:
            print("Select query was not successful")

    def where_test(self, query):
        where_result = self.sql(query)
        if where_result is not None:
            if all(where_result['grade_level'] == '12th'):
                print("WHERE query was successful: All rows have grade_level '12th'") # noqa
            else:
                incorrect_grades = where_result['grade_level'].unique()
                print(f"WHERE query returned rows with unexpected grade levels: {incorrect_grades}") # noqa
        else:
            print("WHERE query was not successful")

    def orderby_test(self, query, order='ASC'):
        orderby_result = self.sql(query).head()
        if orderby_result is not None:
            if order == 'ASC':
                expected_order = orderby_result['last_name'].sort_values(ascending=True) # noqa
            elif order == 'DESC':
                expected_order = orderby_result['last_name'].sort_values(ascending=False) # noqa
            else:
                print("Invalid order specified. Use 'ASC' or 'DESC'.")
                return
            if (orderby_result['last_name'].values == expected_order.values).all(): # noqa
                print(f"ORDER BY query was successful: First few rows are sorted by last_name in {order} order") # noqa
            else:
                incorrect_order = orderby_result['last_name'].tolist()
                print(f"ORDER BY query did not sort correctly. Incorrect order detected: {incorrect_order}") # noqa
        else:
            print("ORDER BY query was not successful")

    def multi_query_test(self, query):
        multi_result = self.sql(query).head()

        if multi_result is not None:
            if not all(multi_result['grade_level'] == '12th'):
                incorrect_grades = multi_result['grade_level'].unique().tolist() # noqa
                print(f"WHERE clause failed: Found grade levels other than '12th': {incorrect_grades}") # noqa

            expected_order = multi_result['last_name'].sort_values(ascending=True) # noqa
            if (multi_result['last_name'].values == expected_order.values).all(): # noqa
                print("Multi-query was successful: All rows have grade_level '12th' and are sorted by last_name") # noqa
            else:
                incorrect_order = multi_result['last_name'].tolist()
                print(f"ORDER BY clause failed: Incorrect order detected: {incorrect_order}") # noqa
        else:
            print("Multi-query was not successful")

    def left_join_test(self, query):
        left_join_result = self.sql(query)

        if left_join_result is not None:
            expected_columns = ['student_id', 'first_name', 'last_name', 'assignment_id', 'grade'] # noqa

            if left_join_result.shape == (300, 5):
                print("LEFT JOIN query shape is correct: (300, 5)")
            else:
                print(f"LEFT JOIN query failed: The result has an unexpected shape of {left_join_result.shape}") # noqa
                return

            actual_columns = list(left_join_result.columns)
            missing_columns = [col for col in expected_columns if col not in actual_columns] # noqa

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
                print(f"INNER JOIN query failed: The result has an unexpected shape of {inner_join_result.shape}") # noqa
                return

            actual_columns = list(inner_join_result.columns)
            missing_columns = [col for col in expected_columns if col not in actual_columns] # noqa

            if not missing_columns:
                print("INNER JOIN query contains all expected columns.")
            else:
                print(f"INNER JOIN query is missing columns: {missing_columns}") # noqa
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
                student_row['email'].strip() == 'student31@school.com'): # noqa
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
                student_row['email'].strip() == 'student31@school.com'): # noqa
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
        expected_cols = ['student_id', 'first_name', 'last_name', 'grade_level', 'grade'] # noqa

        if result is not None:
            if list(result.columns) == expected_cols:
                if (result['grade_level'] == '12th').all():
                    if result['grade'].isin(['A', 'B']).all():
                        print("Logical_operators query was successful with correct columns, grade_level, and grade") # noqa
                    else:
                        print("Logical_operators query returned incorrect grade values") # noqa
                else:
                    print("Logical_operators query returned incorrect grade_level values") # noqa
            else:
                print("Logical_operators query returned incorrect columns")
        else:
            print("Logical_operators query was not successful")

    def filter_query_test(self, query):
        result = sql(query)
        expected_cols = ['student_id', 'first_name', 'last_name', 'grade_level', 'grade'] # noqa

        if result is not None:
            # Check if the columns are correct
            if list(result.columns) == expected_cols:
                # Check if student_id values are between 1 and 7
                if result['student_id'].between(1, 7).all():
                    # Check if first_name starts with 'A' or 'J'
                    if result['first_name'].str.startswith(('A', 'J')).all():
                        # Check if grade is in ('A', 'B', 'C')
                        if result['grade'].isin(['A', 'B', 'C']).all():
                            print("filter_query was successful with correct columns, student_id, first_name, and grade") # noqa
                        else:
                            print("filter_query returned incorrect grade values") # noqa
                    else:
                        print("filter_query returned incorrect first_name values") # noqa
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

        assert provided_result is not None and not provided_result.empty, "Provided query returned no results" # noqa
        assert expected_result is not None and not expected_result.empty, "Internal query returned no results" # noqa

        expected_columns = ['grade_level', 'average_grade']
        assert list(provided_result.columns) == expected_columns, "Column names of the provided query do not match" # noqa

        pd.testing.assert_frame_equal(provided_result.sort_values('grade_level').reset_index(drop=True), # noqa
                                      expected_result.sort_values('grade_level').reset_index(drop=True), # noqa
                                      check_exact=True)

        print("Test passed successfully: Provided query matches the expected results") # noqa

    def test_gpa_query(self, query):
        assert query is not None and not query.empty, "Query returned no results" # noqa

        expected_columns = ['grade_level', 'average_grade']
        assert list(query.columns) == expected_columns, "Column names do not match" # noqa

        expected_data = {
            'grade_level': ['10th', '11th', '12th'],
            'average_grade': [2.170000, 2.175000, 1.916667]
        }
        expected_df = pd.DataFrame(expected_data)

        for index, row in query.iterrows():
            grade_level = row['grade_level']
            average_grade = row['average_grade']

            expected_row = expected_df[expected_df['grade_level'] == grade_level] # noqa
            if not expected_row.empty:
                expected_value = expected_row['average_grade'].values[0]
                assert abs(expected_value - average_grade) < 1e-6, f"Average grade for {grade_level} is incorrect" # noqa

        print("Test passed successfully")

    def case_expression_test(self, query):
        result = sql(query)
        expected_cols = ['first_name', 'last_name', 'grade', 'numeric_score', 'grade_check'] # noqa

        if result is not None:
            # Check if the columns are correct
            if list(result.columns) == expected_cols:
                # Check if grade_check matches the correct case conditions for each row # noqa
                correct_cases = (
                    ((result['numeric_score'] >= 90) & (result['grade_check'] == 'A')) | # noqa
                    ((result['numeric_score'] >= 80) & (result['numeric_score'] < 90) & (result['grade_check'] == 'B')) | # noqa
                    ((result['numeric_score'] >= 70) & (result['numeric_score'] < 80) & (result['grade_check'] == 'C')) | # noqa
                    ((result['numeric_score'] >= 60) & (result['numeric_score'] < 70) & (result['grade_check'] == 'D')) | # noqa
                    ((result['numeric_score'] < 60) & (result['grade_check'] == 'F')) # noqa
                )

                if correct_cases.all():
                    print("case_expression query was successful with correct columns and grade_check logic") # noqa
                else:
                    print("case_expression query returned incorrect grade_check values") # noqa
            else:
                print("case_expression query returned incorrect columns")
        else:
            print("case_expression query was not successful")

    def gpa_view_test(self, conn, query):
        conn.execute("DROP VIEW IF EXISTS StudentGPA;")
        conn.execute(query)
        test_query = "SELECT student_id, first_name, last_name, gpa FROM StudentGPA;" # noqa
        result = pd.read_sql(test_query, conn)

        if result is not None:
            expected_cols = ['student_id', 'first_name', 'last_name', 'gpa']

            if list(result.columns) == expected_cols:
                if result['gpa'].between(0, 100).all():
                    print("gpa_view was created successfully with correct columns and valid GPA values") # noqa
                else:
                    print("gpa_view returned invalid GPA values")
            else:
                print("gpa_view returned incorrect columns")
        else:
            print("gpa_view query was not successful")

    def report_card_view_test(self, conn, query):
        conn.execute("DROP VIEW IF EXISTS ReportCard;")
        conn.execute(query)  
        rc_query = "SELECT student_id, first_name, last_name, `Assignment 1`, `Assignment 2`, `Assignment 3`, avg_participation_grade, avg_behavior_grade FROM ReportCard;" # noqa
        result = pd.read_sql(rc_query, conn)

        if result is not None:
            expected_cols = ['student_id', 'first_name', 'last_name', 'Assignment 1', 'Assignment 2', 'Assignment 3', 'avg_participation_grade', 'avg_behavior_grade'] # noqa

            if list(result.columns) == expected_cols:
                score_columns = ['Assignment 1', 'Assignment 2', 'Assignment 3', 'avg_participation_grade', 'avg_behavior_grade'] # noqa
                result[score_columns] = result[score_columns].apply(pd.to_numeric, errors='coerce') # noqa

                valid_scores = result[score_columns].apply(lambda x: x.between(0, 100)).all().all() # noqa

                if valid_scores:
                    print("ReportCard view was created successfully with correct columns and valid scores") # noqa
                else:
                    print("ReportCard view returned invalid score values")
            else:
                print("ReportCard view returned incorrect columns")
        else:
            print("ReportCard view query was not successful")


# Example usage:
if __name__ == "__main__":
    query_tester = QueryTest()
