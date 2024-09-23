import pandas as pd
import numpy as np
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

def select_test():
    select = sql(select_query)
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


def where_test():
    where_result = sql(where_query)
    if where_result is not None:
        if all(where_result['grade_level'] == '12th'):
            print("WHERE query was successful: All rows have grade_level '12th'")
        else:
            incorrect_grades = where_result['grade_level'].unique()
            print(f"WHERE query returned rows with unexpected grade levels: {incorrect_grades}")
    else:
        print("WHERE query was not successful")


def orderby_test(order='ASC'):
    orderby_result = sql(orderby_query).head()
    if orderby_result is not None:
        if order == 'ASC':
            expected_order = orderby_result['last_name'].sort_values(ascending=True)
        elif order == 'DESC':
            expected_order = orderby_result['last_name'].sort_values(ascending=False)
        else:
            print("Invalid order specified. Use 'ASC' or 'DESC'.")
        if (orderby_result['last_name'].values == expected_order.values).all():
            print(f"ORDER BY query was successful: First few rows are sorted by last_name in {order} order")
        else:
            incorrect_order = orderby_result['last_name'].tolist()
            print(f"ORDER BY query did not sort correctly. Incorrect order detected in the first few rows: {incorrect_order}")
    else:
        print("ORDER BY query was not successful")


def multi_query_test():
    multi_result = sql(multi_query).head()

    if multi_result is not None:
        if not all(multi_result['grade_level'] == '12th'):
            incorrect_grades = multi_result['grade_level'].unique().tolist()
            print(f"WHERE clause failed: Found grade levels other than '12th': {incorrect_grades}")

        expected_order = multi_result['last_name'].sort_values(ascending=True)
        if (multi_result['last_name'].values == expected_order.values).all():
            print("Multi-query was successful: All rows have grade_level '12th' and are sorted by last_name in ascending order")
        else:
            incorrect_order = multi_result['last_name'].tolist()
            print(f"ORDER BY clause failed: Incorrect order detected in the first few rows: {incorrect_order}")
    else:
        print("Multi-query was not successful")


def left_join_test():
    left_join_result = sql(left_join_query)

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


def inner_join_test():
    inner_join_result = sql(inner_join_query)

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


def insert_test():
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


def delete_test():
    student_31_check = sql("SELECT * FROM students WHERE student_id = 31")

    if student_31_check is None or student_31_check.empty:
        print("DELETE query was successful")
    else:
        print("DELETE query failed")
        print("Student still exists:", student_31_check)


def insert_query():
  insert_query = """
INSERT INTO students (student_id, first_name, last_name, grade_level, email)
VALUES (31, 'John', 'Doe', 12, 'student31@school.com')
"""

  conn.execute(insert_query)
  conn.commit()
  return insert_query


def update_query_test():
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


def delete_query():
  delete_query = """
DELETE FROM students WHERE student_id = 31
"""

  conn.execute(delete_query)
  conn.commit()
  return print("Student 31 has been removed")


def logical_operators_test():
    result = sql(Logical_operators)
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


def filter_query_test():
    result = sql(filter_query)
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


def groupby_query_test(groupby_query):
    query = """
    SELECT
        students.grade_level,
        AVG(grades.numeric_score) AS average_grade
    FROM
        students
    INNER JOIN grades ON students.student_id = grades.student_id
    GROUP BY
        students.grade_level;
    """
    result = sql(query)

    assert result is not None and not result.empty, "Query returned no results"

    expected_columns = ['grade_level', 'average_grade']
    assert list(result.columns) == expected_columns, "Column names do not match"

    grouped_levels = result['grade_level'].unique()
    for level in grouped_levels:
        level_data = result[result['grade_level'] == level]
        expected_average = level_data['average_grade'].mean()
        actual_average = level_data['average_grade'].iloc[0]
        assert abs(expected_average - actual_average) < 1e-6, f"Average grade for {level} is incorrect"

    print("Test passed successfully")


def test_gpa_query(gpa_result):
    assert gpa_result is not None and not gpa_result.empty, "Query returned no results"

    expected_columns = ['grade_level', 'average_grade']
    assert list(gpa_result.columns) == expected_columns, "Column names do not match"

    expected_data = {
        'grade_level': ['10th', '11th', '12th'],
        'average_grade': [2.170000, 2.175000, 1.916667]
    }
    expected_df = pd.DataFrame(expected_data)

    for index, row in gpa_result.iterrows():
        grade_level = row['grade_level']
        average_grade = row['average_grade']

        expected_row = expected_df[expected_df['grade_level'] == grade_level]
        if not expected_row.empty:
            expected_value = expected_row['average_grade'].values[0]
            assert abs(expected_value - average_grade) < 1e-6, f"Average grade for {grade_level} is incorrect"

    print("Test passed successfully")


def case_expression_test():
    result = sql(case_expression)
    expected_cols = ['first_name', 'last_name', 'grade', 'numeric_score', 'grade_check']

    if result is not None:
        # Check if the columns are correct
        if list(result.columns) == expected_cols:
            # Check if grade_check matches the correct case conditions for each row
            correct_cases = (
                ((result['numeric_score'] >= 90) & (result['grade_check'] == 'A')) |
                ((result['numeric_score'] >= 80) & (result['numeric_score'] < 90) & (result['grade_check'] == 'B')) |
                ((result['numeric_score'] >= 70) & (result['numeric_score'] < 80) & (result['grade_check'] == 'C')) |
                ((result['numeric_score'] >= 60) & (result['numeric_score'] < 70) & (result['grade_check'] == 'D')) |
                ((result['numeric_score'] < 60) & (result['grade_check'] == 'F'))
            )

            if correct_cases.all():
                print("case_expression query was successful with correct columns and grade_check logic")
            else:
                print("case_expression query returned incorrect grade_check values")
        else:
            print("case_expression query returned incorrect columns")
    else:
        print("case_expression query was not successful")


def gpa_view_test(conn):
    conn.execute("DROP VIEW IF EXISTS StudentGPA;")
    conn.execute(gpa_view)
    query = "SELECT student_id, first_name, last_name, gpa FROM StudentGPA;"
    result = pd.read_sql(query, conn)

    if result is not None:
        expected_cols = ['student_id', 'first_name', 'last_name', 'gpa']

        if list(result.columns) == expected_cols:
            if result['gpa'].between(0, 100).all():
                print("gpa_view was created successfully with correct columns and valid GPA values")
            else:
                print("gpa_view returned invalid GPA values")
        else:
            print("gpa_view returned incorrect columns")
    else:
        print("gpa_view query was not successful")


def report_card_view_test(conn):
    conn.execute("DROP VIEW IF EXISTS ReportCard;")
    conn.execute(report_card_view)  # Use your ReportCard SQL string here
    query = "SELECT student_id, first_name, last_name, `Assignment 1`, `Assignment 2`, `Assignment 3`, avg_participation_grade, avg_behavior_grade FROM ReportCard;"
    result = pd.read_sql(query, conn)

    if result is not None:
        expected_cols = ['student_id', 'first_name', 'last_name', 'Assignment 1', 'Assignment 2', 'Assignment 3', 'avg_participation_grade', 'avg_behavior_grade']

        if list(result.columns) == expected_cols:
            score_columns = ['Assignment 1', 'Assignment 2', 'Assignment 3', 'avg_participation_grade', 'avg_behavior_grade']
            result[score_columns] = result[score_columns].apply(pd.to_numeric, errors='coerce')

            valid_scores = result[score_columns].apply(lambda x: x.between(0, 100)).all().all()

            if valid_scores:
                print("ReportCard view was created successfully with correct columns and valid scores")
            else:
                print("ReportCard view returned invalid score values")
        else:
            print("ReportCard view returned incorrect columns")
    else:
        print("ReportCard view query was not successful")
