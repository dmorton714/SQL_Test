import random
import numpy as np
import pandas as pd
import sqlite3

class SchoolDataGenerator:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.students = None
        self.assignments = None
        self.grades = None
        self.participation_behavior = None

    def generate_data(self):
        random.seed(42)
        np.random.seed(42)

        # First and Last Names Lists
        first_names_list = ["John", "Jane", "Alex", "Emily", "Chris", "Katie", "Michael", "Sarah", "David", "Laura",
                            "James", "Anna", "Robert", "Olivia", "Daniel", "Sophia", "Matthew", "Emma", "Joshua", "Isabella",
                            "Ryan", "Mia", "Andrew", "Ava", "Brandon", "Grace", "Tyler", "Chloe", "Zach", "Lily"]

        last_names_list = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                           "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
                           "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"]

        # 1. Students Data
        students_dict = {
            'student_id': list(range(1, 31)),
            'first_name': random.sample(first_names_list, 30),
            'last_name': random.sample(last_names_list, 30),
            'grade_level': np.random.choice(['10th', '11th', '12th'], size=30).tolist(),
            'email': [f"student{i}@school.com" for i in range(1, 31)]}
        self.students = pd.DataFrame(students_dict)

        # 2. Assignments Data
        assignments_dict = {
            'assignment_id': list(range(1, 11)),
            'assignment_name': [f"Assignment_{i}" for i in range(1, 11)],
            'due_date': pd.date_range(start='2024-09-01', periods=10, freq='W').strftime('%Y-%m-%d').tolist()}
        self.assignments = pd.DataFrame(assignments_dict)

        # 3. Grades Data
        grades_data = []
        grade_to_score = {
            'A': (90, 100),
            'B': (80, 89),
            'C': (70, 79),
            'D': (60, 69),
            'F': (0, 59)}

        for student_id in students_dict['student_id']:
            for assignment_id in assignments_dict['assignment_id']:
                letter_grade = random.choice(['A', 'B', 'C', 'D', 'F'])
                score_range = grade_to_score[letter_grade]
                numeric_score = random.randint(score_range[0], score_range[1])

                grades_data.append({
                    'grade_id': len(grades_data) + 1,
                    'student_id': student_id,
                    'assignment_id': assignment_id,
                    'grade': letter_grade,
                    'numeric_score': numeric_score})

        self.grades = pd.DataFrame(grades_data)

        # 4. Participation and Behavior Data
        participation_behavior_data = []
        for student_id in students_dict['student_id']:
            participation_behavior_data.append({
                'record_id': len(participation_behavior_data) + 1,
                'student_id': student_id,
                'attendance': random.choice(['Present', 'Absent']),
                'participation_grade': random.randint(1, 10),
                'behavior_grade': random.randint(1, 10)})

        self.participation_behavior = pd.DataFrame(participation_behavior_data)

    def populate_db(self):
        # Storing data into SQLite tables
        self.students.to_sql('students', self.conn, index=False, if_exists='replace')
        self.assignments.to_sql('assignments', self.conn, index=False, if_exists='replace')
        self.grades.to_sql('grades', self.conn, index=False, if_exists='replace')
        self.participation_behavior.to_sql('participation_behavior', self.conn, index=False, if_exists='replace')

    def sql(self, query):
        return pd.read_sql_query(query, self.conn)
