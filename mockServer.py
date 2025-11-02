from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/raas/employees', methods=['GET'])
def get_employees():
    data = {
        "Report_Entry": [
            {"Employee_ID": "E1001", "First_Name": "Alice", "Last_Name": "Tan", "Department": "Finance", "Status": "ACTIVE"},
            {"Employee_ID": "E1002", "First_Name": "Bob", "Last_Name": "Lim", "Department": "HR", "Status": "INACTIVE"},
            {"Employee_ID": "E1003", "First_Name": "Cindy", "Last_Name": "Ng", "Department": "IT", "Status": "ACTIVE"},
            {"Employee_ID": "E1004", "First_Name": "Bruce", "Last_Name": "Wayne", "Department": "Crime Fighting", "Status": "ACTIVE"}
        ]
    }
    return jsonify(data)

@app.route('/raas/compensation', methods=['GET'])
def get_compensation():
    data = {
        "Report_Entry": [
            {"Employee_ID": "E1001", "Monthly_Salary": 5500, "Bonus": 800},
            {"Employee_ID": "E1002", "Monthly_Salary": 4700, "Bonus": 500},
            {"Employee_ID": "E1003", "Monthly_Salary": 6200, "Bonus": 1000},
            {"Employee_ID": "E1004", "Monthly_Salary": 7500, "Bonus": 0}
        ]
    }
    return jsonify(data)

if __name__ == '__main__':
    app.run(port=5000)