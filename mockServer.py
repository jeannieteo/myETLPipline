from flask import Flask, jsonify

app = Flask(__name__)
# Just a simple mock server to pretend to be the RaaS endpoints
# Mock endpoint for employee data
@app.route('/raas/employees', methods=['GET'])
def get_employees():
    data = {
        "Report_Entry": [
            {"Employee_ID": "E1001", "First_Name": "Alice, Mui Mui", "Last_Name": "Tan", "Department_ID": "D01", "Status": "ACTIVE"},
            {"Employee_ID": "E1002", "First_Name": "Bobby, Kwan Yew", "Last_Name": "Lim", "Department": "D02", "Status": "INACTIVE"},
            {"Employee_ID": "E1003", "First_Name": "Cindy", "Last_Name": "Ng", "Department": "D03", "Status": "ACTIVE"},
            {"Employee_ID": "E1004", "First_Name": "Bruce", "Last_Name": "Wayne", "Department": "D04", "Status": "ACTIVE"},
            {"Employee_ID": "E1005", "First_Name": "Dick", "Last_Name": "Grayson", "Department": "D04", "Status": "ACTIVE"},
            {"Employee_ID": "E1005", "First_Name": "", "Last_Name": "", "Department": "D04", "Status": "INACTIVE"}
        ]
        
    }
    return jsonify(data)
# jsonify() is a built-in Flask function that converts Python dictionaries and objects into JSON response objects. 
# It also automatically sets:
# The correct Content-Type header (application/json)
# Proper HTTP status codes (default: 200 OK)

# Mock endpoint for compensation /salary data
@app.route('/raas/compensation', methods=['GET'])
def get_compensation():
    data = {
        "Report_Entry": [
            {"Employee_ID": "E1001", "Monthly_Salary": 5500, "Bonus": 800},
            {"Employee_ID": "E1002", "Monthly_Salary": 4700, "Bonus": 500},
            {"Employee_ID": "E1003", "Monthly_Salary": 6200, "Bonus": 1000},
            {"Employee_ID": "E1004", "Monthly_Salary": 7500, "Bonus": 0},
            {"Employee_ID": "E1005", "Monthly_Salary": 7500, "Bonus": 0}
        ]
    }
    return jsonify(data)

# Mock Endpoint for department data
@app.route('/raas/departments', methods=['GET'])
def get_departments():
    data = {
        "Report_Entry": [
            {"Department_ID": "D01", "Department_Name": "Finance", "Manager_ID": "E1001"},
            {"Department_ID": "D02", "Department_Name": "HR", "Manager_ID": "E1002"},
            {"Department_ID": "D03", "Department_Name": "IT", "Manager_ID": "E1003"},
            {"Department_ID": "D04", "Department_Name": "Crime Fighting", "Manager_ID": "E1004"}

        ]
    }
    return jsonify(data)

if __name__ == '__main__':
    app.run(port=5000)