# installs 
----------------------------------------------------------------
pip install flask, sqlalchemy, pyodbc, hdbcli, python-dotenv
-----------------------------------------------------------------

# create a file called .env hide 4 security
-------------------------------------------------------
MAXIMO_DATABASE_URI=maximo_database_connection_string
------------------------------------------------------

# create_app.py
--------------------------------------------
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
import os

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    
    # Load environment variables from a .env file
    load_dotenv()
    
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('MAXIMO_DATABASE_URI')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    db.init_app(app)
    
    with app.app_context():
        from .routes import api_bp
        app.register_blueprint(api_bp)
        
    return app

# Ensure the application runs with the correct context
if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
----------------------------------------------

# config.py
--------------------------------------------------
import os

class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv('MAXIMO_DATABASE_URI')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
---------------------------------------------------

# name File: app/routes.py
--------------------------------------------
from flask import Blueprint, jsonify
from . import db
from sqlalchemy.exc import SQLAlchemyError

api_bp = Blueprint('api', __name__)

@api_bp.route('/api/maximo/assets', methods=['GET'])
def get_assets():
    try:
        result = db.engine.execute('SELECT * FROM assets')
        assets = [dict(row) for row in result]
        return jsonify(assets)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/top_maintained_assets', methods=['GET'])
def get_top_maintained_assets():
    try:
        query = """
        SELECT assetnum, COUNT(workorderid) AS maintenance_count
        FROM workorders
        GROUP BY assetnum
        ORDER BY maintenance_count DESC
        LIMIT 5;
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/avg_time_between_failures', methods=['GET'])
def get_avg_time_between_failures():
    try:
        query = """
        SELECT assetnum, AVG(DATEDIFF(day, prev_maintenance_date, maintenance_date)) AS avg_days_between_failures
        FROM (
            SELECT assetnum, maintenance_date,
                   LAG(maintenance_date) OVER (PARTITION BY assetnum ORDER BY maintenance_date) AS prev_maintenance_date
            FROM workorders
            WHERE status = 'Completed'
        ) AS subquery
        WHERE prev_maintenance_date IS NOT NULL
        GROUP BY assetnum;
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/high_cost_assets', methods=['GET'])
def get_high_cost_assets():
    try:
        query = """
        SELECT assetnum, SUM(cost) AS total_maintenance_cost
        FROM workorders
        GROUP BY assetnum
        HAVING SUM(cost) > 10000;
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/no_maintenance_last_year', methods=['GET'])
def get_no_maintenance_last_year():
    try:
        query = """
        SELECT assetnum
        FROM assets
        WHERE assetnum NOT IN (
            SELECT DISTINCT assetnum
            FROM workorders
            WHERE maintenance_date >= DATEADD(year, -1, GETDATE())
        );
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/top_technicians', methods=['GET'])
def get_top_technicians():
    try:
        query = """
        SELECT technician_id, COUNT(workorderid) AS completed_workorders
        FROM workorders
        WHERE status = 'Completed'
        GROUP BY technician_id
        ORDER BY completed_workorders DESC;
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/last_maintenance_status', methods=['GET'])
def get_last_maintenance_status():
    try:
        query = """
        SELECT a.assetnum, a.description, MAX(w.maintenance_date) AS last_maintenance_date, w.status
        FROM assets a
        JOIN workorders w ON a.assetnum = w.assetnum
        GROUP BY a.assetnum, a.description, w.status;
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/total_downtime_last_month', methods=['GET'])
def get_total_downtime_last_month():
    try:
        query = """
        SELECT assetnum, SUM(DATEDIFF(hour, start_time, end_time)) AS total_downtime_hours
        FROM downtime
        WHERE start_time >= DATEADD(month, -1, GETDATE())
        GROUP BY assetnum;
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/long_duration_workorders', methods=['GET'])
def get_long_duration_workorders():
    try:
        query = """
        SELECT w.workorderid, w.assetnum, w.duration
        FROM workorders w
        JOIN (
            SELECT assetnum, AVG(duration) AS avg_duration
            FROM workorders
            GROUP BY assetnum
        ) AS subquery ON w.assetnum = subquery.assetnum
        WHERE w.duration > subquery.avg_duration;
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/costly_maintenance_assets', methods=['GET'])
def get_costly_maintenance_assets():
    try:
        query = """
        SELECT assetnum, SUM(cost) AS total_cost
        FROM workorders
        WHERE maintenance_date >= DATEADD(month, -6, GETDATE())
        GROUP BY assetnum
        ORDER BY total_cost DESC
        LIMIT 10;
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/api/maximo/maintenance_by_weekday', methods=['GET'])
def get_maintenance_by_weekday():
    try:
        query = """
        SELECT DATENAME(weekday, maintenance_date) AS day_of_week, COUNT(workorderid) AS workorder_count
        FROM workorders
        GROUP BY DATENAME(weekday, maintenance_date)
        ORDER BY workorder_count DESC;
        """
        result = db.engine.execute(query)
        data = [dict(row) for row in result]
        return jsonify(data)
    except SQLAlchemyError as e:
        return jsonify({"error": str(e)}), 500
  
-----------------------------------------------------------------

--------------------------------------------------------------------
--------------------------------------------------------------------

# -----------------------Step 1: Run Your Flask Application------------------------
# Ensure your Flask application is running. You can start your application by navigating to your project directory and running:
-----------------------------------------------------
python create_app.py
----------------------------------------------------------------
# This will start the Flask server, and your API should be accessible at http://localhost:5000.


# ----------------Step 2: Connect Power BI to Your API-------------------------------
#                           Open Power BI Desktop.
#                            Get Data from Web:
#                              Click on Get Data in the Home tab.
#                                Select Web and click Connect.
#                                  Enter the URL of your API endpoint (e.g., http://localhost:5000/api/maximo/assets) and click OK.


#----------------Step 2:Transform Data-------------------------------
#                            Once Power BI fetches the data, you can transform it as needed in the Power Query Editor.
#                             Apply any necessary transformations and click Close & Apply.


# ----------------Step 4: Set Up Incremental Refresh
# A) Define Parameters:
#                     In Power Query Editor, define two parameters: RangeStart and RangeEnd.
#                     These parameters will be used to filter the data that needs to be incrementally refreshed.
# B) Filter Data:
#                    Apply a filter to your data based on RangeStart and RangeEnd. This filter should be applied to a date/time column in your data that indicates when the data was last modified or created.
#                     For example, if your data has a modified_date column, apply the filter to include only rows where modified_date is between RangeStart and RangeEnd.
# C) Set Incremental Refresh:
#                          Go back to Power BI Desktop, right-click on the dataset, and select Incremental refresh.
#                          Define the incremental refresh policy by specifying how many days, months, or years of data to load initially and how frequently to refresh the data.
# Example: Setting Up Incremental Refresh
# A) Define Parameters in Power Query:
#                             Go to Manage Parameters and create two parameters:
#                             RangeStart (DateTime, default value: #datetime(2023, 1, 1, 0, 0, 0))
#                               RangeEnd (DateTime, default value: #datetime(2024, 1, 1, 0, 0, 0))
# B) Filter Data:
#             In the Power Query Editor, select the date/time column you want to filter by (e.g., modified_date).
#              Apply a filter using RangeStart and RangeEnd:
          M code
 -----------------------------------------------------------------
 Table.SelectRows(your_table_name, each [modified_date] >= RangeStart and [modified_date] < RangeEnd)
-----------------------------------------------------------------------
# ------------------------------------------ Steps 5 Set Incremental Refresh:

# A)    Save and close the Power Query Editor.
# B) In Power BI Desktop, right-click on your dataset, and select Incremental refresh.
# C)   Configure the settings to specify how much historical data to load and how frequently to refresh the dataset.

# -------------------------------------- Example: Connecting Flask API to Power BI
# API Endpoint:
# Ensure your API endpoint returns data with a date/time column that can be used for incremental refresh. For example, the get_assets endpoint should return data with a modified_date column.

# Incremental Refresh Query in Power BI:

# In Power Query Editor:

# Define parameters RangeStart and RangeEnd.
# Apply a filter to the data:
 M Code
--------------------------------------------------------------------------------------------
 let
    Source = Json.Document(Web.Contents("http://localhost:5000/api/maximo/assets")),
    "Converted to Table" = Record.ToTable(Source),
    "Filtered Rows" = Table.SelectRows(#"Converted to Table", each [modified_date] >= RangeStart and [modified_date] < RangeEnd)
 in
    "Filtered Rows"
------------------------------------------------------------------------------------------------

# Running and Testing
# Run your Flask application:

--------------------------------------------------------
# python create_app.py
# Connect Power BI to your API and configure incremental refresh:
