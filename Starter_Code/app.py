# Import the dependencies.
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify



#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to each the table
Measurement = Base.classes.measurement
Station = Base.classes.station


# Create our session (link) from Python to the DB
session = Session(engine)


#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start"
    )
    

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of date and precipitation"""
    # Query Measurement for date and precipitation
    results = session.query(Measurement.date, Measurement.prcp).all()
    
    # Convert list of tuples into normal list
    date_prcp = list(np.ravel(results))

   

    # Query Measurement for date for the past yeas

    p_data = session.query(Measurement.date, Measurement.prcp).\
    filter(Measurement.date > '2016-08-23').\
    order_by(Measurement.date.desc() ).all()
    session.close()

    # Convert list of tuples into normal list
    conv_date_prcp = list(np.ravel(p_data))

    return jsonify(date_prcp, conv_date_prcp)
    

@app.route("/api/v1.0/station")
def station():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Returns jsonified data of all of the stations in the database"""
    # Query all Measurement for stations
    results = session.query(Measurement.station).all()

    session.close()
  
    return jsonify(results)



app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Returns jsonified data for the most active station (USC00519281)"""
    # Get the most active station ID from the previous query
    most_active_station_id =  'USC00519281'

    temperature_data = session.query(Measurement.tobs) \
                          .filter(Measurement.station == most_active_station_id).all()


# Query temperature observation data for the most active station within the last 12 months
    temp_data = session.query(Measurement.tobs) \
                          .filter(Measurement.station == most_active_station_id,
                                       Measurement.date >= '2016-08-23') \
                          .all()
    session.close()

    return jsonify(temperature_data, temp_data)


app.route("/api/v1.0/start")
def start():
    # Create our session (link) from Python to the DB
    session = Session(engine)

# Accepts the start date as a parameter from the URL
    temperature_stats = session.query(func.min(Measurement.tobs).label('lowest_temp'),
                                  func.max(Measurement.tobs).label('highest_temp'),
                                  func.avg(Measurement.tobs).label('average_temp')) \
                        .first()
    session.close()

    return jsonify(temperature_stats)


if __name__ == '__main__':
    app.run(debug=True)