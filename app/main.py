from datetime import timedelta
from typing import Annotated
from fastapi import Body, Depends, FastAPI, HTTPException, status
from fastapi.openapi.utils import get_openapi
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from enum import Enum
import logging

from app.authentication import *
from app.mongo import *

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#class Item(BaseModel):
#    name: str
#    price: float
#    is_offer: bool | None = None

app = FastAPI()

#origins = [
#    "http://localhost.tiangolo.com",
#    "https://localhost.tiangolo.com",
#    "http://localhost",
#    "http://localhost:8080",
#]

origins = ["*"]

connection_string = "mongodb://root:soteria@10.32.8.186:27019/?authMechanism=DEFAULT"
logger.info(f"Initializing MongoDB connection to: {connection_string.replace('soteria', '***')}")
db_manager = MongoDBManager(connection_string)
logger.info("MongoDB connection initialized successfully")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Soteria API",
        version="2025.10.24",
        description="OpenAPI implementation for **Soteria** project",
        routes=app.routes,
        tags= [
            {"name":"login", "description":"API users and authentication"},
            {"name":"utilities", "description":"Set of utilities to facilitate the use of the API"},
            {"name":"hotspots", "description":"Retrieve information about the hotspots (segments and intersections)"},
            {"name":"accidents", "description":"Retrieve information and statistics about accidents"},
            {"name":"connected vehicle data", "description":"Retrieve connected vehicle data events and statistics"},
            {"name":"travel demand", "description":"Retrieve travel demand information"},
            {"name":"predictions", "description":"Retrieve predictions based on travel demand and historical data"},
            {"name":"nodes", "description":"Retrieve information about the nodes"},
            {"name":"edges", "description":"Retrieve information about the edges"},
            {"name":"segments", "description":"Retrieve information about the segments"},
        ]
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

@app.post("/token", tags=["login"])
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]) -> Token:
    user = authenticate_user(db_manager, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)
    return Token(access_token=access_token, token_type="bearer")

@app.get("/users/me", tags=["login"], response_model=User)
async def get_actual_user(current_user: Annotated[User, Depends(get_current_active_user)]):
    return current_user

#@app.get("/test/items/{item_id}", tags=["test"])
#def get_item(item_id: int, current_user: Annotated[User, Depends(get_current_active_user)], q: str | None = None):
#    return {"item_id": item_id, "q": q}

#@app.put("/test/items/{item_id}", tags=["test"])
#def update_item(item_id: int, item: Item):
#    return {"item_name": item.name, "item_id": item_id}

@app.get("/{location}/districts", tags=["utilities"])
def get_city_districts_geometries(location: Location):
    """
    Retrieves a list of geometries as polygons of the different city districts
    """
    result = db_manager.get_city_districts("locations", location)
    return result

@app.get("/{location}/hotspots", tags=["hotspots"])
def get_all_hotspots(location: Location, month: int = None, year: int = None, type: GeoType = None, user: UserType = None, severity: Severity = None, quantity: int = None):
    """
    **quantity**: Maximum number of items to return (_None or -1 for all items_)\n
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_all_hotspots("LGL_hotspots", quantity, type, user, severity, month, year)
        case Location.Saxony:
            result = db_manager.get_all_hotspots("LG_saxony_hotspots", quantity, type, user, severity, month, year)
        case _:
            result = []

    return result

@app.get("/{location}/hotspots/viewport", tags=["hotspots"])
def get_hotspots_in_viewport(location: Location, month: int = None, year: int = None, type: GeoType = None, user: UserType = None, severity: Severity = None, sw_lon: float = -3.6895, sw_lat: float = 40.4241, ne_lon: float = -3.6641, ne_lat: float = 40.4347):
    """
    **sw_lon** and **sw_lat**: The longitude and latitude of the bounding box limit point in the South-West\n
    **ne_lon** and **ne_lat**: The longitude and latitude of the bounding box limit point in the North-East
    """
    geometry = create_geometry(sw_lon, sw_lat, ne_lon, ne_lat)

    match location:
        case Location.Madrid:
            result = db_manager.get_hotspots_within_area("LGL_hotspots", geometry, type, user, severity, month, year)
        case Location.Saxony:
            result = db_manager.get_hotspots_within_area("LG_saxony_hotspots", geometry, type, user, severity, month, year)
        case _:
            result = []

    return result    

@app.post("/{location}/hotspots/geo", tags=["hotspots"])
def get_hotspots_in_geometry(location: Location, month: int = None, year: int = None, geometry: Geometry = Body(...), type: GeoType = None, user: UserType = None, severity: Severity = None):
    """
    **Geometry in body as JSON**: {"coordinates": [[[lat,lon],[lat,lon],[lat,lon],[lat,lon],[lat,lon]]], "type": "Polygon"}
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_hotspots_within_area("LGL_hotspots", geometry.model_dump(), type, user, severity, month, year)
        case Location.Saxony:
            result = db_manager.get_hotspots_within_area("LG_saxony_hotspots", geometry.model_dump(), type, user, severity, month, year)
        case _:
            result = []

    return result  

@app.get("/{location}/accidents/stats", tags=["accidents"])
def get_accidents_stats(location: Location, year: int = 2024):

    match location:
        case Location.Madrid:
            result = db_manager.get_accidents_stats("limpioMadridAccidentalidad", year)
        #case Location.Saxony:
        #    result = db_manager.get_accidents_stats("LG_saxony_accidents", year)
        case _:
            result = []

    return result

@app.get("/{location}/accidents/cadas/stats", tags=["accidents"])
def get_accidents_stats(location: Location, year: int = 2024):

    match location:
        case Location.Madrid:
            result = db_manager.get_accidents_stats_cadas("LGL_accidents_CADaS", year)
        case Location.Saxony:
            result = db_manager.get_accidents_stats_cadas("LG_saxony_accidents", year)
        case _:
            result = []

    return result

@app.post("/{location}/accidents/stats/geo", tags=["accidents"])
def get_accidents_stats_in_geometry(location: Location, geometry: Geometry = Body(...), year: int = 2024):
    """
    **Geometry in body as JSON**: {"coordinates": [[[lat,lon],[lat,lon],[lat,lon],[lat,lon],[lat,lon]]], "type": "Polygon"}
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_accidents_stats_within_area("limpioMadridAccidentalidad", geometry.model_dump(), year)
        #case Location.Saxony:
        #    result = db_manager.get_accidents_stats_within_area("LG_saxony_accidents", geometry.model_dump(), year)
        case _:
            result = []

    return result

@app.post("/{location}/accidents/cadas/stats/geo", tags=["accidents"])
def get_accidents_stats_in_geometry(location: Location, geometry: Geometry = Body(...), year: int = 2024):
    """
    **Geometry in body as JSON**: {"coordinates": [[[lat,lon],[lat,lon],[lat,lon],[lat,lon],[lat,lon]]], "type": "Polygon"}
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_accidents_stats_within_area("LGL_accidents_CADaS", geometry.model_dump(), year)
        case Location.Saxony:
            result = db_manager.get_accidents_stats_within_area("LG_saxony_accidents", geometry.model_dump(), year)
        case _:
            result = []

    return result

@app.get("/{location}/accidents/locations", tags=["accidents"])
def get_accidents_locations(location: Location, month: int = None, year: int = 2024, quantity: int = 50):
    """
    **quantity**: set to -1 to get all data (long query)\n
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_all_accidents_locations("LGL_accidents", month, year, quantity)
        #case Location.Saxony:
        #    result = db_manager.get_all_accidents_locations("LG_saxony_accidents", month, year, quantity)
        case _:
            result = []

    return result

@app.get("/{location}/accidents/cadas/locations", tags=["accidents"])
def get_accidents_locations_in_cadas_format(location: Location, month: int = None, year: int = 2024, quantity: int = 50):
    """
    **quantity**: set to -1 to get all data (long query)\n
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_all_cadas_accidents_locations("LGL_accidents_CADaS", month, year, quantity)
        case Location.Saxony:
            result = db_manager.get_all_cadas_accidents_locations("LG_saxony_accidents", month, year, quantity)
        case _:
            result = []

    return result

@app.post("/{location}/accidents/locations/geo", tags=["accidents"])
def get_accidents_locations_in_geometry(location: Location, geometry: Geometry = Body(...), month: int = None, year: int = 2024, quantity: int = 50):
    """
    **quantity**: set to -1 to get all data (long query)\n
    **Geometry in body as JSON**: {"coordinates": [[[lat,lon],[lat,lon],[lat,lon],[lat,lon],[lat,lon]]], "type": "Polygon"}
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_accidents_locations_within_area("LGL_accidents", geometry.model_dump(), month, year, quantity)
        #case Location.Saxony:
        #    result = db_manager.get_accidents_locations_within_area("LG_saxony_accidents", geometry.model_dump(), month, year, quantity)
        case _:
            result = []

    return result

@app.post("/{location}/accidents/cadas/locations/geo", tags=["accidents"])
def get_accidents_locations_in_geometry(location: Location, geometry: Geometry = Body(...), month: int = None, year: int = 2024, quantity: int = 50):
    """
    **quantity**: set to -1 to get all data (long query)\n
    **Geometry in body as JSON**: {"coordinates": [[[lat,lon],[lat,lon],[lat,lon],[lat,lon],[lat,lon]]], "type": "Polygon"}
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_accidents_locations_within_area("LGL_accidents_CADaS", geometry.model_dump(), month, year, quantity)
        case Location.Saxony:
            result = db_manager.get_accidents_locations_within_area("LG_saxony_accidents", geometry.model_dump(), month, year, quantity)
        case _:
            result = []

    return result

@app.get("/{location}/accidents/byhotspot", tags=["accidents"])
def get_accidents_by_hotspot_locations(location: Location, hotspot_location, hotspot_type: GeoType = None, year: int = None):
    """
    **quantity**: set to -1 to get all data (long query)\n
    """
    match location:
        case Location.Madrid:
            match hotspot_type:
                case None | GeoType.intersection:
                    result = db_manager.get_accidents_by_hotspot_locations("LGL_accidents", year, int(hotspot_location), hotspot_type)
                case GeoType.segment:
                    result = db_manager.get_accidents_by_hotspot_locations_for_segments("LGL_accidents", year, str(hotspot_location), hotspot_type)

        case Location.Saxony:
            match hotspot_type:
                case None | GeoType.intersection:
                    result = db_manager.get_accidents_by_hotspot_locations("LG_saxony_accidents", year, int(hotspot_location), hotspot_type)
                case GeoType.segment:
                    result = db_manager.get_accidents_by_hotspot_locations_for_segments("LG_saxony_accidents", year, str(hotspot_location), hotspot_type)

        case _:
            result = []

    return result

@app.get("/{location}/connectedvehicledata", tags=["connected vehicle data"])
def get_connected_vehicle_events(current_user: Annotated[User, Depends(get_current_active_user)], location: Location, event_type: EventType = None, month: int = None, year: int = None, percentile: int = 0, quantity: int = 50):
    """
    **decil**: minimum decile to meet for returned events (0 for all)\n
    **quantity**: set to -1 to get all data (long query)\n
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_all_documents_by_type("LGL_eventFrequency", event_type, month, year, percentile, quantity)
        case _:
            result = []
    return result

@app.get("/{location}/connectedvehicledata/dangerouslocations", tags=["connected vehicle data"])
def get_connected_vehicle_dangerous_locations(current_user: Annotated[User, Depends(get_current_active_user)], location: Location, month: int = None, year: int = None, cornering_right_percentile: int = 0, cornering_left_percentile: int = 0, brake_percentile: int = 0, speed_up_percentile: int = 0, quantity: int = 50):
    """
    **decil**: minimum decile to meet for returned events (0 for all)\n
    **quantity**: set to -1 to get all data (long query)\n
    """
    match location:
        case Location.Madrid:
            percentile = [cornering_right_percentile, cornering_left_percentile, brake_percentile, speed_up_percentile]
            result = db_manager.get_conn_vehicle_dangerous_locations("LGL_eventFrequency", month, year, percentile, quantity)
        case _:
            result = []
    return result

@app.get("/{location}/connectedvehicledata/stats/hotspots", tags=["connected vehicle data"])
def get_connected_vehicle_stats(current_user: Annotated[User, Depends(get_current_active_user)], location: Location):

    match location:
        case Location.Madrid:
            result = db_manager.get_conn_vehicle_stats("madridHotspots", "LGL_eventFrequency", GeoType.intersection)
        case _:
            result = []
    return result

@app.get("/{location}/traveldemand", tags=["travel demand"])
def get_travel_demand(current_user: Annotated[User, Depends(get_current_active_user)], location: Location, quantity: int = 50):
    """
    **quantity**: set to -1 to get all data (long query)\n
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_all_documents_travel_demand("LGL_travelDemandAggregated", quantity)
        case _:
            result = []
    return result

@app.get("/{location}/traveldemand/stats", tags=["travel demand"])
def get_travel_demand_stats(current_user: Annotated[User, Depends(get_current_active_user)], location: Location):

    match location:
        case Location.Madrid:
            result = db_manager.get_demand_stats("LGL_travelDemandAggregated")
        case _:
            result = []
    return result

@app.get("/{location}/traveldemand/accidents", tags=["travel demand"])
def get_aggregated_travel_demand_and_accidents_data(current_user: Annotated[User, Depends(get_current_active_user)], location: Location, quantity: int = 50, demand_type: DemandType = None, accidents_percentile: int = None):

    match location:
        case Location.Madrid:
            result = db_manager.get_all_documents_percentile("LGL_travelDemandAccidents", quantity, demand_type, accidents_percentile)
        case _:
            result = []
    return result

@app.get("/{location}/predictions/accidents", tags=["predictions"])
def get_accident_predictions(current_user: Annotated[User, Depends(get_current_active_user)], location: Location, month: int = None, year: int = 2025, quantity: int = 50, prediction_type: PredictionType = None, user: UserType = None, model_type: ModelType = None, risk_category: RiskCategory = None, error_category: ErrorCategory = None, is_currently_hotspot: bool | None = None):
    """
    **prediction_type**: Filter by prediction type\n
    **user**: Filter by user type\n
    **model_type**: Filter by model type\n
    **risk_category**: Filter by risk category (enum)\n
    **error_category**: Filter by error category (enum)\n
    **is_currently_hotspot**: Filter by whether the location is currently a hotspot (true/false)\n
    **quantity**: set to -1 to get all data (long query)\n
    """
    logger.info(f"GET /{location}/predictions/accidents - Parameters: month={month}, year={year}, quantity={quantity}, prediction_type={prediction_type}, user={user}, model_type={model_type}, risk_category={risk_category}, error_category={error_category}, is_currently_hotspot={is_currently_hotspot}, user={current_user.username}")
    
    try:
        match location:
            case Location.Madrid:
                result = db_manager.get_all_predictions(
                    "LGL_DL_module_predictions_v2",
                    month,
                    year,
                    quantity,
                    prediction_type,
                    user,
                    model_type,
                    risk_category,
                    error_category,
                    is_currently_hotspot
                )
                logger.info(f"Returning {len(result) if result else 0} predictions for Madrid")
            case _:
                logger.info(f"Location {location} not supported - returning empty result")
                result = []
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_accident_predictions: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/{location}/nodes", tags=["nodes"])
def get_all_nodes(location: Location, quantity: int = 50, accident_risk: int = None):
    """
    **quantity**: set to -1 to get all data (long query)\n
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_all_documents_risk("LGL_nodes", quantity, accident_risk)
        case Location.Saxony:
            result = db_manager.get_all_documents_risk("LG_saxony_nodes", quantity, accident_risk)
        case Location.Chania:
            result = db_manager.get_all_documents_risk("LG_chania_nodes", quantity, accident_risk)
        case Location.Igoumenitsa:
            result = db_manager.get_all_documents_risk("LG_igoumenitsa_nodes", quantity, accident_risk)
        case _:
            result = []
    return result

@app.get("/{location}/edges", tags=["edges"])
def get_all_edges(location: Location, quantity: int = 50):
    """
    **quantity**: set to -1 to get all data (long query)\n
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_all_documents("LGL_edges", quantity, None)
        case Location.Saxony:
            result = db_manager.get_all_documents("LG_saxony_edges", quantity, None)
        case Location.Chania:
            result = db_manager.get_all_documents_risk("LG_chania_edges", quantity, None)
        case Location.Igoumenitsa:
            result = db_manager.get_all_documents_risk("LG_igoumenitsa_edges", quantity, None)
        case _:
            result = []

    return result

@app.get("/{location}/nodes/geo", tags=["nodes"])
def get_nodes_in_geometry(location: Location, accident_risk: int | None = None, sw_lon: float = -3.6895, sw_lat: float = 40.4241, ne_lon: float = -3.6641, ne_lat: float = 40.4347):
    """
    **sw_lon** and **sw_lat**: The longitude and latitude of the bounding box limit point in the South-West\n
    **ne_lon** and **ne_lat**: The longitude and latitude of the bounding box limit point in the North-East
    """
    geometry = create_geometry(sw_lon, sw_lat, ne_lon, ne_lat)

    match location:
        case Location.Madrid:
            result = db_manager.get_documents_within_area("LGL_nodes", geometry, accident_risk)
        case Location.Saxony:
            result = db_manager.get_documents_within_area("LG_saxony_nodes", geometry, accident_risk)
        case Location.Chania:
            result = db_manager.get_all_documents_risk("LG_chania_nodes", geometry, accident_risk)
        case Location.Igoumenitsa:
            result = db_manager.get_all_documents_risk("LG_igoumenitsa_nodes", geometry, accident_risk)
        case _:
            result = []

    return result    

@app.get("/{location}/edges/geo", tags=["edges"])
def get_edges_in_geometry(location: Location, sw_lon: float = -3.6895, sw_lat: float = 40.4241, ne_lon: float = -3.6641, ne_lat: float = 40.4347):
    """
    **sw_lon** and **sw_lat**: The longitude and latitude of the bounding box limit point in the South-West\n
    **ne_lon** and **ne_lat**: The longitude and latitude of the bounding box limit point in the North-East
    """
    geometry = create_geometry(sw_lon, sw_lat, ne_lon, ne_lat)

    match location:
        case Location.Madrid:
            result = db_manager.get_documents_within_area("LGL_edges", geometry, None)
        case Location.Saxony:
            result = db_manager.get_documents_within_area("LG_saxony_edges", geometry, None)
        case Location.Chania:
            result = db_manager.get_all_documents_risk("LG_chania_edges", geometry, None)
        case Location.Igoumenitsa:
            result = db_manager.get_all_documents_risk("LG_igoumenitsa_edges", geometry, None)
        case _:
            result = []

    return result    

@app.get("/{location}/segments", tags=["segments"])
def get_all_segments(location: Location, quantity: int | None = 50):
    """
    **quantity**: set to -1 to get all data (long query)\n
    """
    match location:
        case Location.Madrid:
            result = db_manager.get_all_documents("LGL_segments", quantity, None)
        case Location.Saxony:
            result = db_manager.get_all_documents("LG_saxony_segments", quantity, None)
        case Location.Chania:
            result = db_manager.get_all_documents_risk("LG_chania_segments", quantity, None)
        case Location.Igoumenitsa:
            result = db_manager.get_all_documents_risk("LG_igoumenitsa_segments", quantity, None)
        case _:
            result = []

    return result

@app.get("/{location}/segments/geo", tags=["segments"])
def get_segments_in_geometry(location: Location, sw_lon: float = -3.6895, sw_lat: float = 40.4241, ne_lon: float = -3.6641, ne_lat: float = 40.4347):
    """
    **sw_lon** and **sw_lat**: The longitude and latitude of the bounding box limit point in the South-West\n
    **ne_lon** and **ne_lat**: The longitude and latitude of the bounding box limit point in the North-East
    """
    geometry = create_geometry(sw_lon, sw_lat, ne_lon, ne_lat)

    match location:
        case Location.Madrid:
            result = db_manager.get_documents_within_area("LGL_segments", geometry, None)
        case Location.Saxony:
            result = db_manager.get_documents_within_area("LG_saxony_segments", geometry, None)
        case Location.Chania:
            result = db_manager.get_all_documents_risk("LG_chania_segments", geometry, None)
        case Location.Igoumenitsa:
            result = db_manager.get_all_documents_risk("LG_igoumenitsa_segments", geometry, None)
        case _:
            result = []

    return result    

#Utils
def create_geometry(sw_lon, sw_lat, ne_lon, ne_lat):
    #sw_lon, sw_lat = map(float, sw_point.split(','))
    #ne_lon, ne_lat = map(float, ne_point.split(','))
    # Formar la geometría en formato deseado
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [sw_lon, sw_lat],
                [sw_lon, ne_lat],
                [ne_lon, ne_lat],
                [ne_lon, sw_lat],
                [sw_lon, sw_lat]
            ]
        ]
    }  
    return geometry