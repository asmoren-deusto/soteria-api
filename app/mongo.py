from enum import Enum
import datetime
import calendar
import json
import logging
from pydantic import BaseModel
import pymongo

# Configurar logging para mongo.py
logger = logging.getLogger(__name__)

class Location(Enum):
    Madrid = "madrid"
    Saxony = "saxony"
    Chania = "chania"
    Igoumenitsa = "igoumenitsa"

class GeoType(Enum):
    segment = "segment"
    intersection = "intersection"

class UserType(Enum):
    general = "general"
    pedestrian = "pedestrian"
    cyclist = "cyclist"
    motorcycle = "motorcycle"

class ModelType(Enum):
    GNN = "GNN"
    GLM = "GLM"

class Severity(Enum):
    light = "light"
    severe = "severe"
    mass = "mass"

class EventType(Enum): 
    CorneringRight = "cornering_right"
    CorneringLeft = "cornering_left"
    Brake = "brake"
    SpeedUp = "speedup"

class DemandType(Enum): 
    Micromobility = "micro"
    PrivateVehicle = "privateVehicle"

class PredictionType(Enum):
    AccidentNumberAll = "accident_number_all"
    AccidentRiskScoreAbsolute = "accident_risk_score_abs"
    AccidentRiskScoreRelative = "accident_risk_score_rel"

class RiskCategory(Enum):
    VeryLow = "very_low"
    Low = "low"
    Intermediate = "intermediate"
    High = "high"   
    VeryHigh = "very_high"  

class ErrorCategory(Enum):
    VeryLow = "very_low"
    Low = "low"
    Intermediate = "intermediate"
    High = "high"   
    VeryHigh = "very_high"  

class Geometry(BaseModel):
    type: str
    coordinates: list[list[list[float]]]

class MongoDBManager:
    def __init__(self, connection_string):
        self.client = pymongo.MongoClient(connection_string)
        self.db = self.client['SoteriaDB']

    def insert_document(self, collection_name, data):
        collection = self.db[collection_name]
        inserted_doc = collection.insert_one(data)
        return inserted_doc.inserted_id
    
    def get_city_districts(self, collection_name, location: Location):
        collection = self.db[collection_name]
        query = {'location': location.value}
        alldocs = collection.find(query, {'_id': 0})
        print (alldocs)
        return list(alldocs)
    
    def get_accidents_stats(self, collection_name, year):
        collection = self.db[collection_name]

        queryAllAccidents = {'fecha_hora':{'$gte':datetime.datetime(year,1,1),'$lt':datetime.datetime(year+1,1,1)}}
        queryFatalAndSevereAccidents = queryAllAccidents | {'$or': [{'gravedad_lesividad': 'Severe'}, {'gravedad_lesividad': 'Deceased'}]}

        numAllAccidents = len(list(collection.find(queryAllAccidents)))
        numFatalAccidents = len(list(collection.find(queryAllAccidents | {'gravedad_lesividad': 'Deceased'})))
        numSevereAccidents = len(list(collection.find(queryAllAccidents | {'gravedad_lesividad': 'Severe'})))
        numFatalAndSevereAccidents = len(list(collection.find(queryFatalAndSevereAccidents)))

        numFSADriver = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_persona': 'Driver'})))
        numFSAPassenger = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_persona': 'Passenger'})))
        numFSAPedestrian = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_persona': 'Pedestrian'})))

        numFSACoche = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Car'})))
        numFSAVehiculoComercial = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Commercial Vehicle'})))
        numFSAVehiculodeEmergencia = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Emergency Vehicle'})))
        numFSAVMU = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Personal Mobility Vehicle'})))
        numFSAOtrosA = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Others'})))
        numFSAAutobus = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Bus'})))
        numFSADesconocido = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Unknown'})))
        numFSAMotocicleta = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Motorcycle'})))
        numFSABicicleta = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Bicycle'})))
        numFSASinMotor = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Non-Motorized'})))
        numFSATren = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Train'})))

        numFSAAlcance = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Alcance'})))
        numFSAColisionFrontoLateral = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión fronto-lateral'})))
        numFSAOtros = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Otro'})))
        numFSASalidaDeVia = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Solo salida de la vía'})))
        numFSAColisionFrontal = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión frontal'})))
        numFSAChoqueObstaculo = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Choque contra obstáculo fijo'})))
        numFSACaida = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Caída'})))
        numFSAColisionLateral = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión lateral'})))
        numFSAAtropelloPersona = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Atropello a persona'})))
        numFSAColisionMultiple = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión múltiple'})))
        numFSAVuelco = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Vuelco'})))
        numFSAAtropelloAnimal = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Atropello a animal'})))

        numFSARango0 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'Menor de 5 años'})))
        numFSARango5 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 6 a 9 años'})))
        numFSARango10 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 10 a 14 años'})))
        numFSARango15 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 15 a 17 años'})))
        numFSARango17 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 18 a 20 años'})))
        numFSARango20 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 21 a 24 años'})))
        numFSARango25 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 25 a 29 años'})))
        numFSARango30 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 30 a 34 años'})))
        numFSARango35 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 35 a 39 años'})))
        numFSARango40 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 40 a 44 años'})))
        numFSARango45 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 45 a 49 años'})))
        numFSARango50 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 50 a 54 años'})))
        numFSARango55 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 55 a 59 años'})))
        numFSARango60 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 60 a 64 años'})))
        numFSARango65 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 65 a 69 años'})))
        numFSARango70 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 70 a 74 años'})))
        numFSARango75 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'Más de 74 años'})))

        

        return {
            "TotalNumberOfAccidents": {
                "AllAccidents": numAllAccidents, 
                "FatalAccidents": numFatalAccidents, 
                "SevereAccidents": numSevereAccidents,
                "FatalAndSevereAccidents": numFatalAndSevereAccidents
            },
            "FatalAndSevereAccidentsByInvolvedUser": {
                "Driver": numFSADriver,
                "Passenger": numFSAPassenger,
                "Pedestrian": numFSAPedestrian
            },
            "FatalAndSevereAccidentsByUserType": {
                "Car": numFSACoche,
                "Commercial Vehicle": numFSAVehiculoComercial,
                "Emergency Vehicle": numFSAVehiculodeEmergencia,
                "Urban Mobility Vehicle": numFSAVMU,
                "Other": numFSAOtrosA,
                "Bus": numFSAAutobus,
                "Unknown": numFSADesconocido,
                "Motorcycle": numFSAMotocicleta,
                "Bicycle": numFSABicicleta,
                "Non-Motorized": numFSASinMotor,
                "Train": numFSATren,
                "Pedestrian": numFSAPedestrian
            },
            "FatalAndSevereAccidentsByCauses": {
                "Rear-end collision": numFSAAlcance,
                "Front-lateral collision": numFSAColisionFrontoLateral,
                "Other": numFSAOtros,
                "Run-off-the-road only": numFSASalidaDeVia,
                "Head-on collision": numFSAColisionFrontal,
                "Crash into fixed obstacle": numFSAChoqueObstaculo,
                "Falling": numFSACaida,
                "Side impact collision": numFSAColisionLateral,
                "Person run over": numFSAAtropelloPersona,
                "Multiple collision": numFSAColisionMultiple,
                "Roll-over": numFSAVuelco,
                "Animal run over": numFSAAtropelloAnimal
            },
            "FatalAndSevereAccidentsByAgeRange": {
                "Less than 5 years": numFSARango0,
                "From 6 to 9 years": numFSARango5,
                "From 10 to 14 years": numFSARango10,
                "From 15 to 17 years": numFSARango15,
                "From 18 to 20 years": numFSARango17,
                "From 21 to 24 years": numFSARango20,
                "From 25 to 30 years": numFSARango25,
                "From 30 to 34 years": numFSARango30,
                "From 35 to 40 years": numFSARango35,
                "From 40 to 44 years": numFSARango40,
                "From 45 to 50 years": numFSARango45,
                "From 50 to 54 years": numFSARango50,
                "From 55 to 60 years": numFSARango55,
                "From 60 to 64 years": numFSARango60,
                "From 65 to 70 years": numFSARango65,
                "From 70 to 74 years": numFSARango70,
                "More than 74 years": numFSARango75,
            },
        }
    
    
    def get_accidents_stats_cadas(self, collection_name, year):
        collection = self.db[collection_name]

        # The cadas documents store fields under 'properties', e.g. properties.datetime
        # Use the properties.* paths so date and label filters match the document structure.
        queryAllAccidents = {'properties.datetime': {'$gte': datetime.datetime(year, 1, 1), '$lt': datetime.datetime(year + 1, 1, 1)}}
        queryFatalAndSevereAccidents = queryAllAccidents | {
            '$or': [
                {'properties.injury_severity_label': 'Severe'},
                {'properties.injury_severity_label': 'Deceased'}
            ]
        }

        numAllAccidents = len(list(collection.find(queryAllAccidents)))
        numFatalAccidents = len(list(collection.find(queryAllAccidents | {'properties.injury_severity_label': 'Deceased'})))
        numSevereAccidents = len(list(collection.find(queryAllAccidents | {'properties.injury_severity_label': 'Severe'})))
        numFatalAndSevereAccidents = len(list(collection.find(queryFatalAndSevereAccidents)))

        # person_type_label and vehicle_type_label are inside properties and many are arrays —
        # equality queries still match array elements, but the field must be referenced under properties.
        numFSADriver = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.person_type_label': 'Driver'})))
        numFSAPassenger = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.person_type_label': 'Passenger'})))
        numFSAPedestrian = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.person_type_label': 'Pedestrian'})))

        numFSACoche = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Passenger car'})))
        numFSAVehiculoComercial = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Goods Vehicle'})))
        numFSAVehiculodeEmergencia = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Emergency Vehicle'})))
        numFSAVMU = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Personal Mobility Vehicle'})))
        numFSAOtrosA = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Other motor vehicle'})))
        numFSAAutobus = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Bus or coach'})))
        numFSADesconocido = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Unknown'})))
        numFSAMotocicleta = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Motorcycle'})))
        numFSABicicleta = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Pedal Cycle'})))
        numFSASinMotor = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Pedestrian'})))
        numFSATren = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.vehicle_type_label': 'Moped'})))

        # Count both 'Clear' and 'Dry' (and some exports use 'Dry/Clear') as clear weather labels
        numFSAClear = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.weather_label': {'$in': ['Clear', 'Dry', 'Dry/Clear']}})))
        numFSARain = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.weather_label': 'Rain'})))
        numFSASnow = len(list(collection.find(queryFatalAndSevereAccidents | {'properties.weather_label': 'Snow'})))

        #numFSAAlcance = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Alcance'})))
        #numFSAColisionFrontoLateral = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión fronto-lateral'})))
        #numFSAOtros = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Otro'})))
        #numFSASalidaDeVia = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Solo salida de la vía'})))
        #numFSAColisionFrontal = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión frontal'})))
        #numFSAChoqueObstaculo = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Choque contra obstáculo fijo'})))
        #numFSACaida = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Caída'})))
        #numFSAColisionLateral = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión lateral'})))
        #numFSAAtropelloPersona = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Atropello a persona'})))
        #numFSAColisionMultiple = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión múltiple'})))
        #numFSAVuelco = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Vuelco'})))
        #numFSAAtropelloAnimal = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Atropello a animal'})))

        return {
            "TotalNumberOfAccidents": {
                "AllAccidents": numAllAccidents, 
                "FatalAccidents": numFatalAccidents, 
                "SevereAccidents": numSevereAccidents,
                "FatalAndSevereAccidents": numFatalAndSevereAccidents
            },
            "FatalAndSevereAccidentsByInvolvedUser": {
                "Driver": numFSADriver,
                "Passenger": numFSAPassenger,
                "Pedestrian": numFSAPedestrian
            },
            "FatalAndSevereAccidentsByUserType": {
                "Car": numFSACoche,
                "Commercial Vehicle": numFSAVehiculoComercial,
                "Emergency Vehicle": numFSAVehiculodeEmergencia,
                "Urban Mobility Vehicle": numFSAVMU,
                "Other": numFSAOtrosA,
                "Bus": numFSAAutobus,
                "Unknown": numFSADesconocido,
                "Motorcycle": numFSAMotocicleta,
                "Bicycle": numFSABicicleta,
                "Pedestrian": numFSASinMotor,
                "Moped": numFSATren,
            },
            "FatalAndSevereAccidentsByWeatherConditions": {
                "Clear": numFSAClear,
                "Rain": numFSARain,
                "Snow": numFSASnow,
            }
        }
    
    def get_accidents_stats_within_area(self, collection_name, geometry, year):
        collection = self.db[collection_name]

        queryGeo = {'geometry': {'$geoWithin': {'$geometry': geometry}}}
        queryAllAccidents = queryGeo | {'fecha_hora':{'$gte':datetime.datetime(year,1,1),'$lt':datetime.datetime(year+1,1,1)}}
        queryFatalAndSevereAccidents = queryAllAccidents | {'$or': [{'gravedad_lesividad': 'Severe'}, {'gravedad_lesividad': 'Deceased'}]}

        numAllAccidents = len(list(collection.find(queryAllAccidents)))
        numFatalAccidents = len(list(collection.find(queryAllAccidents | {'gravedad_lesividad': 'Deceased'})))
        numSevereAccidents = len(list(collection.find(queryAllAccidents | {'gravedad_lesividad': 'Severe'})))
        numFatalAndSevereAccidents = len(list(collection.find(queryFatalAndSevereAccidents)))

        numFSADriver = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_persona': 'Driver'})))
        numFSAPassenger = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_persona': 'Passenger'})))
        numFSAPedestrian = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_persona': 'Pedestrian'})))

        numFSACoche = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Car'})))
        numFSAVehiculoComercial = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Commercial Vehicle'})))
        numFSAVehiculodeEmergencia = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Emergency Vehicle'})))
        numFSAVMU = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Personal Mobility Vehicle'})))
        numFSAOtrosA = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Others'})))
        numFSAAutobus = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Bus'})))
        numFSADesconocido = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Unknown'})))
        numFSAMotocicleta = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Motorcycle'})))
        numFSABicicleta = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Bicycle'})))
        numFSASinMotor = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Non-Motorized'})))
        numFSATren = len(list(collection.find(queryFatalAndSevereAccidents | {'grupo_tipo_vehiculo': 'Train'})))

        numFSAAlcance = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Alcance'})))
        numFSAColisionFrontoLateral = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión fronto-lateral'})))
        numFSAOtros = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Otro'})))
        numFSASalidaDeVia = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Solo salida de la vía'})))
        numFSAColisionFrontal = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión frontal'})))
        numFSAChoqueObstaculo = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Choque contra obstáculo fijo'})))
        numFSACaida = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Caída'})))
        numFSAColisionLateral = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión lateral'})))
        numFSAAtropelloPersona = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Atropello a persona'})))
        numFSAColisionMultiple = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Colisión múltiple'})))
        numFSAVuelco = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Vuelco'})))
        numFSAAtropelloAnimal = len(list(collection.find(queryFatalAndSevereAccidents | {'tipo_accidente': 'Atropello a animal'})))

        numFSARango0 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'Menor de 5 años'})))
        numFSARango5 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 6 a 9 años'})))
        numFSARango10 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 10 a 14 años'})))
        numFSARango15 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 15 a 17 años'})))
        numFSARango17 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 18 a 20 años'})))
        numFSARango20 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 21 a 24 años'})))
        numFSARango25 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 25 a 29 años'})))
        numFSARango30 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 30 a 34 años'})))
        numFSARango35 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 35 a 39 años'})))
        numFSARango40 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 40 a 44 años'})))
        numFSARango45 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 45 a 49 años'})))
        numFSARango50 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 50 a 54 años'})))
        numFSARango55 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 55 a 59 años'})))
        numFSARango60 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 60 a 64 años'})))
        numFSARango65 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 65 a 69 años'})))
        numFSARango70 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'De 70 a 74 años'})))
        numFSARango75 = len(list(collection.find(queryFatalAndSevereAccidents | {'rango_edad': 'Más de 74 años'})))

        

        return {
            "TotalNumberOfAccidents": {
                "AllAccidents": numAllAccidents, 
                "FatalAccidents": numFatalAccidents, 
                "SevereAccidents": numSevereAccidents,
                "FatalAndSevereAccidents": numFatalAndSevereAccidents
            },
            "FatalAndSevereAccidentsByInvolvedUser": {
                "Driver": numFSADriver,
                "Passenger": numFSAPassenger,
                "Pedestrian": numFSAPedestrian
            },
            "FatalAndSevereAccidentsByUserType": {
                "Car": numFSACoche,
                "Commercial Vehicle": numFSAVehiculoComercial,
                "Emergency Vehicle": numFSAVehiculodeEmergencia,
                "Urban Mobility Vehicle": numFSAVMU,
                "Other": numFSAOtrosA,
                "Bus": numFSAAutobus,
                "Unknown": numFSADesconocido,
                "Motorcycle": numFSAMotocicleta,
                "Bicycle": numFSABicicleta,
                "Non-Motorized": numFSASinMotor,
                "Train": numFSATren,
                "Pedestrian": numFSAPedestrian
            },
            "FatalAndSevereAccidentsByCauses": {
                "Rear-end collision": numFSAAlcance,
                "Front-lateral collision": numFSAColisionFrontoLateral,
                "Other": numFSAOtros,
                "Run-off-the-road only": numFSASalidaDeVia,
                "Head-on collision": numFSAColisionFrontal,
                "Crash into fixed obstacle": numFSAChoqueObstaculo,
                "Falling": numFSACaida,
                "Side impact collision": numFSAColisionLateral,
                "Person run over": numFSAAtropelloPersona,
                "Multiple collision": numFSAColisionMultiple,
                "Roll-over": numFSAVuelco,
                "Animal run over": numFSAAtropelloAnimal
            },
            "FatalAndSevereAccidentsByAgeRange": {
                "Less than 5 years": numFSARango0,
                "From 6 to 9 years": numFSARango5,
                "From 10 to 14 years": numFSARango10,
                "From 15 to 17 years": numFSARango15,
                "From 18 to 20 years": numFSARango17,
                "From 21 to 24 years": numFSARango20,
                "From 25 to 30 years": numFSARango25,
                "From 30 to 34 years": numFSARango30,
                "From 35 to 40 years": numFSARango35,
                "From 40 to 44 years": numFSARango40,
                "From 45 to 50 years": numFSARango45,
                "From 50 to 54 years": numFSARango50,
                "From 55 to 60 years": numFSARango55,
                "From 60 to 64 years": numFSARango60,
                "From 65 to 70 years": numFSARango65,
                "From 70 to 74 years": numFSARango70,
                "More than 74 years": numFSARango75,
            },
        }
    
    def get_demand_stats(self, collection_name):
        collection = self.db[collection_name]

        aggregateTotalDemand = [
            {
                "$group": {
                    "_id": None,
                    "totalMicroDemand": { "$sum": "$properties.total_demand.micro" },
                    "totalPrivateVehicleDemand": { "$sum": "$properties.total_demand.privateVehicle" },
                    "sumPrivateVehicleGender1": { "$sum": "$properties.gender.gender_privateVehicle.1" },
                    "sumPrivateVehicleGender2": { "$sum": "$properties.gender.gender_privateVehicle.2" },
                    "sumMicroGender1": { "$sum": "$properties.gender.gender_micro.1" },
                    "sumMicroGender2": { "$sum": "$properties.gender.gender_micro.2" },
                    "sumHourMicro0": { "$sum": "$properties.hour.hour_micro.0" },
                    "sumHourMicro1": { "$sum": "$properties.hour.hour_micro.1" },
                    "sumHourMicro2": { "$sum": "$properties.hour.hour_micro.2" },
                    "sumHourMicro3": { "$sum": "$properties.hour.hour_micro.3" },
                    "sumHourMicro4": { "$sum": "$properties.hour.hour_micro.4" },
                    "sumHourMicro5": { "$sum": "$properties.hour.hour_micro.5" },
                    "sumHourMicro6": { "$sum": "$properties.hour.hour_micro.6" },
                    "sumHourMicro7": { "$sum": "$properties.hour.hour_micro.7" },
                    "sumHourMicro8": { "$sum": "$properties.hour.hour_micro.8" },
                    "sumHourMicro9": { "$sum": "$properties.hour.hour_micro.9" },
                    "sumHourMicro10": { "$sum": "$properties.hour.hour_micro.10" },
                    "sumHourMicro11": { "$sum": "$properties.hour.hour_micro.11" },
                    "sumHourMicro12": { "$sum": "$properties.hour.hour_micro.12" },
                    "sumHourMicro13": { "$sum": "$properties.hour.hour_micro.13" },
                    "sumHourMicro14": { "$sum": "$properties.hour.hour_micro.14" },
                    "sumHourMicro15": { "$sum": "$properties.hour.hour_micro.15" },
                    "sumHourMicro16": { "$sum": "$properties.hour.hour_micro.16" },
                    "sumHourMicro17": { "$sum": "$properties.hour.hour_micro.17" },
                    "sumHourMicro18": { "$sum": "$properties.hour.hour_micro.18" },
                    "sumHourMicro19": { "$sum": "$properties.hour.hour_micro.19" },
                    "sumHourMicro20": { "$sum": "$properties.hour.hour_micro.20" },
                    "sumHourMicro21": { "$sum": "$properties.hour.hour_micro.21" },
                    "sumHourMicro22": { "$sum": "$properties.hour.hour_micro.22" },
                    "sumHourMicro23": { "$sum": "$properties.hour.hour_micro.23" },
                    "sumHourPrivateVehicle0": { "$sum": "$properties.hour.hour_privateVehicle.0" },
                    "sumHourPrivateVehicle1": { "$sum": "$properties.hour.hour_privateVehicle.1" },
                    "sumHourPrivateVehicle2": { "$sum": "$properties.hour.hour_privateVehicle.2" },
                    "sumHourPrivateVehicle3": { "$sum": "$properties.hour.hour_privateVehicle.3" },
                    "sumHourPrivateVehicle4": { "$sum": "$properties.hour.hour_privateVehicle.4" },
                    "sumHourPrivateVehicle5": { "$sum": "$properties.hour.hour_privateVehicle.5" },
                    "sumHourPrivateVehicle6": { "$sum": "$properties.hour.hour_privateVehicle.6" },
                    "sumHourPrivateVehicle7": { "$sum": "$properties.hour.hour_privateVehicle.7" },
                    "sumHourPrivateVehicle8": { "$sum": "$properties.hour.hour_privateVehicle.8" },
                    "sumHourPrivateVehicle9": { "$sum": "$properties.hour.hour_privateVehicle.9" },
                    "sumHourPrivateVehicle10": { "$sum": "$properties.hour.hour_privateVehicle.10" },
                    "sumHourPrivateVehicle11": { "$sum": "$properties.hour.hour_privateVehicle.11" },
                    "sumHourPrivateVehicle12": { "$sum": "$properties.hour.hour_privateVehicle.12" },
                    "sumHourPrivateVehicle13": { "$sum": "$properties.hour.hour_privateVehicle.13" },
                    "sumHourPrivateVehicle14": { "$sum": "$properties.hour.hour_privateVehicle.14" },
                    "sumHourPrivateVehicle15": { "$sum": "$properties.hour.hour_privateVehicle.15" },
                    "sumHourPrivateVehicle16": { "$sum": "$properties.hour.hour_privateVehicle.16" },
                    "sumHourPrivateVehicle17": { "$sum": "$properties.hour.hour_privateVehicle.17" },
                    "sumHourPrivateVehicle18": { "$sum": "$properties.hour.hour_privateVehicle.18" },
                    "sumHourPrivateVehicle19": { "$sum": "$properties.hour.hour_privateVehicle.19" },
                    "sumHourPrivateVehicle20": { "$sum": "$properties.hour.hour_privateVehicle.20" },
                    "sumHourPrivateVehicle21": { "$sum": "$properties.hour.hour_privateVehicle.21" },
                    "sumHourPrivateVehicle22": { "$sum": "$properties.hour.hour_privateVehicle.22" },
                    "sumHourPrivateVehicle23": { "$sum": "$properties.hour.hour_privateVehicle.23" }
                }
            }
        ]

        result = list(collection.aggregate(aggregateTotalDemand))

        if result:
            return {
                "total_demand" : {
                    "micro": result[0]['totalMicroDemand'],
                    "privateVehicle": result[0]['totalPrivateVehicleDemand']
                },
                "gender": {
                    "gender_micro": {
                        "1": result[0]['sumMicroGender1'],
                        "2": result[0]['sumMicroGender2']
                    },
                    "gender_privateVehicle": {
                        "1": result[0]['sumPrivateVehicleGender1'],
                        "2": result[0]['sumPrivateVehicleGender2']
                    }
                },
                "hour": {
                    "hour_micro":
                        {f'{hour}': result[0][f'sumHourMicro{hour}'] for hour in range(24)},
                    "hour_privateVehicle":
                        {f'{hour}': result[0][f'sumHourPrivateVehicle{hour}'] for hour in range(24)}
                }
            }
                
        else:
            return {}

    def get_all_accidents_locations(self, collection_name, month, year, quantity):
        collection = self.db[collection_name]
        # Obtener rango de fechas
        if month is None:
            if year is None: year, month = obtener_fecha_mas_reciente(collection, date_field="properties.fecha_hora")
            else: month = obtener_mes_mas_reciente(collection, year, date_field="properties.fecha_hora")
        else:
            if year is None: year, _ = obtener_fecha_mas_reciente(collection, date_field="properties.fecha_hora")

        if year is None or month not in range(1, 13): return []
        fecha_inicio = datetime.datetime(year, month, 1)
        fecha_fin = datetime.datetime(year + 1, 1, 1) if month == 12 else datetime.datetime(year, month + 1, 1)

        queryDate = {"properties.fecha_hora": {"$gte": fecha_inicio,"$lt": fecha_fin}}    

        alldocs = collection.find(queryDate,{'_id': 0}) if quantity in (None, -1) else collection.find(queryDate,{'_id': 0}).limit(quantity)

        return list(alldocs)
    
    def get_all_cadas_accidents_locations(self, collection_name, month, year, quantity):
        collection = self.db[collection_name]
        # Obtener rango de fechas
        if month is None:
            if year is None: year, month = obtener_fecha_mas_reciente(collection, date_field="properties.datetime")
            else: month = obtener_mes_mas_reciente(collection, year, date_field="properties.datetime")
        else:
            if year is None: year, _ = obtener_fecha_mas_reciente(collection, date_field="properties.datetime")

        if year is None or month not in range(1, 13): return []
        fecha_inicio = datetime.datetime(year, month, 1)
        fecha_fin = datetime.datetime(year + 1, 1, 1) if month == 12 else datetime.datetime(year, month + 1, 1)

        queryDate = {"properties.datetime": {"$gte": fecha_inicio,"$lt": fecha_fin}}    

        alldocs = collection.find(queryDate,{'_id': 0}) if quantity in (None, -1) else collection.find(queryDate,{'_id': 0}).limit(quantity)

        return list(alldocs)
    
    def get_all_predictions(self, collection_name, month, year, quantity, prediction_type: PredictionType = None, user: UserType = None, model_type: ModelType = None, risk_category: RiskCategory = None, error_category: ErrorCategory = None, is_currently_hotspot: bool | None = None):
        logger.info(f"get_all_predictions called with: collection={collection_name}, month={month}, year={year}, quantity={quantity}, prediction_type={prediction_type}, user={user}, model_type={model_type}, risk_category={risk_category}, error_category={error_category}, is_currently_hotspot={is_currently_hotspot}")
        
        collection = self.db[collection_name]
        
        # Obtener rango de fechas
        if month is None:
            if year is None:
                year, month = obtener_fecha_mas_reciente_array(collection, array_field="properties.predictions", date_field="prediction.start_period")
            else:
                month = obtener_mes_mas_reciente_array(collection, year, array_field="properties.predictions", date_field="prediction.start_period")
        else:
            if year is None:
                year, _ = obtener_fecha_mas_reciente_array(collection, array_field="properties.predictions", date_field="prediction.start_period")

        if year is None or month not in range(1, 13):
            logger.warning(f"Invalid date parameters: year={year}, month={month}")
            return []
            
        # Usar datetime objects para la comparación
        fecha_inicio = datetime.datetime(year, month, 1)
        fecha_fin = datetime.datetime(year + 1, 1, 1) if month == 12 else datetime.datetime(year, month + 1, 1)
            
        logger.info(f"Querying for date range: {fecha_inicio} to {fecha_fin}")

        # Construir consulta con filtros adicionales
        elemMatch_conditions = {
            "prediction.start_period": {"$gte": fecha_inicio, "$lt": fecha_fin}
        }
        # Agregar filtros opcionales dentro del array properties.predictions
        if prediction_type:
            elemMatch_conditions["prediction_type"] = prediction_type.value
        if user:
            elemMatch_conditions["user"] = user.value
        if model_type:
            elemMatch_conditions["model_type"] = model_type.value

        # Añadir filtros que están dentro de cada elemento del array 'properties.predictions'
        if risk_category:
            # en los documentos el campo está dentro de prediction.risk_category
            elemMatch_conditions["prediction.risk_category"] = risk_category.value if isinstance(risk_category, RiskCategory) else risk_category
        if error_category:
            elemMatch_conditions["prediction.error_category"] = error_category.value if isinstance(error_category, ErrorCategory) else error_category
        if is_currently_hotspot is not None:
            # Algunos exportes guardan esto como string "false"/"true"; aceptamos booleano o string
            if isinstance(is_currently_hotspot, bool):
                elemMatch_conditions["prediction.is_currently_hotspot"] = {"$in": [is_currently_hotspot, str(is_currently_hotspot).lower()]}
            else:
                elemMatch_conditions["prediction.is_currently_hotspot"] = is_currently_hotspot

        # Consulta: buscamos un elemento dentro de properties.predictions que cumpla elemMatch_conditions
        query = {"properties.predictions": {"$elemMatch": elemMatch_conditions}}

        logger.info(f"MongoDB query with filters: {query}")

        # Build aggregation pipeline to project only the matching prediction elements
        # Create $filter cond dynamically
        conds = []
        # date range
        conds.append({"$gte": ["$$p.prediction.start_period", fecha_inicio]})
        conds.append({"$lt": ["$$p.prediction.start_period", fecha_fin]})

        if prediction_type:
            conds.append({"$eq": ["$$p.prediction_type", prediction_type.value]})
        if user:
            conds.append({"$eq": ["$$p.user", user.value]})
        if model_type:
            conds.append({"$eq": ["$$p.model_type", model_type.value]})
        if risk_category:
            rc_val = risk_category.value if isinstance(risk_category, RiskCategory) else risk_category
            conds.append({"$eq": ["$$p.prediction.risk_category", rc_val]})
        if error_category:
            ec_val = error_category.value if isinstance(error_category, ErrorCategory) else error_category
            conds.append({"$eq": ["$$p.prediction.error_category", ec_val]})
        if is_currently_hotspot is not None:
            if isinstance(is_currently_hotspot, bool):
                conds.append({"$in": ["$$p.prediction.is_currently_hotspot", [is_currently_hotspot, str(is_currently_hotspot).lower()]]})
            else:
                conds.append({"$eq": ["$$p.prediction.is_currently_hotspot", is_currently_hotspot]})

        # Final condition: if there is only date conds, use $and anyway
        filter_cond = {"$and": conds}

        pipeline = [
            {"$match": query},
            {"$set": {
                "properties.predictions": {
                    "$filter": {
                        "input": "$properties.predictions",
                        "as": "p",
                        "cond": filter_cond
                    }
                }
            }},
            {"$project": {"_id": 0}}
        ]

        if quantity not in (None, -1):
            pipeline.append({"$limit": quantity})

        alldocs = collection.aggregate(pipeline)
        result = list(alldocs)

        logger.info(f"Aggregation returned {len(result)} documents")

        return result
    
    def get_accidents_locations_within_area(self, collection_name, geometry, month, year, quantity):
        collection = self.db[collection_name]
        # Obtener rango de fechas
        if month is None:
            if year is None: year, month = obtener_fecha_mas_reciente(collection, date_field="properties.fecha_hora")
            else: month = obtener_mes_mas_reciente(collection, year, date_field="properties.fecha_hora")
        else:
            if year is None: year, _ = obtener_fecha_mas_reciente(collection, date_field="properties.fecha_hora")

        if year is None or month not in range(1, 13): return []
        fecha_inicio = datetime.datetime(year, month, 1)
        fecha_fin = datetime.datetime(year + 1, 1, 1) if month == 12 else datetime.datetime(year, month + 1, 1)

        queryGeo = {'geometry': {'$geoWithin': {'$geometry': geometry}}}
        queryDate = {"properties.fecha_hora": {"$gte": fecha_inicio,"$lt": fecha_fin}}    
        query = queryDate | queryGeo

        alldocs = collection.find(query,{'_id': 0}) if quantity in (None, -1) else collection.find(query,{'_id': 0}).limit(quantity)

        return list(alldocs)

    def get_accidents_by_hotspot_locations(self, collection_name, year: int, location: int, location_type: GeoType):
        collection = self.db[collection_name]
        queryDate = {'properties.fecha_hora':{'$gte':datetime.datetime(year,1,1),'$lt':datetime.datetime(year+1,1,1)}} if year is not None else {}
        queryLocation = {"properties.locationID": location}
        queryType = {"properties.locationType": location_type.value} if location_type is not None else {}

        queryAll = queryDate | queryLocation | queryType if year is not None else queryLocation | queryType

        alldocs = collection.find(queryAll, {'_id': 0})
        return list(alldocs)
    
    def get_accidents_by_hotspot_locations_for_segments(self, collection_name, year, location: str, location_type: GeoType):
        collection = self.db[collection_name]
        queryDate = {'properties.fecha_hora':{'$gte':datetime.datetime(year,1,1),'$lt':datetime.datetime(year+1,1,1)}} if year is not None else {}

        u, v, key, segmentID = map(int, location.split(","))
        queryLocation = {"properties.locationID": {"u": u, "v": v, "key": key, "segmentID": segmentID}}
        queryType = {"properties.locationType": location_type.value} if location_type is not None else {}

        queryAll = queryDate | queryLocation | queryType if year is not None else queryLocation | queryType

        alldocs = collection.find(queryAll, {'_id': 0})
        return list(alldocs)

    def get_conn_vehicle_stats(self, hotspots_collection_name, events_collection_name, type: GeoType): #Intersection por ahora
        hotspotsCollection = self.db[hotspots_collection_name]
        eventsCollection = self.db[events_collection_name]

        #Hotspots
        #queryGeoType = {'properties.hotspotType': type.value} if type is not None else {}
        #query = {'properties.info': {'$elemMatch': {'user': UserType.pedestrian.value, 'severity': Severity.light.value}}}
        #queryPedestrianLight = queryGeoType | query

        #Events
        #query = {'properties.event_type': EventType.CorneringRight.value}
        #queryCorneringRight = queryGeoType | query

        #hotspotsdocs = hotspotsCollection.find(queryPedestrianLight,{'_id': 0})
        #eventssdocs = eventsCollection.find(queryCorneringRight,{'_id': 0})

        # Crear la consulta de agregación usando $lookup
        pipeline = [
            {
                '$match': {
                    'properties.hotspotType': 'intersection'
                }
            },
            {
                '$lookup': {
                    'from': 'madridEventFrequency', 
                    'localField': 'properties.id', 
                    'foreignField': 'properties.ID', 
                    'pipeline': [
                        {
                            '$match': {
                                'properties.type': 'intersection'
                            }
                        },
                        {
                            '$project': {
                                '_id': 0,
                                'properties.event_type': 1,
                                'properties.D': 1
                            }
                        }
                    ], 
                    'as': 'result'
                }
            },
            {
                '$unwind': '$properties.info'  # Descomponer cada 'info' para acceder a cada 'user' y 'severity'
            },
            {
                '$unwind': '$result'  # Descomponer el array 'result' para trabajar con cada documento relacionado
            },
            {
                '$group': {
                    '_id': {
                        'user': '$properties.info.user',  
                        'severity': '$properties.info.severity',  
                        'event_type': '$result.properties.event_type',  
                        'D': '$result.properties.D'  
                    },
                    'count': {'$sum': 1}  # Contador de ocurrencias para cada combinación
                }
            },
            {
                '$group': {
                    '_id': {
                        'user': '$_id.user',
                        'severity': '$_id.severity',
                        'event_type': '$_id.event_type'
                    },
                    'deciles': {
                        '$push': {
                            'D': '$_id.D',
                            'count': '$count'
                        }
                    },
                    'total': {'$sum': '$count'}  # Suma de todos los deciles para cada event_type
                }
            },
            {
                '$group': {
                    '_id': {
                        'user': '$_id.user',
                        'severity': '$_id.severity'
                    },
                    'event_types': {
                        '$push': {
                            'event_type': '$_id.event_type',
                            'deciles': '$deciles',
                            'total': '$total'  # Incluye el total de los deciles en cada event_type
                        }
                    }
                }
            },
            {
                '$group': {
                    '_id': '$_id.user',
                    'severities': {
                        '$push': {
                            'severity': '$_id.severity',
                            'event_types': '$event_types'
                        }
                    }
                }
            },
            {
                # Cambiar el nombre de '_id' a 'user'
                '$project': {
                    'user': '$_id',  # Renombra '_id' a 'user'
                    '_id': 0,
                    'severities': 1,

                }
            }
        ]

        # Ejecutar la consulta
        results = hotspotsCollection.aggregate(pipeline)

        # Inicializar una lista para almacenar el resultado final
        final_results = []

        for result in results:
            user_entry = {
                'user': result['user'],
                'severities': result['severities']
            }
            final_results.append(user_entry)

        # Imprimir el JSON resultante
        return final_results
        #return list(hotspotsCollection.aggregate(pipeline))
    
    def get_all_hotspots(self, collection_name, quantity, type: GeoType, user: UserType, severity: Severity, month: int, year: int):
        collection = self.db[collection_name]
        queryOne = {'properties.locationType': type.value} if type is not None else {}
        queryTwo = {'properties.info.user': user.value} if user is not None else {}
        queryThree = {'properties.info.severity': severity.value} if severity is not None else {}

        # Obtener rango de fechas
        if month is None:
            if year is None: year, month = obtener_fecha_mas_reciente(collection, date_field="properties.date")
            else: month = obtener_mes_mas_reciente(collection, year, date_field="properties.date")
        else:
            if year is None: year, _ = obtener_fecha_mas_reciente(collection, date_field="properties.date")

        if year is None or month not in range(1, 13): return []
        fecha_inicio = datetime.datetime(year, month, 1)
        fecha_fin = datetime.datetime(year + 1, 1, 1) if month == 12 else datetime.datetime(year, month + 1, 1)

        queryDate = {"properties.date": {"$gte": fecha_inicio,"$lt": fecha_fin}}    

        if (user is not None and severity is not None):
            queryAll = {'properties.info': {'$elemMatch': {'user': user.value, 'severity': severity.value}}}
            query = queryOne | queryAll | queryDate
        else:
            query = queryOne | queryTwo | queryThree | queryDate

        alldocs = collection.find(query,{'_id': 0}) if quantity in (None, -1) else collection.find(query,{'_id': 0}).limit(quantity)
        return list(alldocs)
    
    def get_hotspots_within_area(self, collection_name, geometry, type: GeoType, user: UserType, severity: Severity, month: int, year: int):
        collection = self.db[collection_name]
        queryOne = {'properties.hotspotType': type.value} if type is not None else {}
        queryTwo = {'properties.info.user': user.value} if user is not None else {}
        queryThree = {'properties.info.severity': severity.value} if severity is not None else {}

        # Obtener rango de fechas
        if month is None:
            if year is None: year, month = obtener_fecha_mas_reciente(collection, date_field="properties.date")
            else: month = obtener_mes_mas_reciente(collection, year, date_field="properties.date")
        else:
            if year is None: year, _ = obtener_fecha_mas_reciente(collection, date_field="properties.date")

        if year is None or month not in range(1, 13): return []
        fecha_inicio = datetime.datetime(year, month, 1)
        fecha_fin = datetime.datetime(year + 1, 1, 1) if month == 12 else datetime.datetime(year, month + 1, 1)

        queryDate = {"properties.date": {"$gte": fecha_inicio,"$lt": fecha_fin}}    

        queryGeo = {'geometry': {'$geoWithin': {'$geometry': geometry}}}

        if (user is not None and severity is not None):
            queryAll = {'properties.info': {'$elemMatch': {'user': user.value, 'severity': severity.value}}}
            query = queryOne | queryAll | queryDate | queryGeo
        else:
            query = queryOne | queryTwo | queryThree | queryDate | queryGeo

        alldocs = collection.find(query,{'_id': 0})
        return list(alldocs)
    
    def get_all_documents(self, collection_name, quantity, accident_risk):
        collection = self.db[collection_name]
        #queryOne = {'properties.is_hotspot': is_hotspot} if is_hotspot is not None else {}
        queryTwo = {'properties.accident_risk': {'$gte': accident_risk}} if accident_risk is not None else {}

        query = queryTwo

        alldocs = collection.find(query,{'_id': 0}) if quantity in (None, -1) else collection.find(query,{'_id': 0}).limit(quantity)
        return list(alldocs)

    def get_all_documents_travel_demand(self, collection_name, quantity):
        collection = self.db[collection_name]
        alldocs = collection.find({}, {'_id': 0, 'properties.origin_destination': 0, 'properties.way_id': 0, 'properties.edgeID': 0}) if quantity in (None, -1) else collection.find({}, {'_id': 0, 'properties.origin_destination': 0}).limit(quantity)
        return list(alldocs)
    
    def get_all_documents_risk(self, collection_name, quantity, accident_risk):
        collection = self.db[collection_name]
        #queryOne = {'properties.is_hotspot': is_hotspot} if is_hotspot is not None else {}
        queryTwo = {'properties.accident_risk': {'$gte': accident_risk}} if accident_risk is not None else {}

        query = queryTwo

        alldocs = collection.find(query,{'_id': 0}) if quantity in (None, -1) else collection.find(query,{'_id': 0}).limit(quantity)
        return list(alldocs)
    
    def get_all_documents_percentile(self, collection_name, quantity, demand_type, accident_percentile):
        collection = self.db[collection_name]
        queryOne = {'properties.demandType': demand_type.value} if demand_type is not None else {}
        queryTwo = {'properties.percentile_accidents_per_1000_vehicles': {'$gte': accident_percentile}} if accident_percentile is not None else {}

        query = queryOne | queryTwo

        alldocs = collection.find(query,{'_id': 0}) if quantity in (None, -1) else collection.find(query,{'_id': 0}).limit(quantity)
        return list(alldocs)
    
    def get_all_documents_by_type(self, collection_name, event_type: EventType, month, year, percentile, quantity):
        collection = self.db[collection_name]
        query = {'properties.event_type': event_type.value} if event_type is not None else {}
        queryTwo = {'properties.P': {'$gte': percentile}} if percentile is not None else {}

        # Obtener rango de fechas
        if month is None:
            if year is None: year, month = obtener_fecha_mas_reciente(collection, date_field="properties.start_date")
            else: month = obtener_mes_mas_reciente(collection, year, date_field="properties.start_date")
        else:
            if year is None: year, _ = obtener_fecha_mas_reciente(collection, date_field="properties.start_date")

        if year is None or month not in range(1, 13): return []
        fecha_inicio = datetime.datetime(year, month, 1)
        fecha_fin = datetime.datetime(year + 1, 1, 1) if month == 12 else datetime.datetime(year, month + 1, 1)

        queryDate = {"properties.start_date": {"$gte": fecha_inicio,"$lt": fecha_fin}}    
        print(fecha_inicio, fecha_fin)
 
        query = query | queryTwo | queryDate

        alldocs = collection.find(query,{'_id': 0}) if quantity in (None, -1) else collection.find(query,{'_id': 0}).limit(quantity)
        return list(alldocs)
    
    def get_conn_vehicle_dangerous_locations(self, collection_name, month, year, percentile, quantity):
        collection = self.db[collection_name]

        # Definir los umbrales específicos para cada tipo de evento
        P_min_values = {
            "cornering_right": percentile[0],
            "cornering_left": percentile[1],
            "brake": percentile[2],
            "speedup": percentile[3],
        }

        # Construir condiciones de filtrado dinámicamente
        filter_conditions = []
        for event_type, P_min in P_min_values.items():
            if P_min > 0:  # Solo considerar eventos con umbral mayor a 0
                filter_conditions.append({
                    "$gt": [
                        {
                            "$size": {
                                "$filter": {
                                    "input": "$events",
                                    "as": "event",
                                    "cond": {
                                        "$and": [
                                            {"$eq": ["$$event.event_type", event_type]},
                                            {"$gte": ["$$event.P", P_min]}
                                        ]
                                    }
                                }
                            }
                        }, 0
                    ]
                })

        pipeline = [
            # Filtrar solo "intersection"
            # {"$match": {"properties.locationType": "intersection"}},
            
            # Agrupar por locationID, incluyendo geometry y eventos
            {
                "$group": {
                    "_id": "$properties.locationID",
                    "type": {"$first": "$type"}, 
                    "geometry": {"$first": "$geometry"},  # Tomamos la primera geometría para el locationID
                    "properties": {
                        "$first": {
                            "locationID": "$properties.locationID",
                            "locationType": "$properties.locationType"
                        }
                    },
                    "events": {
                        "$push": {
                            "event_type": "$properties.event_type",
                            "P": "$properties.P",
                            "data": {
                                "start_date": "$properties.start_date",
                                "end_date": "$properties.end_date",
                                "creation_date": "$properties.creation_date",
                                "event_count": "$properties.event_count"
                            }
                        }
                    }
                }
            },

            # Aplicar filtrado dinámico si hay condiciones (evita filtrar si todos los D_min son 0)
            {"$match": {"$expr": {"$and": filter_conditions}}} if filter_conditions else None,

            # Proyección final: Mantener sólo locationID, geometry y eventos filtrados
            {
                "$project": {
                    "_id": 0,
                    "type": 1,
                    "geometry": 1,
                    "properties": {
                        "locationID": "$properties.locationID",
                        "locationType": "$properties.locationType",
                        "events": "$events"
                    }
                }
            }
        ]

        # Eliminar None en caso de que no se haya aplicado el filtrado
        pipeline = [stage for stage in pipeline if stage]

        if quantity not in (None, -1): 
            pipeline.append({"$limit": quantity})

        # Ejecutar la consulta
        alldocs = collection.aggregate(pipeline)
        return list(alldocs)
    
    def get_documents_within_area(self, collection_name, geometry, accident_risk):
        collection = self.db[collection_name]
        #queryOne = {'properties.is_hotspot': is_hotspot} if is_hotspot is not None else {}
        queryTwo = {'properties.accident_risk': {'$gte': accident_risk}} if accident_risk is not None else {}
        print(geometry)
        queryGeo = {'geometry': {'$geoWithin': {'$geometry': geometry}}}

        query =  queryTwo | queryGeo

        alldocs = collection.find(query,{'_id': 0})
        return list(alldocs)

    def find_document(self, collection_name, query):
        collection = self.db[collection_name]
        return collection.find_one(query)

    def update_document(self, collection_name, query, update_data):
        collection = self.db[collection_name]
        collection.update_one(query, {"$set": update_data})

    def delete_document(self, collection_name, query):
        collection = self.db[collection_name]
        collection.delete_one(query)

    def close_connection(self):
        self.client.close()

# Función para obtener el primer y último día del mes de una fecha dada
def get_month_range(date):
    first_day = datetime.datetime(date.year, date.month, 1)
    last_day = datetime.datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1], 23, 59, 59)
    return first_day, last_day

def obtener_mes_mas_reciente(collection, year, date_field="properties.date"):
    """Obtiene el mes más reciente disponible para un año dado en la colección, basado en un campo de fecha específico."""
    doc = collection.find_one(
        {date_field: {"$gte": datetime.datetime(year, 1, 1), "$lt": datetime.datetime(year + 1, 1, 1)}},
        sort=[(date_field, -1)],  # Ordena por fecha descendente
        projection={date_field: 1, "_id": 0}
    )
    # Descomponer el campo en niveles si es un campo anidado
    keys = date_field.split(".")
    if doc:
        fecha = doc
        for key in keys:
            if key in fecha:
                fecha = fecha[key]
            else:
                return None  # Si el campo no existe, retorna None 
        return fecha.month
    return None  # Si no hay documentos o la fecha no es válida

def obtener_fecha_mas_reciente(collection, date_field="properties.date"):
    """Obtiene el año y mes más reciente disponible en la colección basado en un campo de fecha específico."""
    doc = collection.find_one(
        {}, 
        sort=[(date_field, -1)],  # Ordena por fecha descendente
        projection={date_field: 1, "_id": 0}
    )
    # Descomponer el campo en niveles si es un campo anidado
    keys = date_field.split(".")
    if doc:
        fecha = doc
        for key in keys:
            if key in fecha:
                fecha = fecha[key]
            else:
                return None, None  # Si el campo no existe, retorna None
        return fecha.year, fecha.month
    return None, None  # Si no hay documentos o la fecha no es válida

# Obtiene el mes más reciente disponible para un año dado en la colección, cuando el campo está dentro de un array
def obtener_mes_mas_reciente_array(collection, year, array_field="properties.predictions", date_field="prediction.start_period"):
    docs = collection.find({}, {array_field: 1, "_id": 0})
    meses = []
    
    for doc in docs:
        # Extract nested field using dot notation
        arr = doc
        for field_part in array_field.split("."):
            if field_part in arr:
                arr = arr[field_part]
            else:
                arr = []
                break
            
        if not isinstance(arr, list):
            continue
            
        for elem in arr:
            fecha = elem
            for key in date_field.split("."):
                if key in fecha:
                    fecha = fecha[key]
                else:
                    fecha = None
                    break
            
            if fecha and isinstance(fecha, datetime.datetime) and fecha.year == year:
                meses.append(fecha.month)
    
    return max(meses) if meses else None

# Obtiene el año y mes más reciente disponible en la colección cuando el campo está dentro de un array
def obtener_fecha_mas_reciente_array(collection, array_field="properties.predictions", date_field="prediction.start_period"):
    docs = collection.find({}, {array_field: 1, "_id": 0})
    fechas = []
    
    for doc in docs:
        # Extract nested field using dot notation
        arr = doc
        for field_part in array_field.split("."):
            if field_part in arr:
                arr = arr[field_part]
            else:
                arr = []
                break
            
        if not isinstance(arr, list):
            continue
            
        for elem in arr:
            fecha = elem
            for key in date_field.split("."):
                if key in fecha:
                    fecha = fecha[key]
                else:
                    fecha = None
                    break
            
            if fecha and isinstance(fecha, datetime.datetime):
                fechas.append(fecha)
    
    if fechas:
        fecha_max = max(fechas)
        return fecha_max.year, fecha_max.month
    
    return None, None

# Insertar documento
#    data_to_insert = {"nombre": "Ejemplo", "edad": 30}
#    inserted_id = db_manager.insert_document("nombre_de_la_coleccion", data_to_insert)
#    print("ID del documento insertado:", inserted_id)

# Leer documento
#    query = {"nombre": "Ejemplo"}
#    result = db_manager.find_document("nombre_de_la_coleccion", query)
#    print("Documento encontrado:", result)

# Actualizar documento
#    update_query = {"nombre": "Ejemplo"}
#    update_data = {"edad": 31}
#    db_manager.update_document("nombre_de_la_coleccion", update_query, update_data)

# Eliminar documento
#    delete_query = {"nombre": "Ejemplo"}
#    db_manager.delete_document("nombre_de_la_coleccion", delete_query)

# Cerrar conexión
#    db_manager.close_connection()