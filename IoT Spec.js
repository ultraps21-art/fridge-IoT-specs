BASE_TIME_GRANULARITY 1second
T_now SYSTEM_CLOCK

Resources: Fridge_Zones, Sensor_deployment, Fridge_Inventory
//3 relational tables 

CREATE TABLE Fridge_Zones ( 
zone_id INTEGER PRIMARY KEY, //unique integer ID to each compartment of fridge
zone_name ENUM, // "Poultry_Drawer", "Veggie_Drawer", "Fruit Drawer"
zone_location ENUM // "Bottom_Shelf", "Top_Shelf", "Middle_Shelf"
);

CREATE TABLE Sensor_Deployment ( 
device_id STRING PRIMARY KEY, //unique hardwware identifier to each sensor
device_class STRING, //name of the sensor
zone_id INTEGER FOREIGN KEY REFERENCES Fridge_Zones(zone_id)  // links sensor to valid zone_id
);

CREATE TABLE Fridge_Inventory ( 
ItemID STRING PRIMARY KEY, //unique identifier to each food item
ItemType ENUM, -- e.g., 'Poultry','Produce'  //Produce covers both Veggies in zone 2 and fruits in zone 3
ItemName STRING, -- e.g., 'Raw_Chicken' (Zone 1), 'Spinach' (Zone 2), 'Apples' (Zone 3) //stores speific item for filtering
zone_id INTEGER FOREIGN KEY REFERENCES Fridge_Zones(zone_id), // records which compartment holds item
DateAdded INTEGER -- Timestamp (in BASE_TIME_GRANULARITY units) when placed in fridge //timestamp in seconds when food was placed in fridge
);


//event schemas

CREATE EVENTTYPE Ethylene_Reading (
    EVENTTIME 1, //declares timestamp recording when sensor reading occurs
    device_id STRING FOREIGN KEY REFERENCES Sensor_Deployment(device_id), // identifies the specific sensor unit by referencing Sensor_Deployment
    zone_id INTEGER FOREIGN KEY REFERENCES Fridge_Zones(zone_id), //identifies fridge compartment where the reading came from
    ppm_value FLOAT, // stores gas concentration as decimal
    device_id -> zone_id // functional dependency: device_id determines its zone_id
)

//define the raw data sent by physical gas sensors

CREATE EVENTTYPE Ammonia_Reading (
    EVENTTIME 1, //timstamp when alert generated
    device_id STRING FOREIGN KEY REFERENCES Sensor_Deployment(device_id), 
    zone_id INTEGER FOREIGN KEY REFERENCES Fridge_Zones(zone_id), //finds specific fridge zone experiencing high ammonia concentration
    ppm_value FLOAT, //stores computed avg ammonia value across observed window
    device_id -> zone_id 
)

//Triggered Event: Intermediate window alert for elevated ammonia in a zone
CREATE EVENTTYPE High_Ammonia_Alert (
    EVENTTIME 1, // timestamp when alert was generated
    zone_id INTEGER, // finds fridge experiencing high ammonia concentration
    avg_ppm FLOAT // stores avg ammonia value across time window
)

//Triggered Event: Final cross-referenced spoilage alert for raw chicken
CREATE EVENTTYPE ChickenSpoiledAlert (
    EVENTTIME 1, // timestamp when spoilage alert triggered
    ItemID STRING, // exact item that was spoiled
    zone_id INTEGER, // fridge zone spoiled food is in
    avg_ammonia_ppm FLOAT, // gas reading that triggered alert
    age_seconds INTEGER // length of spoiled item in fridge (sec)
)

//Triggered Event: Final cross-referenced spoilage alert for produce
CREATE EVENTTYPE ProduceOverripeAlert (
    EVENTTIME 1,
    ItemID STRING,
    zone_id INTEGER,
    avg_ethylene_ppm FLOAT,
    age_seconds INTEGER
)

//DEVICE CLASSES

// ethylene sensor for tracking fruit/vegetable ripening
CREATE DEVICE CLASS Ethylene_Sensor (
    MEASURES Ethylene_Reading, // specifies which EVENTTYPE schema this class emits
    UNIT "ppm", // declares unit of measurement
    RANGE (0.0, 100.0), // min and max values the sensor can read, excludes values outside range
    RESOLUTION 0.05 // smallest detectable change in ppm
    zone ENUM *
)

// ammonia sensor for detecting meat bacterial breakdown
CREATE DEVICE CLASS Ammonia_Sensor (
    MEASURES Ammonia_Reading,
    UNIT "ppm",
    RANGE (0.0, 50.0),
    RESOLUTION 0.1 // smallest detectable change in ppm
    zone ENUM *
)

//EVENT STREAM
CREATE EVENTSTREAM fridge_sensor_stream (
    EVENTTYPE Ethylene_Reading,
    EVENTTYPE Ammonia_Reading,
    ARRIVALS (
        Ethylene_Reading PERIODIC (1 MIN), // transmits every 1 min
        Ammonia_Reading PERIODIC (1 MIN) // transmits every 1 min
    )
)

//DISTRIBUTIONS

// Ammonia distribution for Zone 1 (Poultry_Drawer on the Bottom_Shelf)
DISTRIBUTION FOR Ammonia_Reading.ppm_value (
    WHERE zone_id = 1 (
        NORMAL (mean = 0.8, std_dev = 0.2) PROB 0.85
        ABOVE (
            NORMAL (mean = 18.5, std_dev = 3.0) PROB 0.12 // spoilage spike (> 12.0 ppm threshold)
        )
        BELOW (
            UNIFORM (low = 0.0, high = 0.2) PROB 0.03 // near-zero sensor dropout
        )
    )
    FAILURE 0.02
)

// Ethylene distribution for Zone 2 (Veggie_Drawer on the Top_Shelf) and Zone 3 (Fruit_Drawer on the Middle_Shelf)
DISTRIBUTION FOR Ethylene_Reading.ppm_value (
    WHERE zone_id = 2 (
        NORMAL (mean = 0.8, std_dev = 0.3) PROB 0.85 // veggies have low baseline ethylene
        ABOVE (
            NORMAL (mean = 20.0, std_dev = 3.5) PROB 0.10 // rotting veggie spike (> 15.0 ppm threshold)
        )
        BELOW (
            UNIFORM (low = 0.0, high = 0.2) PROB 0.05
        )
    )
    WHERE zone_id = 3 (
        NORMAL (mean = 2.0, std_dev = 0.5) PROB 0.80 // fruits naturally emit slightly more baseline ethylene
        ABOVE (
            NORMAL (mean = 25.0, std_dev = 4.5) PROB 0.15 // overripe fruit spike (> 15.0 ppm threshold)
        )
        BELOW (
            UNIFORM (low = 0.0, high = 0.3) PROB 0.05
        )
    )
    FAILURE 0.02
)

//RULES FOR TRIGGERED EVENTS

// Rule 1 (Direct Sensor Fusion): Trigger ChickenSpoiledAlert when average ammonia in a zone
// over a 15-minute (900s) sliding window enters the Abnormal High tier (> 12.0 ppm)
// AND Fridge_Inventory shows Poultry ('Raw_Chicken') added >= 10 days (864000s) ago.

new ChickenSpoiledAlert [SLIDING (s, 900)] ([Ammonia_Reading.zone_id, Fridge_Inventory.ItemID], [AVG(Ammonia_Reading.ppm_value)]) @ s+900+1 :-

// new keyword generates event of that type
// evaluates sliding time window start @ s, duration 900 sec
// groups readings by specific attributes, calculates aggregates (max or avg, etc)
// emites triggered event 1 second after (s,900) completes
    Ammonia_Reading @ t,
    Fridge_Inventory.zone_id = Ammonia_Reading.zone_id,
    Fridge_Inventory.ItemType = 'Poultry',
    Fridge_Inventory.ItemName = 'Raw_Chicken',
    (T_now - Fridge_Inventory.DateAdded) >= 864000, // checks chicken was added at least 10 days ago
    AVG(Ammonia_Reading.ppm_value) > 12.0 // 15 min avg exceeds 12.0 ppm

// Rule 2 (Chained Two-Stage Alert):
// Stage A: Flag High_Ammonia_Alert if average ammonia > 12.0 ppm over 15 minutes (900s)
new High_Ammonia_Alert [SLIDING (s, 900)] ([Ammonia_Reading.zone_id], [AVG(Ammonia_Reading.ppm_value)]) @ s+900+1 :-
    Ammonia_Reading @ t,
    AVG(Ammonia_Reading.ppm_value) > 12.0


// Stage B: Chain High_Ammonia_Alert with Fridge_Inventory lookup to emit ChickenSpoiledAlert
new ChickenSpoiledAlert [SLIDING (s, 60)] ([High_Ammonia_Alert.zone_id, Fridge_Inventory.ItemID], [MAX(High_Ammonia_Alert.avg_ppm)]) @ s+60+1 :-
    High_Ammonia_Alert @ t,
    Fridge_Inventory.zone_id = High_Ammonia_Alert.zone_id,
    Fridge_Inventory.ItemType = 'Poultry',
    (T_now - Fridge_Inventory.DateAdded) >= 864000


// Rule 3 (Produce Ripening / Spoilage): Trigger ProduceOverripeAlert when average ethylene
// over 30 minutes (1800s) exceeds 15.0 ppm and Produce has been stored >= 7 days (604800s).
new ProduceOverripeAlert [SLIDING (s, 1800)] ([Ethylene_Reading.zone_id, Fridge_Inventory.ItemID], [AVG(Ethylene_Reading.ppm_value)]) @ s+1800+1 :-
    Ethylene_Reading @ t,
    Fridge_Inventory.zone_id = Ethylene_Reading.zone_id,
    Fridge_Inventory.ItemType = 'Produce',
    (T_now - Fridge_Inventory.DateAdded) >= 604800,
    AVG(Ethylene_Reading.ppm_value) > 15.0
