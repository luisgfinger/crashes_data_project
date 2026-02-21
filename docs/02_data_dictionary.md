# Data Dictionary (Minimum Target Columns) — Vehicles Dataset

## Identifiers

### UNIQUE_ID
- Description: Unique identifier for the vehicle record.
- Role: Primary key of the Vehicles dataset.

### COLLISION_ID
- Description: Unique identifier of the collision event.
- Role: Foreign key used to join with the Crashes dataset.

---

## Time

### CRASH_DATE
- Description: Date when the collision occurred.
- Original Type (Bronze): String
- Expected Type (Silver): Date
- Notes: Will be combined with CRASH_TIME to create CRASH_DATETIME.

### CRASH_TIME
- Description: Time when the collision occurred.
- Original Type (Bronze): String
- Expected Type (Silver): Time
- Notes: Will be combined with CRASH_DATE to create CRASH_DATETIME.

### CRASH_DATETIME
- Description: Combined date and time of the collision.
- Source: Derived from CRASH_DATE + CRASH_TIME.
- Created In: Silver Layer
- Expected Type: Datetime

---

## Vehicle Information

### VEHICLE_TYPE
- Description: Type/category of the vehicle involved in the collision (e.g., Sedan, Truck, Motorcycle).
- Expected Type: String (categorical)

### VEHICLE_MAKE
- Description: Manufacturer of the vehicle.
- Expected Type: String

### VEHICLE_MODEL
- Description: Model name of the vehicle.
- Expected Type: String

### VEHICLE_YEAR
- Description: Manufacturing year of the vehicle.
- Expected Type: Integer

---

## Driver Information

### DRIVER_SEX
- Description: Reported sex of the driver.
- Expected Type: String (categorical)

### DRIVER_LICENSE_STATUS
- Description: Status of the driver's license at the time of the collision.
- Expected Type: String (categorical)

### DRIVER_LICENSE_JURISDICTION
- Description: Issuing jurisdiction/state of the driver's license.
- Expected Type: String

---

## Contributing Factors

### CONTRIBUTING_FACTOR_1
- Description: Primary contributing factor associated with the vehicle in the collision.
- Expected Type: String (categorical)

### CONTRIBUTING_FACTOR_2
- Description: Secondary contributing factor associated with the vehicle.
- Expected Type: String (categorical)