# 📘 Data Dictionary — Silver Layer (v1)

This document defines the minimum schema contract for the Silver layer.
All listed columns are required and validated during pipeline execution.

---

# 🚗 Vehicles Dataset — Silver v1

**Granularity:**  
1 row = 1 vehicle involved in a collision

**Primary Key:**  
UNIQUE_ID

---

## Identifiers

### UNIQUE_ID
- Description: Unique identifier for the vehicle record.
- Role: Primary key of the Vehicles dataset.
- Expected Type: Integer (nullable)
- Source Column: UNIQUE_ID

### COLLISION_ID
- Description: Identifier linking the vehicle to a specific collision event.
- Role: Foreign key to Crashes dataset.
- Expected Type: Integer (nullable)
- Source Column: COLLISION_ID

---

## Vehicle Information

### VEHICLE_TYPE
- Description: Type/category of the vehicle involved in the collision (e.g., Sedan, Truck, Motorcycle).
- Expected Type: String (categorical)
- Normalization: Trimmed and standardized to snake_case in Silver.

### VEHICLE_MAKE
- Description: Manufacturer of the vehicle.
- Expected Type: String
- Normalization: Trimmed

### VEHICLE_YEAR
- Description: Manufacturing year of the vehicle.
- Expected Type: Integer (Int64)
- Validation Rules:
  - Must be numeric
  - Must be within reasonable year range
  - Invalid values are coerced to NULL

### STATE_REGISTRATION
- Description: State where the vehicle is registered.
- Expected Type: String (2–3 character code)
- Normalization: Trimmed

---

## Damage Information

### VEHICLE_DAMAGE
- Description: Primary area of damage on the vehicle.
- Expected Type: String

### VEHICLE_DAMAGE_1
- Description: Secondary damage area.
- Expected Type: String

### VEHICLE_DAMAGE_2
- Description: Additional damage area.
- Expected Type: String

### VEHICLE_DAMAGE_3
- Description: Additional damage area.
- Expected Type: String

---

# 💥 Crashes Dataset — Silver v1

**Granularity:**  
1 row = 1 collision event

**Primary Key:**  
COLLISION_ID

---

## Identifiers

### UNIQUE_ID
- Description: Vehicle identifier associated with the crash.
- Role: Foreign key to Vehicles dataset (when applicable).
- Expected Type: Integer (nullable)

### COLLISION_ID
- Description: Unique identifier for the collision event.
- Role: Primary key of the Crashes dataset.
- Expected Type: Integer

---

## Temporal Information

### CRASH_DATE
- Description: Date when the collision occurred.
- Expected Type: Datetime
- Transformation:
  - Parsed using pd.to_datetime
  - Invalid formats coerced to NULL
- Validation Rules:
  - Cannot be future date
  - NULL values flagged in DQ
- Derived Columns:
  - crash_year (Int64)

### CRASH_TIME
- Description: Time of the collision.
- Expected Type: String (HH:MM or HH:MM:SS)
- Transformation:
  - Normalized to HH:MM or HH:MM:SS
  - Invalid formats converted to NULL

### crash_year (Derived)
- Description: Year extracted from crash_date.
- Expected Type: Integer (Int64)
- Usage:
  - Partition column for Silver storage
  - Aggregation key in Gold layer

---

## Crash Context

### PRE_CRASH
- Description: Description of vehicle action before the crash.
- Expected Type: String

### TRAVEL_DIRECTION
- Description: Direction the vehicle was traveling.
- Expected Type: String

### POINT_OF_IMPACT
- Description: Location on the vehicle where the impact occurred.
- Expected Type: String

---

## Contributing Factors

### CONTRIBUTING_FACTOR_1
- Description: Primary contributing factor to the crash.
- Expected Type: String (categorical)

### CONTRIBUTING_FACTOR_2
- Description: Secondary contributing factor.
- Expected Type: String (nullable)

---

# 🔒 Data Quality Enforcement (Silver Layer)

The following validations are applied:

- Required column existence check
- Safe numeric casting with coercion
- Date parsing with future-date validation
- Time normalization (HH:MM format enforcement)
- String trimming
- Invalid records redirected to Quarantine
- Metrics generated per execution

---

# 📦 Partition Strategy

Vehicles:
- Partitioned by run_date

Crashes:
- Partitioned by crash_year

---

# 🔗 Dataset Relationships

Vehicles.COLLISION_ID  →  Crashes.COLLISION_ID

This relationship enables Gold-layer analytical models such as:

- Crash severity aggregation
- Vehicle risk profiling
- Contributing factor correlation
- Year-based trend analysis