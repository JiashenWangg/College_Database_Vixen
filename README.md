# College Database Project -- Vixen

## Overview

This project creates a comprehensive PostgreSQL database for tracking college and university data across the United States. The database is designed to help **prospective students and their families** make informed decisions about college selection, as well as assist **admissions professionals** in understanding institutional trends and benchmarking against peer institutions. We also update the IPEDS table whenever it is ran again and the AccredAgency will take the most recent values. 

## Database Purpose

The database consolidates data from two primary sources:
- **IPEDS (Integrated Postsecondary Education Data System)** - Institutional characteristics
- **College Scorecard** - Annual performance metrics (2018-2021)

This allows users to:
- Compare colleges based on admissions rates, costs, and outcomes
- Track trends in tuition, student demographics, and academic offerings over time
- Identify colleges by location, Carnegie classification, and institutional control
- Analyze student loan default rates and first-generation student populations
- Evaluate faculty salaries and student-to-faculty ratios

## Database Schema

### Entity Design Philosophy

The database uses a **normalized schema** with four main entities, each serving a distinct purpose:

1. **Institutions** (Master Table)
   - Stores relatively static institutional characteristics
   - One-to-many relationship with all other tables
   - Primary key: `institution_id` (OPEID code)

2. **Students** 
   - Annual student body characteristics and demographics
   - Tracks admission rates, enrollment, test scores, and student financial backgrounds
   - Primary key: `(institution_id, year)`

3. **Financials**
   - Annual financial metrics set by the institution
   - Includes tuition rates, faculty salaries, and revenue per student
   - Primary key: `(institution_id, year)`

4. **Academics**
   - Annual academic program characteristics
   - Tracks degree offerings and student-to-faculty ratios
   - Primary key: `(institution_id, year)`

### Why This Split?

While all data could theoretically exist in one table, we chose to **separate concerns** for several reasons:

- **Query Efficiency**: Users interested in financial data don't need to load student demographic information
- **Reduced Redundancy**: Institution characteristics don't repeat for every year
- **Clearer Analysis**: Each table focuses on a specific aspect of institutional performance
- **Easier Maintenance**: Updates to one aspect don't require touching unrelated data

## Database Tables

### Institutions Table
```sql
CREATE TABLE Institutions (
   institution_id TEXT PRIMARY KEY, --- 8-digit OPEID code
   name TEXT NOT NULL, 
   accredagency TEXT, ---accrediting agency
   control INT CHECK (control IN (1, 2, 3)), --- 1=Public, 2=Private nonprofit, 3=Proprietary
   CCbasic INT CHECK (CCbasic <= 33), --- Carnegie classifier
   region INT CHECK (region BETWEEN 0 AND 9),
   csba TEXT CHECK (LENGTH(csba) = 5), --- Core Based Statistical Area
   cba TEXT CHECK (LENGTH(cba) = 5), --- Combined Statistical Area
   county_fips TEXT CHECK (LENGTH(county_fips) = 5), --- County FIPS code
   city TEXT,
   state TEXT CHECK (LENGTH(state) = 2),
   address TEXT,
   zip INT CHECK (zip > 0),
   latitude FLOAT,
   longitude FLOAT
);

```

**Data Source**: IPEDS `hd2022.csv` file
- **Note**: The `accredagency` column requires data from a separate accreditation file (currently loaded with a placeholder value)

### Students Table
```sql
CREATE TABLE Students (
   institution_id INT REFERENCES Institutions(institution_id),
   year INT CHECK (year > 0 AND year <= EXTRACT(YEAR FROM CURRENT_DATE)),
   adm_rate FLOAT CHECK (adm_rate >= 0 and adm_rate <= 1), –-- Admission rate
   num_students INT CHECK (num_students >= 0),  
   act FLOAT CHECK (act >= 1 and act <= 36), –-- ACT exam score
   cdr2 FLOAT CHECK (cdr2 >= 0 and cdr2 <= 1), —-- 2 year default rate of student loans
   cdr3 FLOAT CHECK (cdr3 >= 0 and cdr3 <= 1), —-- 3 year default rate of student loans
   PRIMARY KEY (institution_id, year)
);
```

**Data Source**: College Scorecard MERGED files

### Financials Table
```sql
CREATE TABLE Financials (
   institution_id INT REFERENCES Institutions(institution_id),
   year INT CHECK (year > 0 AND year <= EXTRACT(YEAR FROM CURRENT_DATE)),
   tuitionfee_in INT CHECK (tuitionfee_in >= 0), –-- In state tuition
   tuitionfee_out INT CHECK (tuitionfee_out >= 0), –-- Out of state tuition
   tuitionfee_prog INT CHECK (tuitionfee_prog >= 0), –-- Average tuition for program-year –institutions
   tuitfte INT CHECK (tuitfte >= 0), –-- Net tuition revenue per student
   avgfacsal INT CHECK (avgfacsal >= 0), –-- Average faculty salary
   PRIMARY KEY (institution_id, year)
);

```

**Data Source**: College Scorecard MERGED files

### Academics Table
```sql
CREATE TABLE Academics (
   institution_id TEXT REFERENCES Institutions(institution_id),
   year INT CHECK (year > 0 AND year <= EXTRACT(YEAR FROM CURRENT_DATE)),
   preddeg TEXT CHECK (preddeg BETWEEN 0 AND 4),
   highdeg INT CHECK (highdeg BETWEEN 0 AND 4),
   stufacr FLOAT CHECK (stufacr >= 0 AND stufacr <= 1),
   PRIMARY KEY (institution_id, year)
);
```

**Data Source**: College Scorecard MERGED files

## Data Loading Process

### Prerequisites

1. **Python Environment**:
   ```bash
   conda create -n DEpythonsql python=3.13
   conda activate DEpythonsql
   pip install psycopg2-binary pandas
   ```

2. **Database Connection**:
   - PostgreSQL database on Azure
   - Update connection credentials in both loading scripts

3. **Required Data Files**:
   - `hd2022.csv` - IPEDS institutional directory
   - `MERGED2018_19_PP.csv` - College Scorecard 2018 data
   - `MERGED2019_20_PP.csv` - College Scorecard 2019 data
   - `MERGED2020_21_PP.csv` - College Scorecard 2020 data
   - `MERGED2021_22_PP.csv` - College Scorecard 2021 data

### Step 1: Create Database Schema

Run the SQL commands in `part1.ipynb` to create all tables with proper constraints and relationships.

```bash
# In Jupyter notebook
%load_ext sql
%sql postgresql://username:password@server:5432/database
```

Then execute the cell containing all CREATE TABLE statements.

### Step 2: Load IPEDS Data (Institutions Table)

```bash
python load-ipeds.py hd2022.csv
```

**What this does**:
- Reads the IPEDS institutional characteristics file
- Extracts 15 columns mapping to the Institutions table schema
- Inserts data for all U.S. postsecondary institutions
- **Note**: Currently loads `accredagency` as NULL - requires separate IPEDS accreditation file

**Column Mapping**:
- `OPEID` → `institution_id`
- `INSTNM` → `name`
- `CONTROL` → `control`
- `CCBASIC` → `CCbasic`
- `OBEREG` → `region`
- `CBSA` → `csba`
- `CSA` → `cba`
- `COUNTYCD` → `county_fips`
- `CITY` → `city`
- `STABBR` → `state`
- `ADDR` → `address`
- `ZIP` → `zip`
- `LATITUDE` → `latitude`
- `LONGITUD` → `longitude`

### Step 3: Load College Scorecard Data

**Option A - Single File**:
```bash
python load-scorecard.py MERGED2021_22_PP.csv
```

**Option B - All Files at Once**:
```bash
python load-scorecard.py .
```

**What this does**:
- Automatically extracts the year from the filename (e.g., `MERGED2022.csv` → year = 2022)
- Loads data into Students, Financials, and Academics tables simultaneously
- Handles missing data by converting invalid values to NULL
- Provides detailed error reporting for any failed insertions

**Important File Naming Convention**:
⚠️ **Files MUST follow the pattern**: `MERGED<YEAR>.csv`

Examples of valid filenames:
- ✅ `MERGED2022.csv`
- ✅ `scorecard_2021.csv` 
- ❌ `MERGED_2021_PP.csv` (Won't work)

The script uses regex pattern matching to extract the year: `MERGED(\d{4})_\d{2}_PP\.csv`

The years need to be run sequentially in chronological order otherwise the accredagnecy fields will not be valid.

## Data Relationships

### Foreign Key Relationships

```
Institutions (1) ──────< (Many) Students
     │
     ├──────────────────< (Many) Financials
     │
     └──────────────────< (Many) Academics
```

- Each institution can have **multiple years** of student, financial, and academic data
- Each student/financial/academic record **must reference** a valid institution
- The `institution_id` (OPEID code) serves as the linking key across all tables

### Data Integrity Constraints

All tables include CHECK constraints to ensure data quality:
- Rates and percentages must be between 0 and 1
- Years cannot be in the future
- Test scores must be within valid ranges
- Geographic codes must have proper length
- Financial values cannot be negative


## Use Cases

### For Prospective Students & Families

1. **Compare Colleges by Cost**:
   ```sql
   SELECT i.name, i.state, f.tuitionfee_in, f.tuitionfee_out
   FROM Institutions i
   JOIN Financials f ON i.institution_id = f.institution_id
   WHERE f.year = 2021 AND i.region = 5
   ORDER BY f.tuitionfee_in;
   ```

2. **Find Schools with High Admission Rates**:
   ```sql
   SELECT i.name, s.adm_rate, s.num_students
   FROM Institutions i
   JOIN Students s ON i.institution_id = s.institution_id
   WHERE s.year = 2021 AND s.adm_rate > 0.5
   ORDER BY s.adm_rate DESC;
   ```

3. **Analyze Student Loan Default Rates**:
   ```sql
   SELECT i.name, s.cdr3, s.avg_family_income
   FROM Institutions i
   JOIN Students s ON i.institution_id = s.institution_id
   WHERE s.year = 2021
   ORDER BY s.cdr3;
   ```

### For Admissions Professionals

1. **Benchmark Against Peer Institutions**:
   ```sql
   SELECT i.name, s.adm_rate, a.stufacr, f.avgfacsal
   FROM Institutions i
   JOIN Students s ON i.institution_id = s.institution_id
   JOIN Academics a ON i.institution_id = a.institution_id AND s.year = a.year
   JOIN Financials f ON i.institution_id = f.institution_id AND s.year = f.year
   WHERE s.year = 2021 AND i.CCbasic = 15  -- Research Universities
   ORDER BY s.adm_rate;
   ```

2. **Track Enrollment Trends**:
   ```sql
   SELECT year, AVG(num_students) as avg_enrollment
   FROM Students
   WHERE institution_id = '001002'
   GROUP BY year
   ORDER BY year;
   ```

3. **Regional Analysis**:
   ```sql
   SELECT i.region, COUNT(*) as num_colleges, 
          AVG(f.tuitionfee_out) as avg_tuition
   FROM Institutions i
   JOIN Financials f ON i.institution_id = f.institution_id
   WHERE f.year = 2021
   GROUP BY i.region;
   ```

## Known Limitations & Future Work

1. **Accreditation Data**: 
   - The `accredagency` column in Institutions table currently contains placeholder data
   - Future work: Load from IPEDS accreditation file to populate actual accrediting agencies

2. **Year Coverage**: 
   - Current dataset covers 2018-2021
   - Can be extended by loading additional MERGED files following the naming convention

3. **Data Completeness**:
   - Some institutions may have missing data for certain years or metrics
   - The loading scripts handle missing values by converting to NULL

## Troubleshooting

### Common Issues

1. **"integer out of range" error**:
   - Ensure your Institutions table uses TEXT for `institution_id`, not INT
   - Some OPEID codes exceed INT limits

2. **Foreign key constraint violations**:
   - Run `load-ipeds.py` BEFORE `load-scorecard.py`
   - Scorecard data references institutions that must exist first

3. **"Year not found in filename" error**:
   - Verify your College Scorecard files follow the `MERGED<YEAR>_<YR>_PP.csv` pattern
   - Check that files aren't renamed

4. **Connection timeout**:
   - These are large files (50-200MB each)
   - Allow 5-15 minutes per file for loading
   - Check your network connection to Azure

## Project Structure

```
College_Database_Vixen/
├── README.md                   # This file
├── create_tables.ipynb         # Database schema definition
├── load-ipeds.py               # IPEDS data loader
├── load-scorecard.py           # College Scorecard data loader
├── hd2022.csv                  # IPEDS institutional directory
├── MERGED2019.csv              # College Scorecard 2018
├── MERGED2020.csv              # College Scorecard 2019
├── MERGED2021.csv              # College Scorecard 2020
└── MERGED2022.csv              # College Scorecard 2021
```

## Contributors

Pramit V. - Database design and ETL development
Jiashen W - Database design and ETL development
Adam G. - Database design and ETL development

## Data Sources

- **IPEDS**: U.S. Department of Education, National Center for Education Statistics
- **College Scorecard**: U.S. Department of Education

## License

Data sourced from public U.S. government databases. This project is for educational purposes.
