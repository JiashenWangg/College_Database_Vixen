# College_Database_Vixen
# College Database Project

A comprehensive PostgreSQL database system for analyzing U.S. higher education institutions, designed to help prospective students, families, and admissions professionals make informed decisions about college selection and planning.

## Project Overview

This database integrates data from two authoritative sources:
- **IPEDS (Integrated Postsecondary Education Data System)**: Institutional characteristics and basic information
- **College Scorecard**: Annual performance metrics including student outcomes, financials, and academics

The system tracks over 6,000 institutions across multiple years (2018-2025), providing a rich dataset for comparative analysis and decision-making.

## Database Schema

Our schema is organized into four primary entities, each capturing a distinct aspect of higher education institutions:

### 1. **Institutions Table** (Master Reference)
Stores static institutional characteristics that rarely change:
- **Purpose**: Centralized reference table for basic college information
- **Primary Key**: `institution_id` (8-digit OPEID code)
- **Key Attributes**:
  - Name, accrediting agency, control type (public/private/proprietary)
  - Carnegie Classification (research activity level)
  - Geographic information (region, CBSA, county, city, state)
  - Physical location (address, coordinates)

**Why separate?** This design enables efficient one-to-many joins with temporal tables while avoiding data redundancy for information that doesn't change annually.

### 2. **Students Table** (Annual Snapshots)
Tracks student body characteristics and outcomes by year:
- **Primary Key**: `(institution_id, year)`
- **Foreign Key**: References `Institutions(institution_id)`
- **Key Metrics**:
  - Admission rate
  - Total enrollment
  - Average ACT scores
  - Student loan default rates (2-year and 3-year)
  - First-generation student percentage
  - Average family income

**Use Cases**:
- Compare selectivity trends over time
- Assess student body diversity and socioeconomic composition
- Evaluate loan repayment success rates

### 3. **Financials Table** (Annual Cost Data)
Captures institutional pricing and faculty compensation:
- **Primary Key**: `(institution_id, year)`
- **Foreign Key**: References `Institutions(institution_id)`
- **Key Metrics**:
  - In-state and out-of-state tuition
  - Program-specific tuition averages
  - Net tuition revenue per student
  - Average faculty salaries

**Use Cases**:
- Budget planning for prospective students
- Cost comparison across similar institutions
- Understanding pricing strategies and trends
- Faculty compensation analysis

### 4. **Academics Table** (Annual Program Data)
Tracks degree offerings and classroom metrics:
- **Primary Key**: `(institution_id, year)`
- **Foreign Key**: References `Institutions(institution_id)`
- **Key Metrics**:
  - Predominant degree awarded (certificate through doctoral)
  - Highest degree offered
  - Student-to-faculty ratio

**Use Cases**:
- Identify institutions by degree level
- Assess classroom size and faculty availability
- Compare academic focus across institutions

## Entity-Relationship Design

Our schema follows these key principles:

1. **Separation of Concerns**: Each table focuses on a specific domain (institutional identity, student characteristics, finances, academics)

2. **Temporal Design**: Three tables track year-over-year changes while maintaining historical data for trend analysis

3. **Referential Integrity**: Foreign key constraints ensure data consistency and enable reliable joins

4. **Normalized Structure**: Reduces redundancy while maintaining query flexibility

**Relationship Diagram**:
```
Institutions (1) ----< (Many) Students
      |
      +-------------< (Many) Financials
      |
      +-------------< (Many) Academics
```

## Data Loading Process

### Prerequisites
```bash
# Create conda environment
conda create -n DEpythonsql python=3.13
conda activate DEpythonsql

# Install required packages
pip install pandas psycopg2-binary sqlalchemy ipython-sql
```

### Step 1: Load IPEDS Institutional Data
```bash
# Load institution master data (run once)
python load-ipeds.py hd2022.csv
```

**What it does**:
- Extracts institutional characteristics from IPEDS Directory data
- Populates the `Institutions` table with ~6,000+ colleges
- Maps IPEDS fields to database schema:
  - `OPEID` → `institution_id`
  - `INSTNM` → `name`
  - `CONTROL` → `control`
  - Geographic fields → location attributes

### Step 2: Load College Scorecard Annual Data
```bash
# Load single year
python load-scorecard.py MERGED2021_22_PP.csv

# Load all years (2018-2021)
python load-scorecard.py .
```

**What it does**:
- Automatically extracts year from filename (e.g., `MERGED2021_22_PP.csv` → 2021)
- Populates three tables simultaneously:
  - **Students**: Admission rates, enrollment, test scores, default rates, demographics
  - **Financials**: Tuition, fees, faculty salaries
  - **Academics**: Degree levels, student-faculty ratios
- Handles missing data gracefully (converts to NULL)
- Provides detailed error reporting for data quality issues

**Data Cleaning Features**:
- Converts "PrivacySuppressed" and invalid values to NULL
- Validates numeric ranges against CHECK constraints
- Filters out institutions not in the master `Institutions` table (foreign key enforcement)

## Key Features for Users

### For Prospective Students & Families

**1. Cost Comparison**
```sql
-- Compare tuition across states for a specific region
SELECT i.state, AVG(f.tuitionfee_out) as avg_out_state_tuition
FROM Institutions i
JOIN Financials f ON i.institution_id = f.institution_id
WHERE i.region = 5 AND f.year = 2021
GROUP BY i.state
ORDER BY avg_out_state_tuition;
```

**2. Selectivity Analysis**
```sql
-- Find moderately selective schools with good outcomes
SELECT i.name, i.state, s.adm_rate, s.cdr3
FROM Institutions i
JOIN Students s ON i.institution_id = s.institution_id
WHERE s.year = 2021 
  AND s.adm_rate BETWEEN 0.4 AND 0.6
  AND s.cdr3 < 0.05
ORDER BY s.cdr3;
```

**3. Affordability for First-Gen Students**
```sql
-- Schools with high first-gen support and low costs
SELECT i.name, s.first_gen, f.tuitionfee_in, s.avg_family_income
FROM Institutions i
JOIN Students s ON i.institution_id = s.institution_id
JOIN Financials f ON i.institution_id = f.institution_id 
WHERE s.year = 2021 AND f.year = 2021
  AND s.first_gen > 0.3
  AND f.tuitionfee_in < 15000
ORDER BY s.first_gen DESC;
```

### For Admissions Professionals

**1. Competitive Benchmarking**
```sql
-- Compare institution against peer group
SELECT i.name, s.adm_rate, s.act, s.num_students, a.stufacr
FROM Institutions i
JOIN Students s ON i.institution_id = s.institution_id
JOIN Academics a ON i.institution_id = a.institution_id
WHERE i.CCbasic = 15  -- Research Universities
  AND s.year = 2021 AND a.year = 2021
  AND i.region = 3
ORDER BY s.adm_rate;
```

**2. Trend Analysis**
```sql
-- Track enrollment trends over time
SELECT year, SUM(num_students) as total_enrollment
FROM Students
WHERE institution_id = '001002'  -- Specific institution
GROUP BY year
ORDER BY year;
```

**3. Pricing Strategy Analysis**
```sql
-- Net revenue efficiency
SELECT i.name, f.tuitfte, s.num_students, 
       (f.tuitfte * s.num_students) as total_revenue
FROM Institutions i
JOIN Financials f ON i.institution_id = f.institution_id
JOIN Students s ON i.institution_id = s.institution_id
WHERE f.year = 2021 AND s.year = 2021
  AND i.control = 1  -- Public institutions
ORDER BY total_revenue DESC;
```

## Data Integrity & Constraints

### CHECK Constraints
- **Control types**: Limited to (1=Public, 2=Private nonprofit, 3=Proprietary)
- **Rates**: All percentage fields constrained between 0.0 and 1.0
- **ACT scores**: Range 1-36
- **Years**: Cannot exceed current year
- **Counts**: All student/financial counts must be non-negative
- **Geographic codes**: State codes must be 2 characters, FIPS codes 5 characters

### Foreign Key Constraints
- All temporal tables reference `Institutions(institution_id)`
- Ensures data consistency: Cannot insert student/financial/academic data for non-existent institutions
- Cascade behavior: Institutional changes maintain data integrity

### Benefits
- **Data Quality**: Prevents invalid or impossible values
- **Query Reliability**: Analysts can trust data ranges
- **Debugging**: Constraint violations provide immediate feedback during data loading

## Performance Considerations

### Indexing Strategy
```sql
-- Primary key indexes (automatic)
-- Recommended additional indexes:
CREATE INDEX idx_institutions_region ON Institutions(region);
CREATE INDEX idx_institutions_state ON Institutions(state);
CREATE INDEX idx_institutions_carnegie ON Institutions(CCbasic);
CREATE INDEX idx_students_year ON Students(year);
CREATE INDEX idx_financials_year ON Financials(year);
CREATE INDEX idx_academics_year ON Academics(year);
```

### Query Optimization
- Use composite primary keys efficiently for temporal queries
- Join on indexed foreign keys for best performance
- Filter by year early in WHERE clauses for time-series analysis

## File Structure
```
College_Database_Vixen/
├── README.md                    # This file
├── part1.ipynb                  # Schema definition and documentation
├── load-ipeds.py               # IPEDS data loader
├── load-scorecard.py           # College Scorecard data loader
├── hd2022.csv                  # IPEDS institutional data (2022)
├── MERGED2018_19_PP.csv        # College Scorecard data (2018)
├── MERGED2019_20_PP.csv        # College Scorecard data (2019)
├── MERGED2020_21_PP.csv        # College Scorecard data (2020)
└── MERGED2021_22_PP.csv        # College Scorecard data (2021)
```

## Database Connection

**Azure PostgreSQL Configuration**:
```python
host = "debprodserver.postgres.database.azure.com"
database = "pramitv"
user = "pramitv"
port = 5432
```

**Connection String**:
```
postgresql://pramitv:PASSWORD@debprodserver.postgres.database.azure.com:5432/pramitv
```

## Common Queries

### Example 1: Regional Cost Analysis
```sql
-- Average costs by region for public universities
SELECT i.region,
       AVG(f.tuitionfee_in) as avg_in_state,
       AVG(f.tuitionfee_out) as avg_out_state,
       COUNT(*) as num_schools
FROM Institutions i
JOIN Financials f ON i.institution_id = f.institution_id
WHERE i.control = 1 AND f.year = 2021
GROUP BY i.region
ORDER BY i.region;
```

### Example 2: Academic Excellence with Affordability
```sql
-- High-performing schools with reasonable costs
SELECT i.name, i.state, s.act, s.adm_rate, f.tuitionfee_out
FROM Institutions i
JOIN Students s ON i.institution_id = s.institution_id
JOIN Financials f ON i.institution_id = f.institution_id
WHERE s.year = 2021 AND f.year = 2021
  AND s.act > 28
  AND f.tuitionfee_out < 40000
ORDER BY s.act DESC, f.tuitionfee_out;
```

### Example 3: Debt Burden Assessment
```sql
-- Schools with low default rates by control type
SELECT i.control,
       AVG(s.cdr3) as avg_default_rate,
       AVG(f.tuitionfee_prog) as avg_tuition
FROM Institutions i
JOIN Students s ON i.institution_id = s.institution_id
JOIN Financials f ON i.institution_id = f.institution_id
WHERE s.year = 2021 AND f.year = 2021
GROUP BY i.control
ORDER BY avg_default_rate;
```

## Troubleshooting

### Common Issues

**1. Foreign Key Violations**
```
Error: violates foreign key constraint
```
**Solution**: Ensure IPEDS data is loaded first (creates Institutions table)

**2. Integer Out of Range**
```
Error: integer out of range
```
**Solution**: Database schema uses TEXT for institution_id (OPEID codes can be large)

**3. Check Constraint Violations**
```
Error: new row violates check constraint
```
**Solution**: Review data cleaning - invalid values should be converted to NULL

### Data Quality Monitoring
```sql
-- Check for missing data patterns
SELECT year,
       COUNT(*) as total_records,
       SUM(CASE WHEN adm_rate IS NULL THEN 1 ELSE 0 END) as missing_adm_rate,
       SUM(CASE WHEN act IS NULL THEN 1 ELSE 0 END) as missing_act
FROM Students
GROUP BY year
ORDER BY year;
```

## Future Enhancements

1. **Additional Tables**:
   - Program-level data (majors, degrees by field)
   - Earnings outcomes by major
   - Racial/ethnic diversity metrics
   - Graduate program information

2. **Derived Metrics**:
   - Return on investment calculations
   - Peer group comparisons
   - Trend indicators (improving/declining)

3. **Data Visualization**:
   - Dashboard integration (Tableau, PowerBI)
   - Geographic mapping of institutions
   - Time-series charts

## Data Sources

- **IPEDS**: U.S. Department of Education, National Center for Education Statistics
- **College Scorecard**: U.S. Department of Education

## License & Usage

This database is designed for educational and research purposes. Source data is publicly available from federal government databases. Please cite original data sources in any derivative works.

## Support & Contact

For questions about the database structure or queries, refer to [`part1.ipynb`](part1.ipynb ) for detailed schema documentation and design rationale.

---

**Last Updated**: November 2025  
**Database Version**: 1.0  
**Record Count**: ~6,000+ institutions, 4 years of temporal data (2018-2021)
