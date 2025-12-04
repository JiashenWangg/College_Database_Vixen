import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import credentials
import json

# Page configuration
st.set_page_config(
    page_title="College Scorecard Dashboard",
    page_icon="🎓",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_engine():
    password = credentials.DB_PASSWORD
    db_name = credentials.DB_NAME
    andrewid = credentials.DB_USER
    server_name = credentials.DB_HOST
    db_url = f"postgresql://{andrewid}:{password}@{server_name}:5432/{db_name}"
    return create_engine(db_url)

engine = get_engine()

# Helper function to execute queries
@st.cache_data(ttl=600)
def run_query(query):
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

# Database schema information for AI queries
DATABASE_SCHEMA = """
Database Schema:

TABLE: Institutions
- institution_id (INT, PRIMARY KEY): 6-digit institution code
- name (TEXT): Institution name
- accredagency (TEXT): Accrediting agency
- control (INT): 1=Public, 2=Private nonprofit, 3=For-Profit
- CCbasic (INT): Carnegie Classification (15-17=Doctoral, 18-20=Master's, 21-23=Baccalaureate, 1-3=Associate's)
- region (INT): Region code (0-9)
- csba (TEXT): Core Based Statistical Area
- cba (TEXT): Combined Statistical Area
- county_fips (TEXT): County FIPS code
- city (TEXT): City name
- state (TEXT): Two-letter state code
- address (TEXT): Street address
- zip (TEXT): ZIP code
- latitude (FLOAT): Latitude
- longitude (FLOAT): Longitude

TABLE: Students
- institution_id (INT, FOREIGN KEY)
- year (INT): Academic year
- adm_rate (FLOAT): Admission rate (0-1)
- num_students (INT): Number of students
- act (FLOAT): Average ACT score (1-36)
- cdr2 (FLOAT): 2-year cohort default rate (0-1)
- cdr3 (FLOAT): 3-year cohort default rate (0-1)
PRIMARY KEY: (institution_id, year)

TABLE: Financials
- institution_id (INT, FOREIGN KEY)
- year (INT): Academic year
- tuitionfee_in (FLOAT): In-state tuition and fees
- tuitionfee_out (FLOAT): Out-of-state tuition and fees
- tuitionfee_prog (FLOAT): Program-specific tuition
- tuitfte (FLOAT): Tuition revenue per FTE
- avgfacsal (FLOAT): Average faculty salary
PRIMARY KEY: (institution_id, year)

TABLE: Academics
- institution_id (INT, FOREIGN KEY)
- year (INT): Academic year
- preddeg (INT): Predominant degree (0-4)
- highdeg (INT): Highest degree (0-4, where 4=Graduate)
- stufacr (FLOAT): Student-faculty ratio
PRIMARY KEY: (institution_id, year)

Common queries should JOIN these tables on institution_id and year.
Use institution name from Institutions table, not institution_id in results.
"""

def generate_sql_from_nl(user_query, selected_year):
    """Convert natural language to SQL using GitHub Copilot's LLM"""
    
    prompt = f"""You are a SQL expert. Convert the following natural language query into a PostgreSQL query.

{DATABASE_SCHEMA}

User's Question: {user_query}

Selected Year: {selected_year}

Instructions:
1. Generate a valid PostgreSQL SELECT query
2. Always include the institution name (i.name) in SELECT clause
3. Use the year {selected_year} unless user specifies otherwise
4. JOIN tables appropriately
5. Use proper column names from the schema
6. Include appropriate ORDER BY and LIMIT clauses
7. Format numbers nicely (ROUND for decimals)
8. Return ONLY the SQL query, no explanations
9. For percentages stored as 0-1, multiply by 100 in output
10. Common terms: "median" means PERCENTILE_CONT(0.5), "average" means AVG
11. For "highest/lowest/top/bottom", use ORDER BY with LIMIT

SQL Query:"""

    # This is a placeholder - you'll need to implement actual LLM call
    # For now, return a template that works with common queries
    return None

def execute_nl_query(user_query, selected_year):
    """Execute natural language query and return results"""
    
    # Common query patterns - map user intent to SQL
    query_lower = user_query.lower()
    
    # Pattern: highest/lowest tuition
    if any(word in query_lower for word in ['highest', 'most expensive', 'top']) and 'tuition' in query_lower:
        limit = 10
        if 'top' in query_lower:
            # Extract number if present
            import re
            match = re.search(r'top (\d+)', query_lower)
            if match:
                limit = int(match.group(1))
        
        sql = f"""
        SELECT 
            i.name,
            i.state,
            i.city,
            CASE 
                WHEN i.control = 1 THEN 'Public'
                WHEN i.control = 2 THEN 'Private Nonprofit'
                WHEN i.control = 3 THEN 'For-Profit'
            END as institution_type,
            ROUND(f.tuitionfee_out::numeric, 2) as out_state_tuition,
            ROUND(f.tuitionfee_in::numeric, 2) as in_state_tuition
        FROM Institutions i
        INNER JOIN Financials f ON i.institution_id = f.institution_id
        WHERE f.year = {selected_year}
            AND f.tuitionfee_out IS NOT NULL
        ORDER BY f.tuitionfee_out DESC
        LIMIT {limit}
        """
        return sql, "Highest Tuition Institutions"
    
    # Pattern: lowest/cheapest tuition
    elif any(word in query_lower for word in ['lowest', 'cheapest', 'least expensive', 'bottom']) and 'tuition' in query_lower:
        limit = 10
        import re
        match = re.search(r'(top|bottom) (\d+)', query_lower)
        if match:
            limit = int(match.group(2))
        
        sql = f"""
        SELECT 
            i.name,
            i.state,
            i.city,
            CASE 
                WHEN i.control = 1 THEN 'Public'
                WHEN i.control = 2 THEN 'Private Nonprofit'
                WHEN i.control = 3 THEN 'For-Profit'
            END as institution_type,
            ROUND(f.tuitionfee_out::numeric, 2) as out_state_tuition,
            ROUND(f.tuitionfee_in::numeric, 2) as in_state_tuition
        FROM Institutions i
        INNER JOIN Financials f ON i.institution_id = f.institution_id
        WHERE f.year = {selected_year}
            AND f.tuitionfee_out IS NOT NULL
            AND f.tuitionfee_out > 0
        ORDER BY f.tuitionfee_out ASC
        LIMIT {limit}
        """
        return sql, "Lowest Tuition Institutions"
    
    # Pattern: best/worst loan repayment
    elif any(word in query_lower for word in ['best', 'lowest']) and any(word in query_lower for word in ['default', 'repayment', 'loan']):
        limit = 10
        import re
        match = re.search(r'(top|best) (\d+)', query_lower)
        if match:
            limit = int(match.group(2))
        
        sql = f"""
        SELECT 
            i.name,
            i.state,
            i.city,
            CASE 
                WHEN i.control = 1 THEN 'Public'
                WHEN i.control = 2 THEN 'Private Nonprofit'
                WHEN i.control = 3 THEN 'For-Profit'
            END as institution_type,
            ROUND((s.cdr3 * 100)::numeric, 2) as default_rate_pct,
            s.num_students
        FROM Institutions i
        INNER JOIN Students s ON i.institution_id = s.institution_id
        WHERE s.year = {selected_year}
            AND s.cdr3 IS NOT NULL
        ORDER BY s.cdr3 ASC
        LIMIT {limit}
        """
        return sql, "Best Loan Repayment Performance"
    
    # Pattern: worst loan repayment
    elif any(word in query_lower for word in ['worst', 'highest']) and any(word in query_lower for word in ['default', 'repayment', 'loan']):
        limit = 10
        import re
        match = re.search(r'(top|worst) (\d+)', query_lower)
        if match:
            limit = int(match.group(2))
        
        sql = f"""
        SELECT 
            i.name,
            i.state,
            i.city,
            CASE 
                WHEN i.control = 1 THEN 'Public'
                WHEN i.control = 2 THEN 'Private Nonprofit'
                WHEN i.control = 3 THEN 'For-Profit'
            END as institution_type,
            ROUND((s.cdr3 * 100)::numeric, 2) as default_rate_pct,
            s.num_students
        FROM Institutions i
        INNER JOIN Students s ON i.institution_id = s.institution_id
        WHERE s.year = {selected_year}
            AND s.cdr3 IS NOT NULL
        ORDER BY s.cdr3 DESC
        LIMIT {limit}
        """
        return sql, "Worst Loan Repayment Performance"
    
    # Pattern: largest/biggest enrollment
    elif any(word in query_lower for word in ['largest', 'biggest', 'most students']):
        limit = 10
        import re
        match = re.search(r'(top|largest) (\d+)', query_lower)
        if match:
            limit = int(match.group(2))
        
        sql = f"""
        SELECT 
            i.name,
            i.state,
            i.city,
            CASE 
                WHEN i.control = 1 THEN 'Public'
                WHEN i.control = 2 THEN 'Private Nonprofit'
                WHEN i.control = 3 THEN 'For-Profit'
            END as institution_type,
            s.num_students,
            ROUND((s.adm_rate * 100)::numeric, 1) as admission_rate_pct
        FROM Institutions i
        INNER JOIN Students s ON i.institution_id = s.institution_id
        WHERE s.year = {selected_year}
            AND s.num_students IS NOT NULL
        ORDER BY s.num_students DESC
        LIMIT {limit}
        """
        return sql, "Largest Institutions by Enrollment"
    
    # Pattern: highest admission rate (easiest to get into)
    elif any(word in query_lower for word in ['easiest', 'highest admission', 'easy to get']):
        limit = 10
        import re
        match = re.search(r'(top|easiest) (\d+)', query_lower)
        if match:
            limit = int(match.group(2))
        
        sql = f"""
        SELECT 
            i.name,
            i.state,
            i.city,
            CASE 
                WHEN i.control = 1 THEN 'Public'
                WHEN i.control = 2 THEN 'Private Nonprofit'
                WHEN i.control = 3 THEN 'For-Profit'
            END as institution_type,
            ROUND((s.adm_rate * 100)::numeric, 1) as admission_rate_pct,
            s.num_students
        FROM Institutions i
        INNER JOIN Students s ON i.institution_id = s.institution_id
        WHERE s.year = {selected_year}
            AND s.adm_rate IS NOT NULL
            AND s.num_students >= 100
        ORDER BY s.adm_rate DESC
        LIMIT {limit}
        """
        return sql, "Easiest Admission"
    
    # Pattern: most selective (lowest admission rate)
    elif any(word in query_lower for word in ['selective', 'hardest', 'lowest admission', 'hard to get']):
        limit = 10
        import re
        match = re.search(r'(top|most) (\d+)', query_lower)
        if match:
            limit = int(match.group(2))
        
        sql = f"""
        SELECT 
            i.name,
            i.state,
            i.city,
            CASE 
                WHEN i.control = 1 THEN 'Public'
                WHEN i.control = 2 THEN 'Private Nonprofit'
                WHEN i.control = 3 THEN 'For-Profit'
            END as institution_type,
            ROUND((s.adm_rate * 100)::numeric, 1) as admission_rate_pct,
            s.num_students
        FROM Institutions i
        INNER JOIN Students s ON i.institution_id = s.institution_id
        WHERE s.year = {selected_year}
            AND s.adm_rate IS NOT NULL
            AND s.adm_rate > 0
            AND s.num_students >= 100
        ORDER BY s.adm_rate ASC
        LIMIT {limit}
        """
        return sql, "Most Selective Institutions"
    
    # Pattern: highest ACT scores
    elif 'act' in query_lower and any(word in query_lower for word in ['highest', 'top', 'best']):
        limit = 10
        import re
        match = re.search(r'(top|highest) (\d+)', query_lower)
        if match:
            limit = int(match.group(2))
        
        sql = f"""
        SELECT 
            i.name,
            i.state,
            i.city,
            CASE 
                WHEN i.control = 1 THEN 'Public'
                WHEN i.control = 2 THEN 'Private Nonprofit'
                WHEN i.control = 3 THEN 'For-Profit'
            END as institution_type,
            ROUND(s.act::numeric, 1) as avg_act_score,
            ROUND((s.adm_rate * 100)::numeric, 1) as admission_rate_pct
        FROM Institutions i
        INNER JOIN Students s ON i.institution_id = s.institution_id
        WHERE s.year = {selected_year}
            AND s.act IS NOT NULL
        ORDER BY s.act DESC
        LIMIT {limit}
        """
        return sql, "Highest ACT Scores"
    
    # Pattern: highest faculty salary
    elif any(word in query_lower for word in ['faculty', 'professor', 'teacher']) and any(word in query_lower for word in ['salary', 'pay', 'highest']):
        limit = 10
        import re
        match = re.search(r'(top|highest) (\d+)', query_lower)
        if match:
            limit = int(match.group(2))
        
        sql = f"""
        SELECT 
            i.name,
            i.state,
            i.city,
            CASE 
                WHEN i.control = 1 THEN 'Public'
                WHEN i.control = 2 THEN 'Private Nonprofit'
                WHEN i.control = 3 THEN 'For-Profit'
            END as institution_type,
            ROUND(f.avgfacsal::numeric, 2) as avg_faculty_salary,
            ROUND(f.tuitionfee_out::numeric, 2) as out_state_tuition
        FROM Institutions i
        INNER JOIN Financials f ON i.institution_id = f.institution_id
        WHERE f.year = {selected_year}
            AND f.avgfacsal IS NOT NULL
        ORDER BY f.avgfacsal DESC
        LIMIT {limit}
        """
        return sql, "Highest Faculty Salaries"
    
    # Pattern: institutions in a specific state
    elif 'in' in query_lower or 'from' in query_lower:
        # Try to extract state abbreviation
        import re
        # Look for state names or abbreviations
        words = query_lower.split()
        state = None
        for i, word in enumerate(words):
            if word in ['in', 'from'] and i + 1 < len(words):
                state = words[i + 1].upper()
                if len(state) > 2:
                    # Common state name abbreviations
                    state_map = {
                        'california': 'CA', 'texas': 'TX', 'new york': 'NY', 'florida': 'FL',
                        'pennsylvania': 'PA', 'illinois': 'IL', 'ohio': 'OH', 'michigan': 'MI',
                        'georgia': 'GA', 'north carolina': 'NC', 'virginia': 'VA', 'massachusetts': 'MA'
                    }
                    state = state_map.get(state.lower(), state[:2].upper())
                break
        
        if state and len(state) == 2:
            sql = f"""
            SELECT 
                i.name,
                i.city,
                CASE 
                    WHEN i.control = 1 THEN 'Public'
                    WHEN i.control = 2 THEN 'Private Nonprofit'
                    WHEN i.control = 3 THEN 'For-Profit'
                END as institution_type,
                s.num_students,
                ROUND(f.tuitionfee_out::numeric, 2) as out_state_tuition,
                ROUND((s.adm_rate * 100)::numeric, 1) as admission_rate_pct
            FROM Institutions i
            INNER JOIN Students s ON i.institution_id = s.institution_id
            INNER JOIN Financials f ON i.institution_id = f.institution_id AND s.year = f.year
            WHERE s.year = {selected_year}
                AND i.state = '{state}'
                AND s.num_students IS NOT NULL
            ORDER BY s.num_students DESC
            LIMIT 20
            """
            return sql, f"Institutions in {state}"
    
    # Pattern: median/average calculations
    elif 'median' in query_lower or 'average' in query_lower:
        if 'tuition' in query_lower:
            sql = f"""
            SELECT 
                CASE 
                    WHEN i.control = 1 THEN 'Public'
                    WHEN i.control = 2 THEN 'Private Nonprofit'
                    WHEN i.control = 3 THEN 'For-Profit'
                END as institution_type,
                COUNT(*) as num_institutions,
                ROUND(AVG(f.tuitionfee_out)::numeric, 2) as avg_tuition,
                ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.tuitionfee_out)::numeric, 2) as median_tuition,
                ROUND(MIN(f.tuitionfee_out)::numeric, 2) as min_tuition,
                ROUND(MAX(f.tuitionfee_out)::numeric, 2) as max_tuition
            FROM Institutions i
            INNER JOIN Financials f ON i.institution_id = f.institution_id
            WHERE f.year = {selected_year}
                AND f.tuitionfee_out IS NOT NULL
                AND i.control IN (1, 2, 3)
            GROUP BY i.control
            ORDER BY avg_tuition DESC
            """
            return sql, "Tuition Statistics by Institution Type"
    
    # Default: return None if no pattern matched
    return None, None

# Title and introduction
st.title("🎓 College Scorecard Dashboard")
st.markdown("""
This dashboard provides comprehensive insights into U.S. colleges and universities, 
including enrollment, tuition rates, and loan repayment performance.
""")

# Sidebar for year selection
st.sidebar.header("Select Year")
available_years = run_query("SELECT DISTINCT year FROM Students ORDER BY year DESC")
selected_year = st.sidebar.selectbox(
    "Year of Interest:",
    available_years['year'].tolist()
)

# Calculate previous year for comparison
previous_year = selected_year - 1

st.sidebar.markdown(f"**Comparing:** {selected_year} vs {previous_year}")

# AI Query Interface
st.sidebar.markdown("---")
st.sidebar.header("🤖 Ask Questions")
st.sidebar.markdown("Ask questions in plain English!")

# Initialize chat history
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# AI Chat Interface in Sidebar
with st.sidebar.expander("💬 AI Assistant", expanded=False):
    st.markdown("""
    **Example questions:**
    - Find the top 10 most expensive colleges
    - Show me the cheapest tuition institutions
    - Which colleges have the best loan repayment?
    - What are the largest universities by enrollment?
    - Show me colleges in California
    - Which schools are most selective?
    - What's the median tuition by institution type?
    """)
    
    user_question = st.text_input(
        "Ask a question:",
        placeholder="e.g., Find colleges with highest ACT scores",
        key="user_question"
    )
    
    if st.button("Ask", key="ask_button") and user_question:
        with st.spinner("Thinking..."):
            sql_query, title = execute_nl_query(user_question, selected_year)
            
            if sql_query:
                try:
                    result_df = run_query(sql_query)
                    st.session_state.chat_history.append({
                        'question': user_question,
                        'title': title,
                        'result': result_df,
                        'sql': sql_query
                    })
                    st.success(f"Found {len(result_df)} results!")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
            else:
                st.warning("I couldn't understand that question. Try rephrasing or use one of the example questions above.")

# Display chat results in main area if any
if st.session_state.chat_history:
    st.header("🤖 AI Query Results")
    
    # Show most recent query result
    latest = st.session_state.chat_history[-1]
    st.subheader(f"❓ {latest['question']}")
    st.markdown(f"**{latest['title']}**")
    
    if not latest['result'].empty:
        st.dataframe(latest['result'], use_container_width=True, hide_index=True)
        
        # Download button
        csv = latest['result'].to_csv(index=False)
        st.download_button(
            label="📥 Download Results as CSV",
            data=csv,
            file_name=f"query_results_{selected_year}.csv",
            mime="text/csv",
        )
        
        with st.expander("🔍 View SQL Query"):
            st.code(latest['sql'], language="sql")
    else:
        st.info("No results found for this query.")
    
    # Clear history button
    if st.button("Clear Query History"):
        st.session_state.chat_history = []
        st.rerun()
    
    st.markdown("---")

# Main dashboard sections
st.header(f"📊 College Scorecard Report - {selected_year}")

# Section 1: Summary Statistics
st.subheader("1. Institution Summary")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### Institutions by State")
    query_by_state = f"""
    SELECT 
        i.state,
        COUNT(DISTINCT i.institution_id) as institution_count
    FROM Institutions i
    INNER JOIN Students s ON i.institution_id = s.institution_id
    WHERE s.year = {selected_year}
    GROUP BY i.state
    ORDER BY institution_count DESC
    """
    df_by_state = run_query(query_by_state)
    
    # Create map
    fig_map = px.choropleth(
        df_by_state,
        locations='state',
        locationmode="USA-states",
        color='institution_count',
        scope="usa",
        color_continuous_scale="Blues",
        labels={'institution_count': 'Number of Institutions'},
        title=f"Institutions by State ({selected_year})"
    )
    st.plotly_chart(fig_map, use_container_width=True)

with col2:
    st.markdown("#### Institutions by Type")
    query_by_type = f"""
    SELECT 
        CASE 
            WHEN i.control = 1 THEN 'Public'
            WHEN i.control = 2 THEN 'Private Nonprofit'
            WHEN i.control = 3 THEN 'For-Profit'
            ELSE 'Unknown'
        END as institution_type,
        COUNT(DISTINCT i.institution_id) as count
    FROM Institutions i
    INNER JOIN Students s ON i.institution_id = s.institution_id
    WHERE s.year = {selected_year}
    GROUP BY i.control
    ORDER BY count DESC
    """
    df_by_type = run_query(query_by_type)
    
    fig_pie = px.pie(
        df_by_type,
        values='count',
        names='institution_type',
        title=f"Distribution by Institution Type ({selected_year})"
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# Display summary metrics
col1, col2, col3, col4 = st.columns(4)

total_institutions = run_query(f"""
    SELECT COUNT(DISTINCT institution_id) as count 
    FROM Students WHERE year = {selected_year}
""")

total_students = run_query(f"""
    SELECT SUM(num_students) as total 
    FROM Students WHERE year = {selected_year}
""")

avg_admission_rate = run_query(f"""
    SELECT AVG(adm_rate) * 100 as avg_rate 
    FROM Students WHERE year = {selected_year} AND adm_rate IS NOT NULL
""")

col1.metric("Total Institutions", f"{total_institutions['count'][0]:,}")
col2.metric("Total Students", f"{int(total_students['total'][0]):,}")
col3.metric("Avg Admission Rate", f"{avg_admission_rate['avg_rate'][0]:.1f}%")

# Section 2: Tuition Analysis
st.subheader("2. Tuition Rate Analysis")

tab1, tab2 = st.tabs(["By State", "By Carnegie Classification"])

with tab1:
    query_tuition_state = f"""
    SELECT 
        i.state,
        ROUND(AVG(f.tuitionfee_in)::numeric, 2) as avg_in_state,
        ROUND(AVG(f.tuitionfee_out)::numeric, 2) as avg_out_state,
        COUNT(DISTINCT i.institution_id) as num_institutions
    FROM Institutions i
    INNER JOIN Financials f ON i.institution_id = f.institution_id
    WHERE f.year = {selected_year}
    GROUP BY i.state
    HAVING COUNT(DISTINCT i.institution_id) >= 3
    ORDER BY avg_out_state DESC
    LIMIT 20
    """
    df_tuition_state = run_query(query_tuition_state)
    
    fig_tuition = go.Figure()
    fig_tuition.add_trace(go.Bar(
        name='In-State Tuition',
        x=df_tuition_state['state'],
        y=df_tuition_state['avg_in_state']
    ))
    fig_tuition.add_trace(go.Bar(
        name='Out-of-State Tuition',
        x=df_tuition_state['state'],
        y=df_tuition_state['avg_out_state']
    ))
    
    fig_tuition.update_layout(
        title=f"Average Tuition by State - Top 20 ({selected_year})",
        xaxis_title="State",
        yaxis_title="Average Tuition ($)",
        barmode='group',
        height=500
    )
    st.plotly_chart(fig_tuition, use_container_width=True)

with tab2:
    query_tuition_carnegie = f"""
    SELECT 
        CASE 
            WHEN i.CCbasic BETWEEN 15 AND 17 THEN 'Doctoral Universities'
            WHEN i.CCbasic BETWEEN 18 AND 20 THEN 'Master''s Colleges'
            WHEN i.CCbasic BETWEEN 21 AND 23 THEN 'Baccalaureate Colleges'
            WHEN i.CCbasic BETWEEN 1 AND 3 THEN 'Associate''s Colleges'
            ELSE 'Other/Specialized'
        END as carnegie_category,
        ROUND(AVG(f.tuitionfee_in)::numeric, 2) as avg_in_state,
        ROUND(AVG(f.tuitionfee_out)::numeric, 2) as avg_out_state,
        COUNT(DISTINCT i.institution_id) as num_institutions
    FROM Institutions i
    INNER JOIN Financials f ON i.institution_id = f.institution_id
    WHERE f.year = {selected_year} AND i.CCbasic IS NOT NULL
    GROUP BY carnegie_category
    ORDER BY avg_out_state DESC
    """
    df_tuition_carnegie = run_query(query_tuition_carnegie)
    
    fig_carnegie = px.bar(
        df_tuition_carnegie,
        x='carnegie_category',
        y=['avg_in_state', 'avg_out_state'],
        title=f"Average Tuition by Carnegie Classification ({selected_year})",
        labels={'value': 'Average Tuition ($)', 'variable': 'Type'},
        barmode='group',
        height=500
    )
    st.plotly_chart(fig_carnegie, use_container_width=True)

# Section 3: Loan Repayment Performance
st.subheader("3. Loan Repayment Performance")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 🏆 Best Performing Institutions (3-Year CDR)")
    query_best = f"""
    SELECT 
        i.name,
        i.state,
        i.city,
        CASE 
            WHEN i.control = 1 THEN 'Public'
            WHEN i.control = 2 THEN 'Private Nonprofit'
            WHEN i.control = 3 THEN 'For-Profit'
        END as type,
        ROUND((s.cdr3 * 100)::numeric, 2) as cohort_default_rate_pct,
        s.num_students
    FROM Institutions i
    INNER JOIN Students s ON i.institution_id = s.institution_id
    WHERE s.year = {selected_year} 
        AND s.cdr3 IS NOT NULL
        AND s.num_students >= 100
    ORDER BY s.cdr3 ASC
    LIMIT 15
    """
    df_best = run_query(query_best)
    st.dataframe(df_best, use_container_width=True, hide_index=True)

with col2:
    st.markdown("#### ⚠️ Worst Performing Institutions (3-Year CDR)")
    query_worst = f"""
    SELECT 
        i.name,
        i.state,
        i.city,
        CASE 
            WHEN i.control = 1 THEN 'Public'
            WHEN i.control = 2 THEN 'Private Nonprofit'
            WHEN i.control = 3 THEN 'For-Profit'
        END as type,
        ROUND((s.cdr3 * 100)::numeric, 2) as cohort_default_rate_pct,
        s.num_students
    FROM Institutions i
    INNER JOIN Students s ON i.institution_id = s.institution_id
    WHERE s.year = {selected_year} 
        AND s.cdr3 IS NOT NULL
        AND s.num_students >= 100
    ORDER BY s.cdr3 DESC
    LIMIT 15
    """
    df_worst = run_query(query_worst)
    st.dataframe(df_worst, use_container_width=True, hide_index=True)

# Section 4: Trends Over Time
st.subheader("4. Trends Over Time")

tab1, tab2 = st.tabs(["Tuition Trends", "Loan Repayment Trends"])

with tab1:
    query_tuition_trend = f"""
    SELECT 
        f.year,
        CASE 
            WHEN i.control = 1 THEN 'Public'
            WHEN i.control = 2 THEN 'Private Nonprofit'
            WHEN i.control = 3 THEN 'For-Profit'
        END as institution_type,
        ROUND(AVG(f.tuitionfee_in)::numeric, 2) as avg_in_state,
        ROUND(AVG(f.tuitionfee_out)::numeric, 2) as avg_out_state
    FROM Institutions i
    INNER JOIN Financials f ON i.institution_id = f.institution_id
    WHERE i.control IN (1, 2, 3)
    GROUP BY f.year, i.control
    ORDER BY f.year, i.control
    """
    df_tuition_trend = run_query(query_tuition_trend)
    
    fig_trend = px.line(
        df_tuition_trend,
        x='year',
        y='avg_out_state',
        color='institution_type',
        title="Average Out-of-State Tuition Trends by Institution Type",
        labels={'avg_out_state': 'Average Tuition ($)', 'year': 'Year'},
        markers=True
    )
    st.plotly_chart(fig_trend, use_container_width=True)

with tab2:
    query_cdr_trend = f"""
    SELECT 
        s.year,
        CASE 
            WHEN i.control = 1 THEN 'Public'
            WHEN i.control = 2 THEN 'Private Nonprofit'
            WHEN i.control = 3 THEN 'For-Profit'
        END as institution_type,
        ROUND((AVG(s.cdr3) * 100)::numeric, 2) as avg_cdr3_pct
    FROM Institutions i
    INNER JOIN Students s ON i.institution_id = s.institution_id
    WHERE i.control IN (1, 2, 3) AND s.cdr3 IS NOT NULL
    GROUP BY s.year, i.control
    ORDER BY s.year, i.control
    """
    df_cdr_trend = run_query(query_cdr_trend)
    
    fig_cdr = px.line(
        df_cdr_trend,
        x='year',
        y='avg_cdr3_pct',
        color='institution_type',
        title="Average 3-Year Cohort Default Rate Trends by Institution Type",
        labels={'avg_cdr3_pct': 'Default Rate (%)', 'year': 'Year'},
        markers=True
    )
    st.plotly_chart(fig_cdr, use_container_width=True)

# Section 5: Additional Analyses
st.header("📈 Additional Insights")

# Analysis 1: Faculty Salary vs Tuition Correlation
st.subheader("5. Faculty Salary vs Tuition Analysis")

query_salary_tuition = f"""
SELECT 
    i.name,
    i.state,
    CASE 
        WHEN i.control = 1 THEN 'Public'
        WHEN i.control = 2 THEN 'Private Nonprofit'
        WHEN i.control = 3 THEN 'For-Profit'
    END as institution_type,
    f.avgfacsal as avg_faculty_salary,
    f.tuitionfee_out as out_state_tuition,
    COALESCE(s.cdr3 * 100, 5) as default_rate_pct
FROM Institutions i
INNER JOIN Financials f ON i.institution_id = f.institution_id
INNER JOIN Students s ON i.institution_id = s.institution_id AND f.year = s.year
WHERE f.year = {selected_year}
    AND f.avgfacsal IS NOT NULL
    AND f.tuitionfee_out IS NOT NULL
    AND f.avgfacsal > 0
    AND f.tuitionfee_out > 0
"""
df_salary_tuition = run_query(query_salary_tuition)

# Fill any remaining NaN values with a default
df_salary_tuition['default_rate_pct'] = df_salary_tuition['default_rate_pct'].fillna(5)

fig_scatter = px.scatter(
    df_salary_tuition,
    x='avg_faculty_salary',
    y='out_state_tuition',
    color='institution_type',
    size='default_rate_pct',
    hover_data=['name', 'state', 'default_rate_pct'],
    title=f"Faculty Salary vs Tuition (bubble size = default rate) - {selected_year}",
    labels={
        'avg_faculty_salary': 'Average Faculty Salary ($)',
        'out_state_tuition': 'Out-of-State Tuition ($)',
        'default_rate_pct': 'Default Rate (%)'
    }
)
st.plotly_chart(fig_scatter, use_container_width=True)

# Analysis 2: Year-over-Year Changes
st.subheader("6. Year-over-Year Changes")

col1, col2 = st.columns(2)

with col1:
    query_yoy_tuition = f"""
    SELECT 
        i.state,
        ROUND(AVG(f_current.tuitionfee_out)::numeric, 2) as current_tuition,
        ROUND(AVG(f_previous.tuitionfee_out)::numeric, 2) as previous_tuition,
        ROUND(((AVG(f_current.tuitionfee_out) - AVG(f_previous.tuitionfee_out)) / 
               NULLIF(AVG(f_previous.tuitionfee_out), 0) * 100)::numeric, 2) as pct_change
    FROM Institutions i
    INNER JOIN Financials f_current ON i.institution_id = f_current.institution_id
    INNER JOIN Financials f_previous ON i.institution_id = f_previous.institution_id
    WHERE f_current.year = {selected_year}
        AND f_previous.year = {previous_year}
        AND f_current.tuitionfee_out IS NOT NULL
        AND f_previous.tuitionfee_out IS NOT NULL
    GROUP BY i.state
    HAVING COUNT(DISTINCT i.institution_id) >= 3
    ORDER BY pct_change DESC
    LIMIT 15
    """
    df_yoy_tuition = run_query(query_yoy_tuition)
    
    fig_yoy = px.bar(
        df_yoy_tuition,
        x='state',
        y='pct_change',
        title=f"Tuition Change: {previous_year} to {selected_year} (Top 15 States)",
        labels={'pct_change': 'Percent Change (%)', 'state': 'State'},
        color='pct_change',
        color_continuous_scale='RdYlGn_r'
    )
    st.plotly_chart(fig_yoy, use_container_width=True)

with col2:
    query_yoy_cdr = f"""
    SELECT 
        i.state,
        ROUND((AVG(s_current.cdr3) * 100)::numeric, 2) as current_cdr,
        ROUND((AVG(s_previous.cdr3) * 100)::numeric, 2) as previous_cdr,
        ROUND(((AVG(s_current.cdr3) - AVG(s_previous.cdr3)) * 100)::numeric, 2) as change
    FROM Institutions i
    INNER JOIN Students s_current ON i.institution_id = s_current.institution_id
    INNER JOIN Students s_previous ON i.institution_id = s_previous.institution_id
    WHERE s_current.year = {selected_year}
        AND s_previous.year = {previous_year}
        AND s_current.cdr3 IS NOT NULL
        AND s_previous.cdr3 IS NOT NULL
    GROUP BY i.state
    HAVING COUNT(DISTINCT i.institution_id) >= 3
    ORDER BY change
    LIMIT 15
    """
    df_yoy_cdr = run_query(query_yoy_cdr)
    
    fig_yoy_cdr = px.bar(
        df_yoy_cdr,
        x='state',
        y='change',
        title=f"Default Rate Change: {previous_year} to {selected_year} (Top 15 States)",
        labels={'change': 'Percentage Point Change', 'state': 'State'},
        color='change',
        color_continuous_scale='RdYlGn'
    )
    st.plotly_chart(fig_yoy_cdr, use_container_width=True)

# Analysis 3: Student-Faculty Ratio and Academic Degree Analysis
st.subheader("7. Student-Faculty Ratio and Academic Excellence")

query_academics = f"""
SELECT 
    i.state,
    CASE 
        WHEN a.highdeg = 0 THEN 'Non-degree'
        WHEN a.highdeg = 1 THEN 'Certificate'
        WHEN a.highdeg = 2 THEN 'Associate'
        WHEN a.highdeg = 3 THEN 'Bachelor''s'
        WHEN a.highdeg = 4 THEN 'Graduate'
        ELSE 'Unknown'
    END as highest_degree,
    ROUND(AVG(a.stufacr)::numeric, 1) as avg_student_faculty_ratio,
    COUNT(DISTINCT i.institution_id) as num_institutions,
    ROUND(AVG(f.tuitionfee_out)::numeric, 2) as avg_tuition
FROM Institutions i
INNER JOIN Academics a ON i.institution_id = a.institution_id
INNER JOIN Financials f ON i.institution_id = f.institution_id AND a.year = f.year
WHERE a.year = {selected_year}
    AND a.stufacr IS NOT NULL
    AND a.highdeg IS NOT NULL
GROUP BY i.state, a.highdeg
HAVING COUNT(DISTINCT i.institution_id) >= 2
"""
df_academics = run_query(query_academics)

fig_sf_ratio = px.scatter(
    df_academics,
    x='avg_student_faculty_ratio',
    y='avg_tuition',
    color='highest_degree',
    size='num_institutions',
    hover_data=['state'],
    title=f"Student-Faculty Ratio vs Tuition by Highest Degree Offered ({selected_year})",
    labels={
        'avg_student_faculty_ratio': 'Average Student-Faculty Ratio',
        'avg_tuition': 'Average Tuition ($)',
        'num_institutions': 'Number of Institutions'
    }
)
st.plotly_chart(fig_sf_ratio, use_container_width=True)

# Analysis 4: Enrollment Trends
st.subheader("8. Total Enrollment Trends")

query_enrollment = f"""
SELECT 
    s.year,
    CASE 
        WHEN i.control = 1 THEN 'Public'
        WHEN i.control = 2 THEN 'Private Nonprofit'
        WHEN i.control = 3 THEN 'For-Profit'
    END as institution_type,
    SUM(s.num_students) as total_enrollment
FROM Institutions i
INNER JOIN Students s ON i.institution_id = s.institution_id
WHERE i.control IN (1, 2, 3) AND s.num_students IS NOT NULL
GROUP BY s.year, i.control
ORDER BY s.year, i.control
"""
df_enrollment = run_query(query_enrollment)

fig_enrollment = px.line(
    df_enrollment,
    x='year',
    y='total_enrollment',
    color='institution_type',
    title="Total Enrollment Trends by Institution Type",
    labels={'total_enrollment': 'Total Enrollment', 'year': 'Year'},
    markers=True
)
fig_enrollment.update_layout(yaxis_tickformat=',')
st.plotly_chart(fig_enrollment, use_container_width=True)

# Analysis 5: Institutions with Missing Data
st.subheader("9. Data Reporting Changes")

query_missing = f"""
SELECT 
    i.name,
    i.state,
    CASE 
        WHEN i.control = 1 THEN 'Public'
        WHEN i.control = 2 THEN 'Private Nonprofit'
        WHEN i.control = 3 THEN 'For-Profit'
    END as institution_type,
    CASE 
        WHEN s_current.institution_id IS NULL THEN 'Stopped Reporting'
        WHEN s_previous.institution_id IS NULL THEN 'New This Year'
    END as status
FROM Institutions i
LEFT JOIN Students s_current ON i.institution_id = s_current.institution_id 
    AND s_current.year = {selected_year}
LEFT JOIN Students s_previous ON i.institution_id = s_previous.institution_id 
    AND s_previous.year = {previous_year}
WHERE (s_current.institution_id IS NULL AND s_previous.institution_id IS NOT NULL)
   OR (s_previous.institution_id IS NULL AND s_current.institution_id IS NOT NULL)
ORDER BY status, i.state, i.name
"""
df_missing = run_query(query_missing)

if not df_missing.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        stopped = df_missing[df_missing['status'] == 'Stopped Reporting']
        st.markdown(f"#### Institutions that Stopped Reporting ({len(stopped)})")
        if not stopped.empty:
            st.dataframe(stopped[['name', 'state', 'institution_type']], use_container_width=True, hide_index=True)
    
    with col2:
        new = df_missing[df_missing['status'] == 'New This Year']
        st.markdown(f"#### New Institutions This Year ({len(new)})")
        if not new.empty:
            st.dataframe(new[['name', 'state', 'institution_type']], use_container_width=True, hide_index=True)
else:
    st.info("No significant changes in institutional reporting between these years.")

# Footer
st.markdown("---")
st.markdown("""
**Data Notes:**
- CDR (Cohort Default Rate): The percentage of borrowers who default on loans within 2 or 3 years
- Lower CDR indicates better loan repayment performance
- Student-faculty ratio: Number of students per faculty member
- Data filtered to show institutions with sufficient enrollment (≥100 students) where applicable
""")
