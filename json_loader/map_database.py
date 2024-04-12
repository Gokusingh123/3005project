# Dyal Sehra
# 101042059
# In order for these table to be created the database must be created 
# first.
# map_database.py is the file to create all the tables in the database. 
# To populate these tables please run file load_database.py 


import psycopg

    # dbname = 'project_database'
    # user = 'postgres'
    # password = '1234'
    # host = 'localhost' 
    # port = "5432"



# La Liga seasons of 2020/2021, 2019/2020, and 2018/2019.
# Premier League season of 2003/2004.


# Connect to your PostgreSQL database
# try: 
conn = psycopg.connect(
    dbname="project_database",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
# except: 
#     print("Unable to connect to the database")

# Create a cursor object
cur = conn.cursor()

# SQL statement to create the Competition table
cur.execute( """
CREATE TABLE IF NOT EXISTS Competition (
    competition_id INTEGER,
    season_id INTEGER,
    competition_name VARCHAR(255),
    competition_gender VARCHAR(50),
    country_name VARCHAR(255),
    season_name VARCHAR(255),
    match_updated TIMESTAMP,
    match_available TIMESTAMP,
    PRIMARY KEY (competition_id, season_id)
)
""")

# MATCHES DATASET


# SQL statement to create the Stadium table
cur.execute("""
CREATE TABLE IF NOT EXISTS Stadium (
    stadium_id SERIAL PRIMARY KEY,
    stadium_name VARCHAR(255)
)
""")

# SQL statement to create the Referee table
cur.execute("""
CREATE TABLE IF NOT EXISTS Referee (
    referee_id SERIAL PRIMARY KEY,
    referee_name VARCHAR(255)
)
""")

# SQL statement to create the Country table
cur.execute("""
CREATE TABLE IF NOT EXISTS Country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(255)
)
""")

# SQL statement to create the Manager table
cur.execute("""
CREATE TABLE IF NOT EXISTS Manager (
    manager_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    nickname VARCHAR(255),
    dob DATE,
    country_name VARCHAR(255)
)
""")

# SQL statement to create the Team table
cur.execute("""
CREATE TABLE IF NOT EXISTS Team (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(255)
)
""")

# SQL statement to create the Competition Stage table
cur.execute("""
CREATE TABLE IF NOT EXISTS CompetitionStage (
    competition_stage_id SERIAL PRIMARY KEY,
    competition_stage_name VARCHAR(255)
)
""")

# SQL statement to create the Matches table
cur.execute("""
CREATE TABLE IF NOT EXISTS Matches (
    match_id SERIAL PRIMARY KEY,
    competition_id INTEGER,
    season_id INTEGER,
    country_name VARCHAR(255),
    match_date DATE,
    kick_off TIME,
    stadium_id INTEGER,
    stadium_country VARCHAR(255),
    referee_id INTEGER,
    referee_country_id INTEGER,
    home_team_id INTEGER,
    home_team_gender VARCHAR(50),
    home_team_manager_id INTEGER,
    home_team_group VARCHAR(50),
    home_team_country_id INTEGER,
    away_team_id INTEGER,
    away_team_gender VARCHAR(50),
    away_team_manager_id INTEGER,
    away_team_group VARCHAR(50),
    away_team_country_id INTEGER,
    home_score INTEGER,
    away_score INTEGER,
    match_status VARCHAR(50),
    match_week INTEGER,
    competition_stage_id INTEGER,
    last_updated TIMESTAMP,
    data_version VARCHAR(50),
    FOREIGN KEY (competition_id, season_id) REFERENCES Competition(competition_id, season_id),
    FOREIGN KEY (stadium_id) REFERENCES Stadium(stadium_id),
    FOREIGN KEY (referee_id) REFERENCES Referee(referee_id),
    FOREIGN KEY (referee_country_id) REFERENCES Country(country_id),
    FOREIGN KEY (home_team_id) REFERENCES Team(team_id),
    FOREIGN KEY (home_team_manager_id) REFERENCES Manager(manager_id),
    FOREIGN KEY (home_team_country_id) REFERENCES Country(country_id),
    FOREIGN KEY (away_team_id) REFERENCES Team(team_id),
    FOREIGN KEY (away_team_manager_id) REFERENCES Manager(manager_id),
    FOREIGN KEY (away_team_country_id) REFERENCES Country(country_id),
    FOREIGN KEY (competition_stage_id) REFERENCES CompetitionStage(competition_stage_id)
)
""")


# SQL statement to create the Player table
cur.execute("""
CREATE TABLE IF NOT EXISTS Player (
    player_id SERIAL PRIMARY KEY,
    player_name VARCHAR(255),
    player_nickname VARCHAR(255),
    country_id INTEGER REFERENCES Country(country_id),
    jersey_number INTEGER
)
""")

# SQL statement to create the Match Lineup table
cur.execute("""
CREATE TABLE IF NOT EXISTS Match_Lineup (
    lineup_id SERIAL PRIMARY KEY,
    match_id INTEGER REFERENCES Matches(match_id),
    team_id INTEGER REFERENCES Team(team_id),
    player_id INTEGER REFERENCES Player(player_id)
)
""")


# SQL statement to create the Event Tyepe table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Event_type (
    event_type_id SERIAL PRIMARY KEY,
    event_name VARCHAR(255)
)
""")



# SQL statement to create the Position table
cur.execute("""
CREATE TABLE IF NOT EXISTS Position (
    position_id INTEGER PRIMARY KEY,
    position_name VARCHAR(50)
)
""")

# SQL statement to create the Lineup table
cur.execute("""
CREATE TABLE IF NOT EXISTS Lineup (
    lineup_id SERIAL PRIMARY KEY,
    event_id INTEGER REFERENCES Event_type(event_type_id),
    player_id INTEGER REFERENCES Player(player_id),
    position_id INTEGER REFERENCES Position(position_id),
    jersey_number INTEGER 
)
""")


# SQL statement to create the Events table
cur.execute("""
CREATE TABLE IF NOT EXISTS Events (
    event_id SERIAL PRIMARY KEY,
    event_uuid VARCHAR(50),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    period INTEGER,
    timestamp TIMESTAMP,
    minute INTEGER,
    second INTEGER,
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    possession INTEGER,
    possession_team_id INTEGER REFERENCES Team(team_id),
    play_pattern_name VARCHAR(50),
    team_id INTEGER REFERENCES Team(team_id),
    player_id INTEGER REFERENCES Player(player_id),
    position_id INTEGER REFERENCES Position(position_id),
    location_x INTEGER,
    location_y INTEGER,
    duration DECIMAL,
    under_pressure BOOLEAN,
    off_camera BOOLEAN,
    out BOOLEAN,
    tactics_formation VARCHAR(50),
    tactics_lineup INTEGER REFERENCES Lineup(lineup_id)
)
""")

# SQL statement to create the Related_Events table
cur.execute("""
CREATE TABLE IF NOT EXISTS Related_Events (
    event_uuid VARCHAR(50),
    event_id INTEGER REFERENCES Events(event_id),
    related_event_uuid VARCHAR(255)
)
""")



# SQL statement to create the 50/50 table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Outcome_50_50 (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    possession_team_id INTEGER REFERENCES Team(team_id),
    outcome VARCHAR(255),
    counterpress BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Bad behaviour table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Bad_Behaviour (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    card VARCHAR(255),
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Ball receipt table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Ball_Receipt (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    outcome VARCHAR(255),
    PRIMARY KEY (event_type_id, match_id, event_name,index)
)
""")

# SQL statement to create the Ball Recovery table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Ball_Recovery (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    offensive BOOLEAN,
    recovery_failure BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Block table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Block (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    deflection BOOLEAN,
    offensive BOOLEAN,
    save_block BOOLEAN,
    counterpress BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Carry table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Carry (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    team_id INTEGER REFERENCES Team(team_id),
    end_location_x INTEGER,
    end_location_y INTEGER,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Clearance table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Clearance (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    aerial_won BOOLEAN,
    body_part VARCHAR(255),
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Dribble table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Dribble (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    overrun BOOLEAN,
    nutmeg BOOLEAN,
    outcome VARCHAR(255),
    no_touch BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Dribble Past table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Dribbled_Past (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    counterpress BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Duel table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Duel (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    duel_type VARCHAR(255),
    duel_outcome VARCHAR(255),
    counterpress BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Foul Commited table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Foul_Committed (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    foul_type VARCHAR(255),
    counterpass BOOLEAN,
    offensive BOOLEAN,
    advantage BOOLEAN,
    penalty BOOLEAN,
    card VARCHAR(255),
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Foul Won table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Foul_Won (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    defensive BOOLEAN,
    advantage BOOLEAN,
    penalty BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Goalkeeper table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Goalkeeper (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    position_name VARCHAR(255),
    technique_name VARCHAR(255),
    body_part_name VARCHAR(255),
    type_name VARCHAR(255),
    outcome_name VARCHAR(255),
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Half End  table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Half_End (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    early_video_end BOOLEAN,
    match_suspended BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Half Start table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Half_Start (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    late_video_start BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Injury stoppage table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Injury_Stoppage (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    in_chain BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Interception table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Interception (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    outcome VARCHAR(255),
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Miscontrol table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Miscontrol (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    aerial_won BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Pass table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Pass (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    recipient_id INTEGER REFERENCES Player(player_id),
    length DECIMAL,
    angle DECIMAL,
    height_name VARCHAR(255),
    end_location_x INTEGER,
    end_location_y INTEGER,
    assisted_shot_id VARCHAR(255),
    backheel BOOLEAN,
    deflected BOOLEAN,
    miscommunication BOOLEAN,
    if_cross BOOLEAN,
    cut_back BOOLEAN,
    switch BOOLEAN,
    shot_assist BOOLEAN,
    goal_assist BOOLEAN,
    body_part_name VARCHAR(255),
    type_name VARCHAR(255),
    outcome_name VARCHAR(255),
    technique_name VARCHAR(255),
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Player Off table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Player_Off (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    permanent BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Pressure table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Pressure (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    counterpress BOOLEAN,
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Shot table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Shot (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    key_pass_id VARCHAR(255),
    end_location_x INTEGER,
    end_location_y INTEGER,
    end_location_z INTEGER,
    aerial_won BOOLEAN,
    follows_dribble BOOLEAN,
    first_time BOOLEAN,
    open_goal BOOLEAN,
    statsbomb_xg NUMERIC,
    deflected BOOLEAN,
    technique_name VARCHAR(255),
    body_part_name VARCHAR(255),
    type_name VARCHAR(255),
    outcome_name VARCHAR(255),
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")

# SQL statement to create the Substitution table
cur.execute ("""
CREATE TABLE IF NOT EXISTS Substitution (
    event_type_id INTEGER REFERENCES Event_type(event_type_id),
    match_id INTEGER REFERENCES Matches(match_id),
    index INTEGER,
    event_name VARCHAR(255),
    replacement_id INTEGER REFERENCES Player(player_id),
    outcome VARCHAR(255),
    PRIMARY KEY (event_type_id, match_id, event_name, index)
)
""")


# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

