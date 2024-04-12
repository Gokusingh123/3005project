# Dyal Sehra
# 101042059
# In order for the database to be populated the data folder
# must be in the same directory as this load_database.py file
# Additionally the database should already be created and 
# the tables should be added using map_database. 
# Note: running script a second time will cause dublicate 
# values error. 


import json
import os
from datetime import datetime

import psycopg


# Connect to your PostgreSQL database
conn = psycopg.connect(
    dbname="project_database",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)

# Create a cursor object
cur = conn.cursor()

# Function to populate the Competition table
def populate_competition_table(desired_seasons, desired_competitions):
    # Read competitions.json file
    with open('data/competitions.json', 'r') as file:
        competitions_data = json.load(file)
    
    # Iterate over each competition object
    for competition in competitions_data:
        # Extract relevant data
        season_name = competition['season_name']
        competition_name = competition['competition_name']
        
        # Check if the season is in the list of desired seasons
        if season_name in desired_seasons and competition_name in desired_competitions:
            # Extract other relevant data
            competition_id = competition['competition_id']
            season_id = competition['season_id']
            competition_gender = competition['competition_gender']
            country_name = competition['country_name']
            match_updated_str = competition['match_updated']
            match_available_str = competition['match_available']
            
            # Convert string timestamps to datetime objects
            match_updated = datetime.fromisoformat(match_updated_str)
            match_available = datetime.fromisoformat(match_available_str)
            
            # Insert data into Competition table
            cur.execute("""
                INSERT INTO Competition (competition_id, season_id, competition_name, competition_gender,
                                         country_name, season_name, match_updated, match_available)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (competition_id, season_id, competition_name, competition_gender,
                  country_name, season_name, match_updated, match_available))
    
    print("Competition table populated successfully.")

# Define the list of desired seasons
desired_seasons = ['2020/2021', '2019/2020', '2018/2019', '2003/2004']
desired_competitions = ['La Liga','Premier League']

# Call the function with the list of desired seasons

populate_competition_table(desired_seasons, desired_competitions)


# Commit the transaction
conn.commit()


# MATCHES DATASET TABLES: 


# Function to populate the Stadium table
def populate_stadium_table():
    matches_folder = "data/matches"
    existing_stadium_ids = set()  # Keep track of existing stadium_ids
    
    # Fetch existing stadium_ids from the database
    cur.execute("SELECT stadium_id FROM Stadium")
    rows = cur.fetchall()
    for row in rows:
        existing_stadium_ids.add(row[0])
    
    for competition_id in os.listdir(matches_folder):
        competition_path = os.path.join(matches_folder, competition_id)
        if os.path.isdir(competition_path):
            for season_id in os.listdir(competition_path):
                season_path = os.path.join(competition_path, season_id)
                if os.path.isfile(season_path):
                    with open(season_path, 'r') as file:
                        matches_data = json.load(file)
                        for match in matches_data:
                            if "stadium" in match: # Check if "stadium" key exists 
                                stadium_id = match["stadium"]["id"]
                                stadium_name = match["stadium"]["name"]
                                if stadium_id not in existing_stadium_ids:  # Check if stadium_id already exists
                                    cur.execute("INSERT INTO Stadium (stadium_id, stadium_name) VALUES (%s, %s)", (stadium_id, stadium_name))
                                    conn.commit()
                                    existing_stadium_ids.add(stadium_id)  # Add the new stadium_id to the set of existing stadium_ids
    print("Stadium table populated successfully.")

populate_stadium_table()


# Function to populate the Referee table
def populate_referee_table():
    matches_folder = "data/matches"
    existing_referee_ids = set()  # Keep track of existing referee_ids
    
    # Fetch existing referee_ids from the database
    cur.execute("SELECT referee_id FROM Referee")
    rows = cur.fetchall()
    for row in rows:
        existing_referee_ids.add(row[0])
    
    for competition_id in os.listdir(matches_folder):
        competition_path = os.path.join(matches_folder, competition_id)
        if os.path.isdir(competition_path):
            for season_id in os.listdir(competition_path):
                season_path = os.path.join(competition_path, season_id)
                if os.path.isfile(season_path):
                    with open(season_path, 'r') as file:
                        matches_data = json.load(file)
                        for match in matches_data:
                            if "referee" in match:  # Check if "referee" key exists
                                referee_id = match["referee"]["id"]
                                referee_name = match["referee"]["name"]
                                if referee_id not in existing_referee_ids:  # Check if referee_id already exists
                                    cur.execute("INSERT INTO Referee (referee_id, referee_name) VALUES (%s, %s)", (referee_id, referee_name))
                                    conn.commit()
                                    existing_referee_ids.add(referee_id)  # Add the new referee_id to the set of existing referee_ids
    print("Referee table populated successfully.")

populate_referee_table()

# Function to populate the Country table
def populate_country_table():
    matches_folder = "data/matches"
    lineups_folder = "data/lineups"
    existing_country_ids = set()  # Keep track of existing country_ids
    
    # Fetch existing country_ids from the database
    cur.execute("SELECT country_id FROM Country")
    rows = cur.fetchall()
    for row in rows:
        existing_country_ids.add(row[0])
    
    for competition_id in os.listdir(matches_folder):
        competition_path = os.path.join(matches_folder, competition_id)
        if os.path.isdir(competition_path):
            for season_id in os.listdir(competition_path):
                season_path = os.path.join(competition_path, season_id)
                if os.path.isfile(season_path):
                    with open(season_path, 'r') as file:
                        matches_data = json.load(file)
                        for match in matches_data:
                            if "stadium" in match and "country" in match["stadium"]:
                                play_country_id = match["stadium"]["country"]["id"]
                                play_country_name = match["stadium"]["country"]["name"]
                                
                                # Add play country
                                if play_country_id not in existing_country_ids:
                                    cur.execute("INSERT INTO Country (country_id, country_name) VALUES (%s, %s)", (play_country_id, play_country_name))
                                    conn.commit()
                                    existing_country_ids.add(play_country_id)
                            
                            if "referee" in match and "country" in match["referee"]:
                                referee_country_id = match["referee"]["country"]["id"]
                                referee_country_name = match["referee"]["country"]["name"]
                                
                                # Add referee country
                                if referee_country_id not in existing_country_ids:
                                    cur.execute("INSERT INTO Country (country_id, country_name) VALUES (%s, %s)", (referee_country_id, referee_country_name))
                                    conn.commit()
                                    existing_country_ids.add(referee_country_id)
                            
                            home_country_id = match["home_team"]["country"]["id"]
                            home_country_name = match["home_team"]["country"]["name"]
                            
                            # Add home country
                            if home_country_id not in existing_country_ids:
                                cur.execute("INSERT INTO Country (country_id, country_name) VALUES (%s, %s)", (home_country_id, home_country_name))
                                conn.commit()
                                existing_country_ids.add(home_country_id)
                            
                            away_country_id = match["away_team"]["country"]["id"]
                            away_country_name = match["away_team"]["country"]["name"]
                            
                            # Add away country
                            if away_country_id not in existing_country_ids:
                                cur.execute("INSERT INTO Country (country_id, country_name) VALUES (%s, %s)", (away_country_id, away_country_name))
                                conn.commit()
                                existing_country_ids.add(away_country_id)
    # Iterate through lineup files
    for filename in os.listdir(lineups_folder):
        if filename.endswith(".json"):
            filepath = os.path.join(lineups_folder, filename)
            with open(filepath, 'r') as file:
                lineup_data = json.load(file)
                # Check if lineup data is a list
                if isinstance(lineup_data, list):
                    for team_lineup in lineup_data:
                        for player_info in team_lineup["lineup"]:
                            
                            # Add country if not already in the database
                            if "country" in player_info:
                                country_id = player_info["country"]["id"]
                                country_name = player_info["country"]["name"]

                                if country_id not in existing_country_ids:
                                    cur.execute("""
                                        INSERT INTO Country (country_id, country_name)
                                        VALUES (%s, %s)
                                    """, (country_id, country_name))
                                    conn.commit()
                                    existing_country_ids.add(country_id)
    print("Country table populated successfully.")

populate_country_table()


# Function to populate the Manager table
def populate_manager_table():
    matches_folder = "data/matches"
    existing_manager_ids = set()  # Keep track of existing manager_ids
    
    # Fetch existing manager_ids from the database
    cur.execute("SELECT manager_id FROM Manager")
    rows = cur.fetchall()
    for row in rows:
        existing_manager_ids.add(row[0])
    
    for competition_id in os.listdir(matches_folder):
        competition_path = os.path.join(matches_folder, competition_id)
        if os.path.isdir(competition_path):
            for season_id in os.listdir(competition_path):
                season_path = os.path.join(competition_path, season_id)
                if os.path.isfile(season_path):
                    with open(season_path, 'r') as file:
                        matches_data = json.load(file)
                        for match in matches_data:
                            home_team = match.get("home_team", {})
                            away_team = match.get("away_team", {})
                            
                            # Add home manager if available
                            home_managers = home_team.get("managers", [])
                            if home_managers:
                                home_manager = home_managers[0]
                                home_manager_id = home_manager["id"]
                                if home_manager_id not in existing_manager_ids:
                                    home_manager_country_name = home_manager["country"]["name"]
                                    home_manager_name = home_manager["name"]
                                    home_manager_nickname = home_manager.get("nickname", None)
                                    home_manager_dob = home_manager["dob"]
                                    cur.execute("INSERT INTO Manager (manager_id, name, nickname, dob, country_name) VALUES (%s, %s, %s, %s, %s)",
                                                (home_manager_id, home_manager_name, home_manager_nickname, home_manager_dob, home_manager_country_name))
                                    conn.commit()
                                    existing_manager_ids.add(home_manager_id)
                            
                            # Add away manager if available
                            away_managers = away_team.get("managers", [])
                            if away_managers:
                                away_manager = away_managers[0]
                                away_manager_id = away_manager["id"]
                                if away_manager_id not in existing_manager_ids:
                                    away_manager_country_name = away_manager["country"]["name"]
                                    away_manager_name = away_manager["name"]
                                    away_manager_nickname = away_manager.get("nickname", None)
                                    away_manager_dob = away_manager["dob"]
                                    cur.execute("INSERT INTO Manager (manager_id, name, nickname, dob, country_name) VALUES (%s, %s, %s, %s, %s)",
                                                (away_manager_id, away_manager_name, away_manager_nickname, away_manager_dob, away_manager_country_name))
                                    conn.commit()
                                    existing_manager_ids.add(away_manager_id)
    print("Manager table populated successfully.")

populate_manager_table()


# Function to populate the Team table
def populate_team_table():
    matches_folder = "data/matches"
    existing_team_ids = set()  # Keep track of existing team_ids
    
    # Fetch existing team_ids from the database
    cur.execute("SELECT team_id FROM Team")
    rows = cur.fetchall()
    for row in rows:
        existing_team_ids.add(row[0])
    
    for competition_id in os.listdir(matches_folder):
        competition_path = os.path.join(matches_folder, competition_id)
        if os.path.isdir(competition_path):
            for season_id in os.listdir(competition_path):
                season_path = os.path.join(competition_path, season_id)
                if os.path.isfile(season_path):
                    with open(season_path, 'r') as file:
                        matches_data = json.load(file)
                        for match in matches_data:
                            home_team_id = match["home_team"]["home_team_id"]
                            home_team_name = match["home_team"]["home_team_name"]
                            away_team_id = match["away_team"]["away_team_id"]
                            away_team_name = match["away_team"]["away_team_name"]

                            # Add home team
                            if home_team_id not in existing_team_ids:
                                cur.execute("INSERT INTO Team (team_id, team_name) VALUES (%s, %s)", (home_team_id, home_team_name))
                                conn.commit()
                                existing_team_ids.add(home_team_id)
                            
                            # Add away team
                            if away_team_id not in existing_team_ids:
                                cur.execute("INSERT INTO Team (team_id, team_name) VALUES (%s, %s)", (away_team_id, away_team_name))
                                conn.commit()
                                existing_team_ids.add(away_team_id)
    print("Team table populated successfully.")

populate_team_table()

# Function to populate the CompetitionStage table
def populate_competition_stage_table():
    matches_folder = "data/matches"
    existing_stage_ids = set()  # Keep track of existing competition_stage_ids
    
    # Fetch existing competition_stage_ids from the database
    cur.execute("SELECT competition_stage_id FROM CompetitionStage")
    rows = cur.fetchall()
    for row in rows:
        existing_stage_ids.add(row[0])
    
    for competition_id in os.listdir(matches_folder):
        competition_path = os.path.join(matches_folder, competition_id)
        if os.path.isdir(competition_path):
            for season_id in os.listdir(competition_path):
                season_path = os.path.join(competition_path, season_id)
                if os.path.isfile(season_path):
                    with open(season_path, 'r') as file:
                        matches_data = json.load(file)
                        for match in matches_data:
                            stage_id = match["competition_stage"]["id"]
                            stage_name = match["competition_stage"]["name"]

                            # Add competition stage
                            if stage_id not in existing_stage_ids:
                                cur.execute("INSERT INTO CompetitionStage (competition_stage_id, competition_stage_name) VALUES (%s, %s)", (stage_id, stage_name))
                                conn.commit()
                                existing_stage_ids.add(stage_id)
    print("CompetitionStage table populated successfully.")

populate_competition_stage_table()


# Function to populate the Matches table
def populate_matches_table(desired_seasons, desired_competitions):
    matches_folder = "data/matches"
    for competition_id in os.listdir(matches_folder):
        competition_path = os.path.join(matches_folder, competition_id)
        if os.path.isdir(competition_path):
            for season_id in os.listdir(competition_path):
                season_path = os.path.join(competition_path, season_id)
                if os.path.isfile(season_path):
                    with open(season_path, 'r') as file:
                        matches_data = json.load(file)
                        for match in matches_data:
                             # Extract season and competition information
                            season_name = match["season"]["season_name"]
                            competition_name = match["competition"]["competition_name"]

                            if season_name in desired_seasons and competition_name in desired_competitions:

                                home_team_manager_id = match["home_team"].get("managers", [{}])[0].get("id", None)
                                away_team_manager_id = match["away_team"].get("managers", [{}])[0].get("id", None)
                                referee_id = match.get("referee", {}).get("id", None)
                                referee_country = match.get("referee", {}).get("country", {}).get("id", None)
                                stadium_id = match.get("stadium", {}).get("id", None)
                                stadium_country_name = match.get("stadium", {}).get("country", {}).get("name", None)
                                data_version = match.get("metadata", {}).get("data_version", None)
                                                     
                                
                                cur.execute("""
                                    INSERT INTO Matches (
                                        match_id, competition_id, country_name, season_id,
                                        match_date, kick_off, stadium_id, stadium_country,
                                        referee_id, referee_country_id, home_team_id,
                                        home_team_gender, home_team_manager_id, home_team_group,
                                        home_team_country_id, away_team_id, away_team_gender,
                                        away_team_manager_id, away_team_group, away_team_country_id,
                                        home_score, away_score, match_status, match_week,
                                        competition_stage_id, last_updated, data_version
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s
                                    )
                                """, (
                                    match["match_id"], match["competition"]["competition_id"], match["competition"]["country_name"],
                                    match["season"]["season_id"], match["match_date"], match["kick_off"], stadium_id,
                                    stadium_country_name, referee_id, referee_country,
                                    match["home_team"]["home_team_id"], match["home_team"]["home_team_gender"], home_team_manager_id,
                                    match["home_team"]["home_team_group"], match["home_team"]["country"]["id"], match["away_team"]["away_team_id"],
                                    match["away_team"]["away_team_gender"], away_team_manager_id, match["away_team"]["away_team_group"],
                                    match["away_team"]["country"]["id"], match["home_score"], match["away_score"], match["match_status"],
                                    match["match_week"], match["competition_stage"]["id"], match["last_updated"], data_version
                                ))
                                conn.commit()
    print("Matches table populated successfully.")

populate_matches_table(desired_seasons, desired_competitions)


# LINEUPS DATASET TABLES: 
# Updated the country table with countries from the Lineups dataset

# Function to populate the Player table
def populate_player_table():

    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    player_ids = set()  # Keep track of existing player IDs
    
    # Fetch existing player IDs from the database
    cur.execute("SELECT player_id FROM Player")
    rows = cur.fetchall()
    for row in rows:
        player_ids.add(row[0])
    
    lineup_folder = "data/lineups"
    
    # Iterate through lineup files
    for filename in os.listdir(lineup_folder):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:

            if filename.endswith(".json"):
                filepath = os.path.join(lineup_folder, filename)
                with open(filepath, 'r') as file:
                    lineup_data = json.load(file)
                    # Check if lineup data is a list
                    if isinstance(lineup_data, list):
                        for team_lineup in lineup_data:
                            team_id = team_lineup["team_id"]
                            for player_info in team_lineup["lineup"]:
                                player_id = player_info["player_id"]
                                player_name = player_info["player_name"]
                                player_nickname = player_info.get("player_nickname", None)

                                if "country" in player_info:
                                    country_id = player_info["country"]["id"]
                                
                                jersey_number = player_info["jersey_number"]
                                
                                # Add player if not already in the database
                                if player_id not in player_ids:
                                    cur.execute("""
                                        INSERT INTO Player (player_id, player_name, player_nickname, country_id, jersey_number)
                                        VALUES (%s, %s, %s, %s, %s)
                                    """, (player_id, player_name, player_nickname, country_id, jersey_number))
                                    conn.commit()
                                    player_ids.add(player_id)
                    else:
                        print(f"Invalid lineup data format in file: {filename}")
    
    print("Player table populated successfully.")

populate_player_table()


# Function to populate the Match_Lineup table
def populate_match_lineup_table():

    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    # Iterate through lineup files
    lineup_folder = "data/lineups"
    for filename in os.listdir(lineup_folder):
        if filename.endswith(".json"):
            filepath = os.path.join(lineup_folder, filename)
            with open(filepath, 'r') as file:
                lineup_data = json.load(file)

                match_id = int(filename.split(".")[0])

                if match_id in existing_match_ids:
                    for team_lineup in lineup_data:
                        # match_id = int(filename.split(".")[0])  # Extract match_id from filename
                        team_id = team_lineup["team_id"]
                        for player_info in team_lineup["lineup"]:
                            player_id = player_info["player_id"]
                            # Insert player into Match_Lineup table
                            cur.execute("""
                                INSERT INTO Match_Lineup (match_id, team_id, player_id)
                                VALUES (%s, %s, %s)
                            """, (match_id, team_id, player_id))
                            conn.commit()

    print("Match_Lineup table populated successfully.")

# Call the function to populate the Match_Lineup table

populate_match_lineup_table()


# EVENTS DATASET TABLES: 

# Function to populate Event_type table
def populate_event_type_table():

    # Fetch existing event types from the database
    cur.execute("SELECT event_name FROM Event_type")
    rows = cur.fetchall()
    existing_event_types = set(row[0] for row in rows)

    events_folder = "data/events"
    for filename in os.listdir(events_folder):
        if filename.endswith(".json"):
            filepath = os.path.join(events_folder, filename)
            with open(filepath, 'r') as file:
                events_data = json.load(file)
                # Track event types encountered in this match file
                event_types_in_file = set()
                for event in events_data:
                    event_id = event["type"]["id"]
                    event_name = event["type"]["name"]

                    if event_name not in existing_event_types and event_name not in event_types_in_file:
                        # Insert event type into Event_type table
                        cur.execute("""
                            INSERT INTO Event_type (event_type_id, event_name)
                            VALUES (%s, %s)
                        """, (event_id, event_name))
                        conn.commit()
                        existing_event_types.add(event_name)
                        event_types_in_file.add(event_name)

    print("Event_type table populated successfully.")

populate_event_type_table()



def populate_position_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    existing_positions = set()

    # Fetch existing positions from the database
    cur.execute("SELECT position_id FROM Position")
    rows = cur.fetchall()
    for row in rows:
        existing_positions.add(row[0])

    events_folder = "data/events"
    for filename in os.listdir(events_folder):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:

            if filename.endswith(".json"):
                filepath = os.path.join(events_folder, filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        tactics = event.get("tactics")
                        if tactics:
                            lineup = tactics.get("lineup")
                            if lineup:
                                for player in lineup:
                                    position = player.get("position")
                                    if position:
                                        position_id = position.get("id")
                                        position_name = position.get("name")
                                        if position_id not in existing_positions:
                                            # Insert position into Position table
                                            cur.execute("""
                                                INSERT INTO Position (position_id, position_name)
                                                VALUES (%s,%s)
                                            """, (position_id,position_name))
                                            conn.commit()
                                            existing_positions.add(position_id)

    print("Position table populated successfully.")

populate_position_table()

def populate_lineup_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                with open(os.path.join("data/events", filename), "r") as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if "tactics" in event:
                            formation = event["tactics"]["formation"]
                            lineup = event["tactics"]["lineup"]
                            for player_data in lineup:
                                player_id = player_data["player"]["id"]
                                position_id = player_data["position"]["id"]
                                jersey_number = player_data["jersey_number"]
                                # Insert data into Lineup table
                                cur.execute("""
                                    INSERT INTO Lineup (event_id, player_id, position_id, jersey_number)
                                    VALUES (%s, %s, %s, %s)
                                """, (event["type"]["id"], player_id, position_id, jersey_number))
                                conn.commit()

    print("Lineup table populated successfully.")

populate_lineup_table()


# Function to convert timestamp string to PostgreSQL timestamp format
def convert_timestamp(timestamp_str):
    return datetime.strptime(timestamp_str, '%H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S.%f')


# Function to populate the Events table
def populate_events_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    events_folder = "data/events"
    batch_size = 1000  

    for filename in os.listdir(events_folder):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join(events_folder, filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)

                    # Prepare a list to store values for batch insertion
                    batch_values = []

                    for event in events_data:
                        match_id = int(filename.split(".")[0])
                        event_uuid = event["id"]
                        index = event["index"]
                        period = event["period"]
                        timestamp = convert_timestamp(event["timestamp"])
                        minute = event["minute"]
                        second = event["second"]
                        event_type_id = event["type"]["id"]
                        possession = event["possession"]
                        possession_team_id = event["possession_team"]["id"]
                        play_pattern_name = event["play_pattern"]["name"]
                        team_id = event["team"]["id"]
                        player_id = event.get("player", {}).get("id")
                        position_id = event.get("position", {}).get("id")
                        if not position_id:
                            position_id = None
                        location_x = event.get("location", [0, 0])[0]
                        location_y = event.get("location", [0, 0])[1]
                        duration = event.get("duration", 0.0)
                        under_pressure = event.get("under_pressure", False)
                        off_camera = event.get("off_camera", False)
                        out = event.get("out", False)

                        # Append values to the batch list
                        batch_values.append((event_uuid, match_id, index, period, timestamp, minute, second,
                                            event_type_id, possession, possession_team_id, play_pattern_name,
                                            team_id, player_id, position_id, location_x, location_y,
                                            duration, under_pressure, off_camera, out))

                        # If the batch size is reached, execute the batch insertion
                        if len(batch_values) == batch_size:
                            execute_batch_insert(batch_values)
                            batch_values = []

                    # Insert any remaining rows
                    if batch_values:
                        execute_batch_insert(batch_values)

    print("Events table populated successfully.")


def execute_batch_insert(batch_values):
    # Execute a batch insertion with the provided values
    cur.executemany("""
        INSERT INTO Events (event_uuid, match_id, index, period, timestamp, minute, second, 
                            event_type_id, possession, possession_team_id, play_pattern_name, 
                            team_id, player_id, position_id, location_x, location_y, duration, 
                            under_pressure, off_camera, out)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, batch_values)
    conn.commit()

populate_events_table()


def populate_related_events_table():
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 
    # Prepare a list of tuples containing data for batch insert
    data = []
    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        event_uuid = event.get("id")
                        related_events = event.get("related_events", [])
                        for related_event_uuid in related_events:
                            data.append((event_uuid, related_event_uuid))

    # Batch insert into Related_Events table
    cur.executemany("""
        INSERT INTO Related_Events (event_uuid, related_event_uuid) 
        VALUES (%s, %s)
    """, data)
    conn.commit()

    print("Related_Events table populated successfully.")

populate_related_events_table()

def populate_outcome_50_50_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "50/50":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            possession_team_id = event["possession_team"]["id"]
                            outcome = event["50_50"]["outcome"]["name"]
                            counterpress = event.get("counterpress", False)

                            cur.execute("""
                                INSERT INTO Outcome_50_50 (event_type_id, match_id, index, event_name,
                                                            possession_team_id, outcome, counterpress)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, possession_team_id,
                                outcome, counterpress))
                            conn.commit()

    print("Outcome_50_50 table populated successfully.")

populate_outcome_50_50_table()

def populate_bad_behaviour_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if "bad_behaviour" in event:
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            card = event["bad_behaviour"]["card"]["name"]

                            cur.execute("""
                                INSERT INTO Bad_Behaviour (event_type_id, match_id, index, event_name, card)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, card))
                            conn.commit()

    print("Bad_Behaviour table populated successfully.")

populate_bad_behaviour_table()

def populate_ball_receipt_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:        
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if "ball_receipt" in event:
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            outcome = event["ball_receipt"]["outcome"]["name"]

                            cur.execute("""
                                INSERT INTO Ball_Receipt (event_type_id, match_id, index, event_name, outcome)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, outcome))
                            conn.commit()

    print("Ball_Receipt table populated successfully.")

populate_ball_receipt_table()


def populate_ball_recovery_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if "ball_recovery" in event:
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            offensive = event["ball_recovery"].get("offensive", False)
                            recovery_failure = event["ball_recovery"].get("recovery_failure", False)

                            cur.execute("""
                                INSERT INTO Ball_Recovery (event_type_id, match_id, index, event_name, offensive, recovery_failure)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, offensive, recovery_failure))
                            conn.commit()

    print("Ball_Recovery table populated successfully.")

populate_ball_recovery_table()


def populate_block_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    batch_data = []

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if "block" in event:
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            block_data = event["block"]
                            deflection = block_data.get("deflection", False)
                            offensive = block_data.get("offensive", False)
                            save_block = block_data.get("save_block", False)
                            counterpress = block_data.get("counterpress", False)

                            batch_data.append((event_type_id, match_id, index, event_name, deflection, offensive, save_block, counterpress))

    # Batch insert
    cur.executemany("""
        INSERT INTO Block (event_type_id, match_id, index, event_name, deflection, offensive, save_block, counterpress)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, batch_data)
    conn.commit()

    print("Block table populated successfully.")

populate_block_table()

def populate_carry_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if "carry" in event:
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            team_id = event["team"]["id"]
                            end_location = event["carry"]["end_location"]
                            end_location_x = end_location[0]
                            end_location_y = end_location[1]

                            cur.execute("""
                                INSERT INTO Carry (event_type_id, match_id, index, event_name, team_id, end_location_x, end_location_y)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, team_id, end_location_x, end_location_y))
                            conn.commit()

    print("Carry table populated successfully.")

populate_carry_table()


def populate_clearance_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if "clearance" in event:
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            aerial_won = event["clearance"].get("aerial_won", False)
                            body_part = event["clearance"].get("body_part", {}).get("name")

                            cur.execute("""
                                INSERT INTO Clearance (event_type_id, match_id, index, event_name, aerial_won, body_part)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, aerial_won, body_part))
                            conn.commit()

    print("Clearance table populated successfully.")

populate_clearance_table()


def populate_dribble_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    batch_data = []

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if "dribble" in event:
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            dribble_data = event["dribble"]
                            overrun = dribble_data.get("overrun", False)
                            nutmeg = dribble_data.get("nutmeg", False)
                            outcome = dribble_data["outcome"].get("name", None)
                            no_touch = dribble_data.get("no_touch", False)

                            batch_data.append((event_type_id, match_id, index, event_name, overrun, nutmeg, outcome, no_touch))

    # Batch insert
    cur.executemany("""
        INSERT INTO Dribble (event_type_id, match_id, index, event_name, overrun, nutmeg, outcome, no_touch)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, batch_data)
    conn.commit()

    print("Dribble table populated successfully.")

populate_dribble_table()



def populate_dribbled_past_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Dribbled Past":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            counterpress = event.get("counterpress", False)

                            cur.execute("""
                                INSERT INTO Dribbled_Past (event_type_id, match_id, index, event_name, counterpress)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, counterpress))
                            conn.commit()

    print("Dribbled_Past table populated successfully.")

populate_dribbled_past_table()


def populate_duel_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Duel":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            duel_type = event["duel"]["type"]["name"]
                            duel_outcome = event["duel"].get("outcome", {}).get("name")
                            counterpress = event.get("counterpress", False)

                            cur.execute("""
                                INSERT INTO Duel (event_type_id, match_id, index, event_name, duel_type, duel_outcome, counterpress)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, duel_type, duel_outcome, counterpress))
                            conn.commit()

    print("Duel table populated successfully.")

populate_duel_table()


def populate_foul_committed_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    batch_data = []

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Foul Committed":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            foul_type = event.get("foul_commited",{}).get("type",{}).get("name", None)
                            counterpass = event.get("foul_commited",{}).get("counterpress", False)
                            offensive = event.get("foul_commited",{}).get("offensive", False)
                            advantage = event.get("foul_commited",{}).get("advantage", False)
                            penalty = event.get("foul_commited",{}).get("penalty", False)
                            card = event.get("foul_commited",{}).get("card", {}).get("name")

                            batch_data.append((event_type_id, match_id, index, event_name, foul_type, counterpass, offensive, advantage, penalty, card))

    # Batch insert
    cur.executemany("""
        INSERT INTO Foul_Committed (event_type_id, match_id, index, event_name, foul_type, counterpass, offensive, advantage, penalty, card)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, batch_data)
    conn.commit()

    print("Foul_Committed table populated successfully.")

populate_foul_committed_table()



def populate_foul_won_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Foul Won":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            defensive = event.get("foul_won",{}).get("defensive", False)
                            advantage = event.get("foul_won",{}).get("advantage", False)
                            penalty = event.get("foul_won",{}).get("penalty", False)

                            cur.execute("""
                                INSERT INTO Foul_Won (event_type_id, match_id, index, event_name, defensive, advantage, penalty)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, defensive, advantage, penalty))
                            conn.commit()

    print("Foul_Won table populated successfully.")

populate_foul_won_table()



def populate_goalkeeper_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    batch_data = []

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Goal Keeper":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            position_name = event.get("goalkeeper").get("position", {}).get("name")
                            technique_name = event.get("goalkeeper").get("technique", {}).get("name")
                            body_part_name = event.get("goalkeeper").get("body_part", {}).get("name")
                            type_name = event.get("goalkeeper").get("type", {}).get("name")
                            outcome_name = event.get("goalkeeper").get("outcome", {}).get("name")

                            batch_data.append((event_type_id, match_id, index, event_name, 
                                            position_name, technique_name, body_part_name,
                                            type_name, outcome_name))

    # Batch insert
    cur.executemany("""
        INSERT INTO Goalkeeper (event_type_id, match_id, index, event_name, 
                                position_name, technique_name, body_part_name,
                                type_name, outcome_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, batch_data)
    conn.commit()

    print("Goalkeeper table populated successfully.")

populate_goalkeeper_table()



def populate_half_end_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Half End":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            early_video_end = event.get("early_video_end", False)
                            match_suspended = event.get("match_suspended", False)

                            cur.execute("""
                                INSERT INTO Half_End (event_type_id, match_id, index, event_name, 
                                                    early_video_end, match_suspended)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, 
                                early_video_end, match_suspended))
                            conn.commit()

    print("Half_End table populated successfully.")

populate_half_end_table()


def populate_half_start_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()}

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Half Start":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            late_video_start = event.get("late_video_start", False)

                            cur.execute("""
                                INSERT INTO Half_Start (event_type_id, match_id, index, event_name, late_video_start)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, late_video_start))
                            conn.commit()

    print("Half_Start table populated successfully.")

populate_half_start_table()


def populate_injury_stoppage_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Injury Stoppage":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            in_chain = event.get("in_chain",{}).get("in_chain", False)

                            cur.execute("""
                                INSERT INTO Injury_Stoppage (event_type_id, match_id, index, event_name, in_chain)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, in_chain))
                            conn.commit()

    print("Injury_Stoppage table populated successfully.")

populate_injury_stoppage_table()


def populate_interception_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Interception":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            outcome = event.get("interception",{}).get("outcome",{}).get("name",None)

                            cur.execute("""
                                INSERT INTO Interception (event_type_id, match_id, index, event_name, outcome)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, outcome))
                            conn.commit()

    print("Interception table populated successfully.")

populate_interception_table()


def populate_miscontrol_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Miscontrol":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            aerial_won = event.get("miscontrol",{}).get("aerial_won", False)

                            cur.execute("""
                                INSERT INTO Miscontrol (event_type_id, match_id, index, event_name, aerial_won)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, aerial_won))
                            conn.commit()

    print("Miscontrol table populated successfully.")

populate_miscontrol_table()


def populate_pass_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    pass_events = []

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Pass":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            recipient_id = event.get("pass",{}).get("recipient",{}).get("id")
                            length = event.get("pass",{}).get("length", None)
                            angle = event.get("pass",{}).get("angle",None)
                            height_name = event.get("pass",{}).get("height",{}).get("name")
                            end_location_x = event.get("pass",{}).get("end_location")[0]
                            end_location_y = event.get("pass",{}).get("end_location")[1]
                            assisted_shot_id = event.get("pass",{}).get("assisted_shot_id", None)
                            backheel = event.get("pass",{}).get("backheel", False)
                            deflected = event.get("pass",{}).get("deflected", False)
                            miscommunication = event.get("pass",{}).get("miscommunication", False)
                            if_cross = event.get("pass",{}).get("cross", False)
                            cut_back = event.get("pass",{}).get("cut_back", False)
                            switch = event.get("pass",{}).get("switch", False)
                            shot_assist = event.get("pass",{}).get("shot_assist",False)
                            goal_assist = event.get("pass",{}).get("goal_assist", False)
                            body_part_name = event.get("pass",{}).get("body_part", {}).get("name", None)
                            type_name = event.get("pass",{}).get("type", {}).get("name", None)
                            outcome_name = event.get("pass",{}).get("outcome", {}).get("name", None)
                            technique_name = event.get("pass",{}).get("technique", {}).get("name", None)

                            pass_events.append((event_type_id, match_id, index, event_name, recipient_id, length, angle,
                                                height_name, end_location_x, end_location_y, assisted_shot_id, backheel,
                                                deflected, miscommunication, if_cross, cut_back, switch, shot_assist,
                                                goal_assist, body_part_name, type_name, outcome_name, technique_name))

    cur.executemany("""
        INSERT INTO Pass (event_type_id, match_id, index, event_name, recipient_id, length, angle, height_name, 
                          end_location_x, end_location_y, assisted_shot_id, backheel, deflected, miscommunication, 
                          if_cross, cut_back, switch, shot_assist, goal_assist, body_part_name, type_name, 
                          outcome_name, technique_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, pass_events)
    conn.commit()

    print("Pass table populated successfully.")

populate_pass_table()


def populate_player_off_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        event_type = event.get("type", {})
                        event_name = event_type.get("name")
                        if event["type"]["name"] == "Player Off":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            permanent = event.get("player_off",{}).get("permanent",False)
                            
                            cur.execute("""
                                INSERT INTO Player_Off (event_type_id, match_id, index, event_name, permanent)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, permanent))
                            conn.commit()

    print("Player_Off table populated successfully.")

populate_player_off_table()


def populate_pressure_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        if event["type"]["name"] == "Pressure":
                            event_type_id = event["type"]["id"]
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            event_name = event["type"]["name"]
                            counterpress = event.get("counterpress", False)
                            
                            cur.execute("""
                                INSERT INTO Pressure (event_type_id, match_id, index, event_name, counterpress)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (event_type_id, match_id, index, event_name, counterpress))
                            conn.commit()

    print("Pressure table populated successfully.")

populate_pressure_table()


def populate_shot_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    batch_data = []
    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        event_type = event.get("type", {})
                        event_name = event_type.get("name")
                        if event_name == "Shot":
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            key_pass_id = event.get("shot").get("key_pass_id")
                            end_location_x = event.get("shot").get("end_location")[0]
                            end_location_y = event.get("shot").get("end_location")[1]
                            if len(event.get("shot").get("end_location")) == 3:
                                end_location_z = event.get("shot").get("end_location")[2]
                            aerial_won = event.get("shot").get("aerial_won", False)
                            follows_dribble = event.get("shot").get("follows_dribble", False)
                            first_time = event.get("shot").get("first_time", False)
                            open_goal = event.get("shot").get("open_goal", False)
                            statsbomb_xg = event.get("shot").get("statsbomb_xg", 0.0)
                            deflected = event.get("shot").get("deflected", False)
                            technique_name = event.get("shot").get("technique", {}).get("name")
                            body_part_name = event.get("shot").get("body_part", {}).get("name")
                            type_name = event.get("shot").get("type", {}).get("name")
                            outcome_name = event.get("shot").get("outcome", {}).get("name")

                            batch_data.append((event_type["id"], match_id, index, event_name, key_pass_id,
                                            end_location_x, end_location_y, end_location_z, aerial_won, follows_dribble,
                                            first_time, open_goal, statsbomb_xg, deflected, technique_name,
                                            body_part_name, type_name, outcome_name))
                            if len(batch_data) >= 1000:
                                cur.executemany("""
                                    INSERT INTO Shot (event_type_id, match_id, index, event_name, key_pass_id,
                                                    end_location_x, end_location_y, end_location_z, aerial_won, follows_dribble,
                                                    first_time, open_goal, statsbomb_xg, deflected, technique_name,
                                                    body_part_name, type_name, outcome_name)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """, batch_data)
                                conn.commit()
                                batch_data = []

    if batch_data:
        cur.executemany("""
            INSERT INTO Shot (event_type_id, match_id, index, event_name, key_pass_id,
                             end_location_x, end_location_y, end_location_z, aerial_won, follows_dribble,
                             first_time, open_goal, statsbomb_xg, deflected, technique_name,
                             body_part_name, type_name, outcome_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch_data)
        conn.commit()

    print("Shot table populated successfully.")

populate_shot_table()


def populate_substitution_table():
    # Fetch existing match IDs from the Matches table
    cur.execute("SELECT match_id FROM Matches")
    existing_match_ids = {row[0] for row in cur.fetchall()} 

    for filename in os.listdir("data/events"):
        match_id = int(filename.split(".")[0])

        if match_id in existing_match_ids:
            if filename.endswith(".json"):
                filepath = os.path.join("data/events", filename)
                with open(filepath, 'r') as file:
                    events_data = json.load(file)
                    for event in events_data:
                        event_type = event.get("type", {})
                        event_name = event_type.get("name")
                        if event_name == "Substitution":
                            match_id = int(filename.split(".")[0])
                            index = event["index"]
                            replacement_id = event.get("substitution").get("replacement", {}).get("id")
                            outcome = event.get("substitution").get("outcome", {}).get("name")

                            cur.execute("""
                                INSERT INTO Substitution (event_type_id, match_id, index, event_name, replacement_id, outcome) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (event_type["id"], match_id, index, event_name, replacement_id, outcome))
                            conn.commit()

    print("Substitution table populated successfully.")

populate_substitution_table()



# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()


