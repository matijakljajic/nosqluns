CREATE CONSTRAINT country_code IF NOT EXISTS
FOR (c:Country) REQUIRE c.country_code IS UNIQUE;

CREATE CONSTRAINT sport_code IF NOT EXISTS
FOR (s:Sport) REQUIRE s.sport_code IS UNIQUE;

CREATE CONSTRAINT venue_code IF NOT EXISTS
FOR (v:Venue) REQUIRE v.venue_code IS UNIQUE;

CREATE CONSTRAINT event_code IF NOT EXISTS
FOR (e:Event) REQUIRE e.event_code IS UNIQUE;

CREATE CONSTRAINT session_id IF NOT EXISTS
FOR (s:Session) REQUIRE s.session_id IS UNIQUE;

CREATE CONSTRAINT athlete_code IF NOT EXISTS
FOR (a:Athlete) REQUIRE a.athlete_code IS UNIQUE;

CREATE CONSTRAINT coach_code IF NOT EXISTS
FOR (c:Coach) REQUIRE c.coach_code IS UNIQUE;

CREATE CONSTRAINT team_code IF NOT EXISTS
FOR (t:Team) REQUIRE t.team_code IS UNIQUE;

CREATE CONSTRAINT official_code IF NOT EXISTS
FOR (o:Official) REQUIRE o.official_code IS UNIQUE;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///nodes_countries.csv' AS row
  WITH row
  WHERE row.`country_code:ID(Country-ID)` IS NOT NULL
  MERGE (c:Country {country_code: row.`country_code:ID(Country-ID)`})
  SET c.name = row.name,
      c.name_long = row.name_long,
      c.tag = row.tag,
      c.note = row.note
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///nodes_sports.csv' AS row
  WITH row
  WHERE row.`sport_code:ID(Sport-ID)` IS NOT NULL
  MERGE (s:Sport {sport_code: row.`sport_code:ID(Sport-ID)`})
  SET s.name = row.name,
      s.tag = row.tag,
      s.url = row.url
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///nodes_venues.csv' AS row
  WITH row,
       [value IN split(row.sport_codes, ';') WHERE value <> '' | trim(value)] AS sport_codes
  WHERE row.`venue_code:ID(Venue-ID)` IS NOT NULL
  MERGE (v:Venue {venue_code: row.`venue_code:ID(Venue-ID)`})
  SET v.name = row.name,
      v.location = row.location,
      v.tag = row.tag,
      v.url = row.url,
      v.sport_codes = sport_codes
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///nodes_events.csv' AS row
  WITH row,
       [value IN split(row.stages, ';') WHERE value <> '' | trim(value)] AS stages
  WHERE row.`event_code:ID(Event-ID)` IS NOT NULL
  MERGE (e:Event {event_code: row.`event_code:ID(Event-ID)`})
  SET e.name = row.name,
      e.gender = row.gender,
      e.sport_name = row.sport_name,
      e.sport_code = row.sport_code,
      e.stages = stages,
      e.has_medalists = CASE toLower(row.has_medalists)
                          WHEN 'true' THEN true
                          WHEN '1' THEN true
                          ELSE false
                        END
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///nodes_sessions.csv' AS row
  WITH row
  WHERE row.`session_id:ID(Session-ID)` IS NOT NULL
  MERGE (s:Session {session_id: row.`session_id:ID(Session-ID)`})
  SET s.start_datetime = CASE row.start_datetime
                           WHEN '' THEN NULL
                           ELSE datetime(row.start_datetime)
                         END,
      s.end_datetime = CASE row.end_datetime
                         WHEN '' THEN NULL
                         ELSE datetime(row.end_datetime)
                       END,
      s.day = CASE row.day
                WHEN '' THEN NULL
                ELSE date(row.day)
              END,
      s.status = row.status,
      s.sport = row.sport,
      s.sport_code = row.sport_code,
      s.event_phase = row.event_phase,
      s.event_name = row.event_name,
      s.gender = row.gender,
      s.event_type = row.event_type,
      s.medal_session = CASE row.medal_session
                          WHEN '1' THEN true
                          WHEN 'true' THEN true
                          WHEN 'True' THEN true
                          ELSE false
                        END,
      s.venue_code = row.venue_code
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///nodes_teams.csv' AS row
  WITH row
  WHERE row.`team_code:ID(Team-ID)` IS NOT NULL
  MERGE (t:Team {team_code: row.`team_code:ID(Team-ID)`})
  SET t.name = row.name,
      t.gender = row.gender,
      t.country_code = row.country_code,
      t.country = row.country,
      t.discipline = row.discipline,
      t.discipline_code = row.discipline_code,
      t.events = row.events,
      t.num_athletes = CASE row.num_athletes
                         WHEN '' THEN NULL
                         ELSE toInteger(toFloat(row.num_athletes))
                       END,
      t.num_coaches = CASE row.num_coaches
                        WHEN '' THEN NULL
                        ELSE toInteger(toFloat(row.num_coaches))
                      END
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///nodes_athletes.csv' AS row
  WITH row,
       [value IN split(row.disciplines, ';') WHERE value <> '' | trim(value)] AS disciplines,
       [value IN split(row.events, ';') WHERE value <> '' | trim(value)] AS events
  WHERE row.`athlete_code:ID(Athlete-ID)` IS NOT NULL
  MERGE (a:Athlete {athlete_code: row.`athlete_code:ID(Athlete-ID)`})
  SET a.name = row.name,
      a.name_short = row.name_short,
      a.gender = row.gender,
      a.function = row.function,
      a.country_code = row.country_code,
      a.nationality_code = row.nationality_code,
      a.height = CASE row.height
                   WHEN '' THEN NULL
                   ELSE toFloat(row.height)
                 END,
      a.weight = CASE row.weight
                   WHEN '' THEN NULL
                   ELSE toFloat(row.weight)
                 END,
      a.disciplines = disciplines,
      a.events = events,
      a.birth_date = CASE row.birth_date
                       WHEN '' THEN NULL
                       ELSE date(row.birth_date)
                     END,
      a.birth_place = row.birth_place
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///nodes_coaches.csv' AS row
  WITH row
  WHERE row.`coach_code:ID(Coach-ID)` IS NOT NULL
  MERGE (c:Coach {coach_code: row.`coach_code:ID(Coach-ID)`})
  SET c.name = row.name,
      c.gender = row.gender,
      c.function = row.function,
      c.category = row.category,
      c.country_code = row.country_code,
      c.country = row.country
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///nodes_officials.csv' AS row
  WITH row,
       [value IN split(row.disciplines, ';') WHERE value <> '' | trim(value)] AS disciplines
  WHERE row.`official_code:ID(Official-ID)` IS NOT NULL
  MERGE (o:Official {official_code: row.`official_code:ID(Official-ID)`})
  SET o.name = row.name,
      o.gender = row.gender,
      o.function = row.function,
      o.category = row.category,
      o.organisation_code = row.organisation_code,
      o.organisation = row.organisation,
      o.disciplines = disciplines
} IN TRANSACTIONS OF 1000 ROWS;
