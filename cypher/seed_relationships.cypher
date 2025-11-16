CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_athlete_country.csv' AS row
  WITH row
  WHERE row.`:START_ID(Athlete-ID)` IS NOT NULL AND row.`:END_ID(Country-ID)` IS NOT NULL
  MATCH (a:Athlete {athlete_code: row.`:START_ID(Athlete-ID)`})
  MATCH (c:Country {country_code: row.`:END_ID(Country-ID)`})
  MERGE (a)-[:REPRESENTS]->(c)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_coach_country.csv' AS row
  WITH row
  WHERE row.`:START_ID(Coach-ID)` IS NOT NULL AND row.`:END_ID(Country-ID)` IS NOT NULL
  MATCH (c:Coach {coach_code: row.`:START_ID(Coach-ID)`})
  MATCH (country:Country {country_code: row.`:END_ID(Country-ID)`})
  MERGE (c)-[:REPRESENTS]->(country)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_team_country.csv' AS row
  WITH row
  WHERE row.`:START_ID(Team-ID)` IS NOT NULL AND row.`:END_ID(Country-ID)` IS NOT NULL
  MATCH (t:Team {team_code: row.`:START_ID(Team-ID)`})
  MATCH (c:Country {country_code: row.`:END_ID(Country-ID)`})
  MERGE (t)-[:REPRESENTS]->(c)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_official_country.csv' AS row
  WITH row
  WHERE row.`:START_ID(Official-ID)` IS NOT NULL AND row.`:END_ID(Country-ID)` IS NOT NULL
  MATCH (o:Official {official_code: row.`:START_ID(Official-ID)`})
  MATCH (c:Country {country_code: row.`:END_ID(Country-ID)`})
  MERGE (o)-[:REPRESENTS]->(c)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_event_sport.csv' AS row
  WITH row
  WHERE row.`:START_ID(Event-ID)` IS NOT NULL AND row.`:END_ID(Sport-ID)` IS NOT NULL
  MATCH (e:Event {event_code: row.`:START_ID(Event-ID)`})
  MATCH (s:Sport {sport_code: row.`:END_ID(Sport-ID)`})
  MERGE (e)-[:PART_OF]->(s)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_event_venue.csv' AS row
  WITH row
  WHERE row.`:START_ID(Event-ID)` IS NOT NULL AND row.`:END_ID(Venue-ID)` IS NOT NULL
  MATCH (e:Event {event_code: row.`:START_ID(Event-ID)`})
  MATCH (v:Venue {venue_code: row.`:END_ID(Venue-ID)`})
  MERGE (e)-[:HOSTED_AT]->(v)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_session_sport.csv' AS row
  WITH row
  WHERE row.`:START_ID(Session-ID)` IS NOT NULL AND row.`:END_ID(Sport-ID)` IS NOT NULL
  MATCH (s:Session {session_id: row.`:START_ID(Session-ID)`})
  MATCH (sport:Sport {sport_code: row.`:END_ID(Sport-ID)`})
  MERGE (s)-[:SESSION_FOR]->(sport)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_session_venue.csv' AS row
  WITH row
  WHERE row.`:START_ID(Session-ID)` IS NOT NULL AND row.`:END_ID(Venue-ID)` IS NOT NULL
  MATCH (s:Session {session_id: row.`:START_ID(Session-ID)`})
  MATCH (v:Venue {venue_code: row.`:END_ID(Venue-ID)`})
  MERGE (s)-[:HELD_AT]->(v)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_sport_venue.csv' AS row
  WITH row
  WHERE row.`:START_ID(Sport-ID)` IS NOT NULL AND row.`:END_ID(Venue-ID)` IS NOT NULL
  MATCH (s:Sport {sport_code: row.`:START_ID(Sport-ID)`})
  MATCH (v:Venue {venue_code: row.`:END_ID(Venue-ID)`})
  MERGE (s)-[:USE_VENUE]->(v)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_athlete_team.csv' AS row
  WITH row
  WHERE row.`:START_ID(Athlete-ID)` IS NOT NULL AND row.`:END_ID(Team-ID)` IS NOT NULL
  MATCH (a:Athlete {athlete_code: row.`:START_ID(Athlete-ID)`})
  MATCH (t:Team {team_code: row.`:END_ID(Team-ID)`})
  MERGE (a)-[:MEMBER_OF]->(t)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_coach_team.csv' AS row
  WITH row
  WHERE row.`:START_ID(Coach-ID)` IS NOT NULL AND row.`:END_ID(Team-ID)` IS NOT NULL
  MATCH (c:Coach {coach_code: row.`:START_ID(Coach-ID)`})
  MATCH (t:Team {team_code: row.`:END_ID(Team-ID)`})
  MERGE (c)-[:COACHES]->(t)
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_athlete_medals.csv' AS row
  WITH row
  WHERE row.`:START_ID(Athlete-ID)` IS NOT NULL AND row.`:END_ID(Event-ID)` IS NOT NULL
  MATCH (a:Athlete {athlete_code: row.`:START_ID(Athlete-ID)`})
  MATCH (e:Event {event_code: row.`:END_ID(Event-ID)`})
  MERGE (a)-[rel:WON_MEDAL]->(e)
  SET rel.medal_type = row.medal_type,
      rel.medal_date = CASE row.medal_date
                         WHEN '' THEN NULL
                         ELSE date(row.medal_date)
                       END
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_team_medals.csv' AS row
  WITH row
  WHERE row.`:START_ID(Team-ID)` IS NOT NULL AND row.`:END_ID(Event-ID)` IS NOT NULL
  MATCH (t:Team {team_code: row.`:START_ID(Team-ID)`})
  MATCH (e:Event {event_code: row.`:END_ID(Event-ID)`})
  MERGE (t)-[rel:WON_MEDAL]->(e)
  SET rel.medal_type = row.medal_type,
      rel.medal_date = CASE row.medal_date
                         WHEN '' THEN NULL
                         ELSE date(row.medal_date)
                       END
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_athlete_event_results.csv' AS row
  WITH row,
       CASE
         WHEN row.stage_code IS NULL OR row.stage_code = ''
           THEN row.`:START_ID(Athlete-ID)` + '|' + row.`:END_ID(Event-ID)` + '|' + coalesce(row.stage, '') + '|' + coalesce(row.date, '')
         ELSE row.`:START_ID(Athlete-ID)` + '|' + row.stage_code
       END AS instance_id
  WHERE row.`:START_ID(Athlete-ID)` IS NOT NULL AND row.`:END_ID(Event-ID)` IS NOT NULL
  MATCH (a:Athlete {athlete_code: row.`:START_ID(Athlete-ID)`})
  MATCH (e:Event {event_code: row.`:END_ID(Event-ID)`})
  MERGE (a)-[rel:COMPETED_IN {instance_id: instance_id}]->(e)
  SET rel.stage_code = CASE row.stage_code WHEN '' THEN NULL ELSE row.stage_code END,
      rel.event_stage = row.event_stage,
      rel.stage = row.stage,
      rel.stage_datetime = CASE row.date
                             WHEN '' THEN NULL
                             ELSE datetime(row.date)
                           END,
      rel.result = row.result,
      rel.result_type = row.result_type,
      rel.result_status = row.result_status,
      rel.result_diff = row.result_diff,
      rel.bib = row.bib,
      rel.rank = CASE row.rank
                   WHEN '' THEN NULL
                   ELSE toInteger(toFloat(row.rank))
                 END
} IN TRANSACTIONS OF 1000 ROWS;


CALL {
  LOAD CSV WITH HEADERS FROM 'file:///rels_team_event_results.csv' AS row
  WITH row,
       CASE
         WHEN row.stage_code IS NULL OR row.stage_code = ''
           THEN row.`:START_ID(Team-ID)` + '|' + row.`:END_ID(Event-ID)` + '|' + coalesce(row.stage, '') + '|' + coalesce(row.date, '')
         ELSE row.`:START_ID(Team-ID)` + '|' + row.stage_code
       END AS instance_id
  WHERE row.`:START_ID(Team-ID)` IS NOT NULL AND row.`:END_ID(Event-ID)` IS NOT NULL
  MATCH (t:Team {team_code: row.`:START_ID(Team-ID)`})
  MATCH (e:Event {event_code: row.`:END_ID(Event-ID)`})
  MERGE (t)-[rel:COMPETED_IN {instance_id: instance_id}]->(e)
  SET rel.stage_code = CASE row.stage_code WHEN '' THEN NULL ELSE row.stage_code END,
      rel.event_stage = row.event_stage,
      rel.stage = row.stage,
      rel.stage_datetime = CASE row.date
                             WHEN '' THEN NULL
                             ELSE datetime(row.date)
                           END,
      rel.result = row.result,
      rel.result_type = row.result_type,
      rel.result_status = row.result_status,
      rel.result_diff = row.result_diff,
      rel.bib = row.bib,
      rel.rank = CASE row.rank
                   WHEN '' THEN NULL
                   ELSE toInteger(toFloat(row.rank))
                 END
} IN TRANSACTIONS OF 1000 ROWS;
